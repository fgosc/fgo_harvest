import copy
import json
import io
import os
from datetime import date, datetime, timedelta
from enum import Enum
from logging import getLogger
from typing import (
    Any, BinaryIO, Dict, List,
    Sequence, Set,
)
from typing_extensions import Protocol

import boto3  # type: ignore
import botocore.exceptions  # type: ignore
from jinja2 import (
    Environment,
    PackageLoader,
    select_autoescape,
)

from . import freequest, twitter

logger = getLogger(__name__)
jinja2_env = Environment(
    loader=PackageLoader('chalicelib', 'templates'),
    autoescape=select_autoescape(['html'])
)


def json_serialize_helper(o: Any):
    if hasattr(o, 'isoformat'):
        return o.isoformat()
    raise TypeError(
        f'Object of type {o.__class__.__name__} is not JSON serializable'
    )


class OutputFormat(Enum):
    JSON = ('json', 'json')
    TEXT = ('txt', 'txt')
    USERHTML = ('userhtml', 'html')
    DATEHTML = ('datehtml', 'html')
    QUESTHTML = ('questhtml', 'html')


class AbstractTweetStorage:
    def put(self, key: str, tweets: List[twitter.TweetCopy]) -> None:
        s = json.dumps(
            [tw.as_dict() for tw in tweets],
            ensure_ascii=False,
            default=json_serialize_helper,
        )
        self._put_json(key, s)
    
    def _put_json(self, key: str, sjson: str) -> None:
        raise NotImplementedError

    def readall(self) -> List[twitter.TweetCopy]:
        raise NotImplementedError

    @staticmethod
    def _load_hook(d: Dict[str, Any]) -> Dict[str, Any]:
        if 'created_at' in d:
            v = d['created_at']
            d['created_at'] = datetime.fromisoformat(v)
        return d


class FilesystemTweetStorage(AbstractTweetStorage):
    def __init__(self, output_dir: str):
        self.output_dir = output_dir

    def _put_json(self, key: str, sjson: str) -> None:
        filepath = os.path.join(self.output_dir, key)
        with open(filepath, 'w') as fp:
            fp.write(sjson)
    
    def readall(self) -> List[twitter.TweetCopy]:
        tweets: List[twitter.TweetCopy] = []
        id_cache: Set[int] = set()

        entries = os.listdir(self.output_dir)
        for entry in entries:
            if os.path.splitext(entry)[1] != '.json':
                continue
            entrypath = os.path.join(self.output_dir, entry)
            logger.info(f'loading {entrypath}')
            with open(entrypath) as fp:
                loaded = json.load(fp)
            _tweets = [twitter.TweetCopy.retrieve(e) for e in loaded]
            logger.info(f'{len(_tweets)} tweets retrieved')
            for tw in _tweets:
                if tw.tweet_id in id_cache:
                    logger.warning('ignoring duplicate tweet: %s', tw.tweet_id)
                else:
                    tweets.append(tw)
                    id_cache.add(tw.tweet_id)

        # 新しい順
        tweets.sort(key=lambda e: e.tweet_id)
        tweets.reverse()

        logger.info(f'total: {len(tweets)} tweets')
        return tweets


class AmazonS3TweetStorage(AbstractTweetStorage):
    def __init__(self, bucket: str, output_dir: str):
        self.s3 = boto3.resource('s3')
        self.bucket = self.s3.Bucket(bucket)
        self.output_dir = output_dir

    def _put_json(self, key: str, sjson: str) -> None:
        s3key = f'{self.output_dir}/{key}'
        obj = self.bucket.Object(s3key)
        bio = io.BytesIO(sjson.encode('UTF-8'))
        
        obj.upload_fileobj(bio)

    def readall(self) -> List[twitter.TweetCopy]:
        tweets: List[twitter.TweetCopy] = []
        id_cache: Set[int] = set()

        object_summaries = self.bucket.objects.filter(Prefix=self.output_dir)

        for entry in object_summaries:
            resp = entry.get()
            loaded = json.load(resp['Body'])
            _tweets = [twitter.TweetCopy.retrieve(e) for e in loaded]
            logger.info(f'{len(_tweets)} tweets retrieved')
            for tw in _tweets:
                if tw.tweet_id in id_cache:
                    logger.warning('ignoring duplicate tweet: %s', tw.tweet_id)
                else:
                    tweets.append(tw)
                    id_cache.add(tw.tweet_id)

        # 新しい順
        tweets.sort(key=lambda e: e.tweet_id)
        tweets.reverse()

        logger.info(f'total: {len(tweets)} tweets')
        return tweets


class SupportPartitioningRule(Protocol):
    def dispatch(
            self,
            partitions: Dict[str, List[twitter.RunReport]],
            report: twitter.RunReport) -> None:
        ...


class PartitioningRuleByDate:
    def dispatch(
            self,
            partitions: Dict[str, List[twitter.RunReport]],
            report: twitter.RunReport,
        ) -> None:

        date = report.timestamp.date().isoformat()
        if date not in partitions:
            partitions[date] = []
        partitions[date].append(report)


class PartitioningRuleByUser:
    def dispatch(
            self,
            partitions: Dict[str, List[twitter.RunReport]],
            report: twitter.RunReport,
        ) -> None:

        if report.reporter not in partitions:
            partitions[report.reporter] = []
        partitions[report.reporter].append(report)
        # TODO 今のところ解析不能ツイートは含まれない想定の
        # コードになっているが、将来そうしたツイートもまとめて
        # ここを通過することになった場合に対応が必要。


class PartitioningRuleByQuest:
    def dispatch(
            self,
            partitions: Dict[str, List[twitter.RunReport]],
            report: twitter.RunReport,
        ) -> None:

        detector = freequest.defaultDetector
        qid = detector.get_quest_id(report.chapter, report.place)
        if qid not in partitions:
            partitions[qid] = []
        partitions[qid].append(report)


class AbstractRecorder:
    # TODO サブクラスとスーパークラスに処理が分散しており
    # コードを追うときにいったりきたりが必要。
    # 正直いっていまいちな設計。そのうち考え直したい

    def __init__(
            self,
            partitioningRule: SupportPartitioningRule,
            formats: Sequence[OutputFormat],
        ):
        self.partitions: Dict[str, List[twitter.RunReport]] = {}
        self.partitioningRule = partitioningRule
        self.formats = formats

    def add(self, report: twitter.RunReport) -> None:
        self.partitioningRule.dispatch(self.partitions, report)
    
    def _get_original_json(self, key: str) -> List[Dict[str, Any]]:
        raise NotImplementedError

    def _load_json(self, buf: str) -> List[Dict[str, Any]]:
        return json.loads(buf, object_hook=AbstractRecorder._load_hook)

    def _get_output_stream(self, key: str, fmt: str) -> BinaryIO:
        raise NotImplementedError

    def _close_stream(self, stream: BinaryIO) -> None:
        raise NotImplementedError

    def save(self, force: bool = False, ignore_original: bool = False):
        for key, reports in self.partitions.items():
            if len(reports) == 0:
                continue
            if ignore_original:
                logger.info(f'ignore original: {key}.json')
                original = []
            else:
                original = self._get_original_json(key)

            for outputFormat in self.formats:
                _, ext = outputFormat.value
                logger.info(f'target file: {key}.{ext}')
                processor = create_processor(outputFormat)
                merged_reports = processor.merge(reports, original)
                if not force and merged_reports == original:
                    logger.info(f'no new reports to write {key}.{ext}, skip')
                    continue
                stream = self._get_output_stream(key, ext)
                logger.info(f'writing reports to {key}.{ext}')
                processor.dump(merged_reports, stream, key=key)
                logger.info('done')
                self._close_stream(stream)

    @staticmethod
    def _load_hook(d: Dict[str, Any]) -> Dict[str, Any]:
        if 'timestamp' in d:
            ts = d['timestamp']
            d['timestamp'] = datetime.fromisoformat(ts)
        return d


class FilesystemRecorder(AbstractRecorder):
    def __init__(
            self,
            rootdir: str,
            partitioningRule: SupportPartitioningRule,
            formats: Sequence[OutputFormat],
        ):
        super().__init__(partitioningRule, formats)
        if not os.path.exists(rootdir):
            logger.info(f'create a directory "{rootdir}"')
            os.makedirs(rootdir)
        self.rootdir = rootdir

    def _get_original_json(self, key: str) -> List[Dict[str, Any]]:
        filepath = os.path.join(self.rootdir, f'{key}.json')
        if not os.path.exists(filepath):
            return []
        with open(filepath) as fp:
            buf = fp.read()
            if not buf:
                return []
            return self._load_json(buf)

    def _get_output_stream(self, key: str, fmt: str) -> BinaryIO:
        filepath = os.path.join(self.rootdir, f'{key}.{fmt}')
        return open(filepath, 'wb')

    def _close_stream(self, stream: BinaryIO) -> None:
        stream.close()


class AmazonS3Recorder(AbstractRecorder):
    def __init__(
            self,
            bucket: str,
            output_dir: str,
            partitioningRule: SupportPartitioningRule,
            formats: Sequence[OutputFormat],
        ):
        super().__init__(partitioningRule, formats)
        self.s3 = boto3.resource('s3')
        self.s3client = boto3.client('s3')
        self.bucket = self.s3.Bucket(bucket)
        self.output_dir = output_dir
        self.key_stream_pairs: Dict[str, BinaryIO] = {}

    def _exists(self, s3key: str) -> bool:
        try:
            self.s3client.head_object(
                Bucket=self.bucket.name,
                Key=s3key,
            )
            return True

        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            # Unexpceted Error
            raise

    def _get_original_json(self, key: str) -> List[Dict[str, Any]]:
        s3key = f'{self.output_dir}/{key}.json'
        logger.info(f'get s3://{self.bucket.name}/{s3key}')
        if not self._exists(s3key):
            return []
        
        bio = io.BytesIO()
        self.bucket.download_fileobj(s3key, bio)
        return self._load_json(bio.getvalue().decode('UTF-8'))

    def _get_output_stream(self, key: str, fmt: str) -> BinaryIO:
        bio = io.BytesIO()
        # この時点で key を記憶しておかないと後で stream を渡されたときに
        # 対応する key を復元できなくなる
        s3key = f'{self.output_dir}/{key}.{fmt}'
        self.key_stream_pairs[s3key] = bio
        return bio

    def _close_stream(self, stream: BinaryIO) -> None:
        for s3key, bio in self.key_stream_pairs.items():
            if bio is not stream:
                continue
            obj = self.bucket.Object(s3key)
            if s3key.endswith('.json'):
                content_type = 'application/json'
            elif s3key.endswith('.html'):
                content_type = 'text/html'
            elif s3key.endswith('.txt'):
                content_type = 'text/plain'
            else:
                content_type = 'application/octet-stream'
            logger.info(f'put s3://{self.bucket.name}/{s3key}, content_type={content_type}')
            bio.seek(0)
            obj.upload_fileobj(stream, ExtraArgs={'ContentType': content_type})
            stream.close()
            return
        raise ValueError('could not put a stream object to S3')


class Processor:
    def _make_index(self, original: List[Dict[str, Any]]):
        s = set()
        for r in original:
            s.add(r['id'])
        return s

    def merge(self,
            reports: List[twitter.RunReport],
            original: List[Dict[str, Any]],
        ):
        logger.info('original reports: %d', len(original))
        merged = copy.deepcopy(original)
        index = self._make_index(merged)

        c = 0
        for report in reports:
            if report.tweet_id not in index:
                merged.append(report.as_dict())
                c += 1
        merged.sort(key=lambda e: e['id'])
        merged.reverse()
        logger.info('additional reports: %d', c)
        return merged

    def dump(
            self,
            merged_reports: List[Dict[str, Any]],
            stream: BinaryIO,
            **kwargs,
        ):
        raise NotImplementedError


class JSONProcessor(Processor):
    def dump(
            self,
            merged_reports: List[Dict[str, Any]],
            stream: BinaryIO,
            **kwargs,
        ):
        s = json.dumps(
            merged_reports,
            ensure_ascii=False,
            default=json_serialize_helper,
        )
        stream.write(s.encode('UTF-8'))


class TextProcessor(Processor):
    def dump(
            self,
            merged_reports: List[Dict[str, Any]],
            stream: BinaryIO,
            **kwargs,
        ):
        raise NotImplementedError


class DateHTMLProcessor(Processor):
    template_html = 'report_bydate.jinja2'

    def dump(
            self,
            merged_reports: List[Dict[str, Any]],
            stream: BinaryIO,
            **kwargs,
        ):
        freequest_reports = [r for r in merged_reports if r['freequest']]
        event_reports = [r for r in merged_reports if not r['freequest']]
        today = kwargs['key']
        today_obj = date.fromisoformat(today)
        yesterday = (today_obj + timedelta(days=-1)).isoformat()
        tomorrow = (today_obj + timedelta(days=+1)).isoformat()
        template = jinja2_env.get_template(self.template_html)
        html = template.render(
            freequest_reports=freequest_reports,
            event_reports=event_reports,
            yesterday=yesterday,
            today=today,
            tomorrow=tomorrow,
        )
        stream.write(html.encode('UTF-8'))


class UserHTMLProcessor(Processor):
    template_html = 'report_byuser.jinja2'

    def dump(
            self,
            merged_reports: List[Dict[str, Any]],
            stream: BinaryIO,
            **kwargs,
        ):
        freequest_reports = [r for r in merged_reports if r['freequest']]
        event_reports = [r for r in merged_reports if not r['freequest']]
        template = jinja2_env.get_template(self.template_html)
        html = template.render(
            freequest_reports=freequest_reports,
            event_reports=event_reports,
            reporter=kwargs['key'],
        )
        stream.write(html.encode('UTF-8'))


class QuestHTMLProcessor(Processor):
    template_html = 'report_byquest.jinja2'

    def dump(
            self,
            merged_reports: List[Dict[str, Any]],
            stream: BinaryIO,
            **kwargs,
        ):
        template = jinja2_env.get_template(self.template_html)
        html = template.render(
            reports=merged_reports,
            quest=freequest.defaultDetector.get_quest_name(kwargs['key']),
        )
        stream.write(html.encode('UTF-8'))


def create_processor(fmt: OutputFormat) -> Processor:
    if fmt == OutputFormat.JSON:
        return JSONProcessor()
    elif fmt == OutputFormat.TEXT:
        return TextProcessor()
    elif fmt == OutputFormat.DATEHTML:
        return DateHTMLProcessor()
    elif fmt == OutputFormat.USERHTML:
        return UserHTMLProcessor()
    elif fmt == OutputFormat.QUESTHTML:
        return QuestHTMLProcessor()

    raise ValueError(f'Unsupported format: {fmt}')
