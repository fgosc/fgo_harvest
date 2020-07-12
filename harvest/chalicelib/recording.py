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
from jinja2 import (
    Environment,
    PackageLoader,
    select_autoescape,
)

from . import freequest, storage, twitter

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


class Recorder:
    def __init__(
            self,
            partitioningRule: SupportPartitioningRule,
            fileStorage: storage.SupportStorage,
            basedir: str,
            formats: Sequence[OutputFormat],
        ):
        self.partitions: Dict[str, List[twitter.RunReport]] = {}
        self.partitioningRule = partitioningRule
        self.fileStorage = fileStorage
        self.basedir = basedir
        self.formats = formats
        self.basepath = fileStorage.path_object(self.basedir)

    def add(self, report: twitter.RunReport) -> None:
        self.partitioningRule.dispatch(self.partitions, report)

    @staticmethod
    def _load_hook(d: Dict[str, Any]) -> Dict[str, Any]:
        if 'timestamp' in d:
            ts = d['timestamp']
            d['timestamp'] = datetime.fromisoformat(ts)
        return d

    def _get_original_json(self, key: str) -> List[Dict[str, Any]]:
        keypath = str(self.basepath / key)
        logger.info('retrieving original json: %s', keypath)
        text = self.fileStorage.get_as_text(keypath)
        return json.loads(text, object_hook=Recorder._load_hook)

    def save(self, force: bool = False, ignore_original: bool = False):
        for key, reports in self.partitions.items():
            if len(reports) == 0:
                continue
            if ignore_original:
                logger.info(f'ignore original json: {key}.json')
                original = []
            else:
                original = self._get_original_json(key)

            for outputFormat in self.formats:
                _, ext = outputFormat.value
                targetfile = f'{key}.{ext}'
                logger.info(f'target file: {targetfile}')
                processor = create_processor(outputFormat)
                merged_reports = processor.merge(reports, original)
                if not force and merged_reports == original:
                    logger.info(f'no new reports to write {targetfile}, skip')
                    continue
                path = str(self.basepath / targetfile)
                logger.info('report path: %s', path)
                stream = self.fileStorage.get_output_stream(path)
                logger.info('writing reports to %s', targetfile)
                processor.dump(merged_reports, stream, key=key)
                logger.info('done')
                self.fileStorage.close_output_stream(stream)


class Processor:
    def _make_index(self, original: List[Dict[str, Any]]):
        s = set()
        for r in original:
            s.add(r['id'])
        return s

    def merge(
            self,
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
