import copy
import json
import io
import os
import pathlib
from datetime import date, datetime, timedelta
from enum import Enum
from logging import getLogger
from operator import itemgetter
from typing import (
    cast, Any, BinaryIO, Dict, List,
    Sequence, Set, Union,
)
from typing_extensions import Protocol

import boto3  # type: ignore
from jinja2 import (
    Environment,
    PackageLoader,
    select_autoescape,
)

from . import freequest, storage, timezone, twitter

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
    USERLISTHTML = ('userlisthtml', 'html')
    QUESTLISTHTML = ('questlisthtml', 'html')


class ErrorOutputFormat(Enum):
    JSON = ('json', 'json')
    HTML = ('html', 'html')


class AbstractTweetStorage:
    """
        TODO できればストレージ固有の処理は storage モジュールに統合したい。
        そうすると、このクラスの役割は
        - List[twitter.TweetCopy] の永続化
        - readall() による List[twitter.TweetCopy] の復元
        だけになる。
        ただし優先度は低い。
        また、簡単に統合できるかどうかはわからない。ざっと現実装を見た感じでは
        - 書き込み処理
        - パスを指定すると、その下にあるファイルをなめて
          次々に stream を返すようなイテレータを返す処理
        が提供されればよさそう。
    """
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


class SupportDictConversible(Protocol):
    def as_dict(self) -> Dict[str, Any]:
        ...

    def get_id(self) -> Any:
        ...


class SupportPartitioningRule(Protocol):
    def dispatch(
        self,
        partitions: Dict[str, List[SupportDictConversible]],
        report: twitter.RunReport,
    ) -> None:
        ...


class SupportStatefulPartitioningRule(Protocol):
    def setup(
        self,
        fileStorage: storage.SupportStorage,
        basepath: pathlib.PurePath,
    ) -> None:
        ...

    def dispatch(
        self,
        partitions: Dict[str, List[SupportDictConversible]],
        report: twitter.RunReport,
    ) -> None:
        ...


class PartitioningRuleByDate:
    def dispatch(
        self,
        partitions: Dict[str, List[SupportDictConversible]],
        report: twitter.RunReport,
    ) -> None:

        date = report.timestamp.date().isoformat()
        if date not in partitions:
            partitions[date] = []
        partitions[date].append(report)


class PartitioningRuleByUser:
    def dispatch(
        self,
        partitions: Dict[str, List[SupportDictConversible]],
        report: twitter.RunReport,
    ) -> None:

        if report.reporter not in partitions:
            partitions[report.reporter] = []
        partitions[report.reporter].append(report)


class PartitioningRuleByQuest:
    def dispatch(
        self,
        partitions: Dict[str, List[SupportDictConversible]],
        report: twitter.RunReport,
    ) -> None:

        detector = freequest.defaultDetector
        year = report.timestamp.year
        qid = detector.get_quest_id(report.chapter, report.place, year)
        if qid not in partitions:
            partitions[qid] = []
        partitions[qid].append(report)


class UserListElement:
    def __init__(self, uid):
        self.uid = uid

    def as_dict(self) -> Dict[str, Any]:
        """
            for SupportDictConversible
        """
        return {
            'id': self.uid
        }

    def get_id(self) -> Any:
        """
            for SupportDictConversible
        """
        return self.uid


class PartitioningRuleByUserList:
    """
        既存の PartitioningRule の枠組みを利用して
        user list を作る
    """
    def __init__(self):
        self.existing_reporters: Set[str] = set()

    def dispatch(
        self,
        partitions: Dict[str, List[SupportDictConversible]],
        report: twitter.RunReport,
    ) -> None:

        if report.reporter in self.existing_reporters:
            return
        self.existing_reporters.add(report.reporter)

        e = UserListElement(report.reporter)

        # パーティションは常に1つ
        if 'all' not in partitions:
            partitions['all'] = []
        partitions['all'].append(e)


class QuestListElement:
    def __init__(
        self,
        chapter: str,
        place: str,
        timestamp: datetime,
        count: int = 1,
    ):
        detector = freequest.defaultDetector
        self.quest_id = detector.get_quest_id(chapter, place, timestamp.year)
        self.is_freequest = detector.is_freequest(chapter, place)
        self.quest_name = detector.get_quest_name(self.quest_id)
        self.chapter = chapter
        self.place = place
        self.since = timestamp
        self.latest = timestamp
        self.count = count

    def countup(self, timestamp: datetime) -> None:
        self.count += 1
        self.latest = timestamp

    def as_dict(self) -> Dict[str, Any]:
        """
            for SupportDictConversible
        """
        return {
            'id': self.quest_id,
            'name': self.quest_name,
            'is_freequest': self.is_freequest,
            'chapter': self.chapter,
            'place': self.place,
            'since': self.since.isoformat(),
            'latest': self.latest,
            'count': self.count,
        }

    def get_id(self) -> Any:
        """
            for SupportDictConversible
        """
        return self.quest_id


class PartitioningRuleByQuestList:
    """
        既存の PartitioningRule の枠組みを利用して
        quest list を作る
    """
    def __init__(self, rebuild=False):
        self.quest_dict: Dict[str, QuestListElement] = {}
        self.rebuild = rebuild

    def setup(
        self,
        fileStorage: storage.SupportStorage,
        basepath: pathlib.PurePath,
    ) -> None:
        # rebuild ならば蓄積されたデータは使わないので、
        # setup の必要はない。
        if self.rebuild:
            return

        # 既存クエストであっても countup をする必要があるため、
        # 最初に過去データをすべてロードしておく必要がある。
        filepath = str(basepath / 'all.json')
        text = fileStorage.get_as_text(filepath)

        def _load_hook(d: Dict[str, Any]) -> Dict[str, Any]:
            if 'since' in d:
                ts = d['since']
                d['since'] = datetime.fromisoformat(ts)
            if 'latest' in d:
                ts = d['latest']
                d['latest'] = datetime.fromisoformat(ts)
            return d

        quest_list = json.loads(text, object_hook=_load_hook)
        for q in quest_list:
            e = QuestListElement(q['chapter'], q['place'], q['since'])
            if e.quest_id != q['id']:
                logger.error(f'json: {q}')
                raise ValueError('incorrect data: {}'.format(q['id']))
            # TODO JSON の後方互換性
            e.latest = q.get('latest', q['since'])
            e.count = q.get('count', 0)
            self.quest_dict[e.quest_id] = e

    def dispatch(
        self,
        partitions: Dict[str, List[SupportDictConversible]],
        report: twitter.RunReport,
    ) -> None:

        e = QuestListElement(report.chapter, report.place, report.timestamp)

        if e.quest_id not in self.quest_dict:
            self.quest_dict[e.quest_id] = e
            new_entry = True
        else:
            existing_e = self.quest_dict[e.quest_id]
            # より古いデータが見つかった場合は、その値で since を上書き
            if existing_e.since < e.since:
                existing_e.since = e.since

            existing_e.countup(e.latest)
            new_entry = False

        actual_e = self.quest_dict[e.quest_id]

        # パーティションは常に1つ
        if 'all' not in partitions:
            partitions['all'] = [e for e in self.quest_dict.values()]

        ps = partitions['all']
        if new_entry:
            ps.append(actual_e)

        partitions['all'] = ps


class Recorder:
    def __init__(
        self,
        partitioningRule: Union[
            SupportPartitioningRule,
            SupportStatefulPartitioningRule
        ],
        fileStorage: storage.SupportStorage,
        basedir: str,
        formats: Sequence[OutputFormat],
    ):
        self.partitions: Dict[str, List[SupportDictConversible]] = {}
        self.partitioningRule = partitioningRule
        self.fileStorage = fileStorage
        self.basedir = basedir
        self.formats = formats
        self.basepath = fileStorage.path_object(self.basedir)
        # for SupportStatefulPartitioningRule
        if hasattr(self.partitioningRule, 'setup'):
            statefulPartitioningRule = cast(
                SupportStatefulPartitioningRule,
                self.partitioningRule,
            )
            statefulPartitioningRule.setup(self.fileStorage, self.basepath)

    def add(self, report: twitter.RunReport) -> None:
        self.partitioningRule.dispatch(self.partitions, report)

    @staticmethod
    def _load_hook(d: Dict[str, Any]) -> Dict[str, Any]:
        if 'timestamp' in d:
            ts = d['timestamp']
            d['timestamp'] = datetime.fromisoformat(ts)
        return d

    def _get_original_json(self, key: str) -> List[Dict[str, Any]]:
        keypath = str(self.basepath / f'{key}.json')
        logger.info('retrieving original json: %s', keypath)
        text = self.fileStorage.get_as_text(keypath)
        if text == '':
            return []
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
                merger = ReportMerger()
                merged_reports = merger.merge(reports, original)
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


class PageProcessorSupport(Protocol):
    def dump(
        self,
        merged_reports: List[Dict[str, Any]],
        stream: BinaryIO,
        **kwargs,
    ):
        ...


class ReportMerger:
    """
        original の各要素に id エントリが必ずあり、それが
        additional_items の id と一致する型であることが
        暗黙的に要求される。
    """
    def _make_index(self, original: List[Dict[str, Any]]):
        s = set()
        for r in original:
            s.add(r['id'])
        return s

    def merge(
        self,
        additional_items: List[SupportDictConversible],
        original: List[Dict[str, Any]],
    ):
        logger.info('original reports: %d', len(original))
        merged = copy.deepcopy(original)
        index = self._make_index(merged)

        c = 0
        for item in additional_items:
            if item.get_id() not in index:
                merged.append(item.as_dict())
                c += 1
        merged.sort(key=lambda e: e['id'])
        merged.reverse()
        logger.info('additional reports: %d', c)
        return merged


class JSONPageProcessor:
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


class TextPageProcessor:
    def dump(
        self,
        merged_reports: List[Dict[str, Any]],
        stream: BinaryIO,
        **kwargs,
    ):
        # TODO 実装
        raise NotImplementedError


class DateHTMLPageProcessor:
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


class UserHTMLPageProcessor:
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


class QuestHTMLPageProcessor:
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


class UserListHTMLPageProcessor:
    template_html = 'all_user.jinja2'

    def dump(
        self,
        merged_reports: List[Dict[str, Any]],
        stream: BinaryIO,
        **kwargs,
    ):
        template = jinja2_env.get_template(self.template_html)
        html = template.render(
            users=sorted(merged_reports, key=itemgetter('id')),
        )
        stream.write(html.encode('UTF-8'))


class QuestListHTMLPageProcessor:
    template_html = 'all_quest.jinja2'

    def dump(
        self,
        merged_reports: List[Dict[str, Any]],
        stream: BinaryIO,
        **kwargs,
    ):
        freequests = [r for r in merged_reports if r['is_freequest']]
        eventquests = [r for r in merged_reports if not r['is_freequest']]
        template = jinja2_env.get_template(self.template_html)
        html = template.render(
            freequests=sorted(freequests, key=itemgetter('id')),
            eventquests=sorted(
                            eventquests,
                            key=itemgetter('since'),
                            reverse=True,
                        ),
        )
        stream.write(html.encode('UTF-8'))


def create_processor(fmt: OutputFormat) -> PageProcessorSupport:
    if fmt == OutputFormat.JSON:
        return JSONPageProcessor()
    elif fmt == OutputFormat.TEXT:
        return TextPageProcessor()
    elif fmt == OutputFormat.DATEHTML:
        return DateHTMLPageProcessor()
    elif fmt == OutputFormat.USERHTML:
        return UserHTMLPageProcessor()
    elif fmt == OutputFormat.QUESTHTML:
        return QuestHTMLPageProcessor()
    elif fmt == OutputFormat.USERLISTHTML:
        return UserListHTMLPageProcessor()
    elif fmt == OutputFormat.QUESTLISTHTML:
        return QuestListHTMLPageProcessor()

    raise ValueError(f'Unsupported format: {fmt}')


class LatestDatePageBuilder:
    def __init__(
        self,
        fileStorage: storage.SupportStorage,
        basedir: str,
    ):
        self.fileStorage = fileStorage
        self.basedir = basedir
        self.basepath = fileStorage.path_object(basedir)

    def _find_latest_page(self, origin: datetime) -> str:
        # 30 は適当な数値。それだけさかのぼれば何かしらの
        # ファイルがあるだろうという期待の数値。
        # ふつうは当日か前日のデータが見つかるだろう。
        for i in range(30):
            target_date = origin - timedelta(days=i)
            filename = '{}.html'.format(target_date.date().isoformat())
            keypath = str(self.basepath / filename)
            if self.fileStorage.exists(keypath):
                return str(keypath)
        return ''

    def _latest_path(self):
        return str(self.basepath / 'latest.html')

    def build(self):
        """
            プログラム実行時点の日付で yyyy-MM-dd.html を探す。
            なければ1日前に戻る。これを繰り返して最新の
            yyyy-MM-dd.html を特定する。特定できたらこれを
            latest.html という名前でコピーする。
        """
        now = timezone.now()
        src = self._find_latest_page(now)
        if not src:
            logger.warning('skip building the latest page')
            return
        dest = self._latest_path()
        logger.info('building the latest page from "%s"', src)
        self.fileStorage.copy(src, dest)


class ErrorPageRecorder:
    def __init__(
        self,
        fileStorage: storage.SupportStorage,
        basedir: str,
        key: str,
        formats: Sequence[ErrorOutputFormat],
    ):
        self.fileStorage = fileStorage
        self.basedir = basedir
        self.key = key
        self.formats = formats
        self.errors: List[twitter.ParseErrorTweet] = []
        self.basepath = fileStorage.path_object(basedir)

    def add_error(self, tweet: twitter.ParseErrorTweet) -> None:
        self.errors.append(tweet)

    @staticmethod
    def _load_hook(d: Dict[str, Any]) -> Dict[str, Any]:
        if 'timestamp' in d:
            ts = d['timestamp']
            d['timestamp'] = datetime.fromisoformat(ts)
        return d

    def _get_original_json(self, key: str) -> List[Dict[str, Any]]:
        keypath = str(self.basepath / f'{key}.json')
        logger.info('retrieving original json: %s', keypath)
        text = self.fileStorage.get_as_text(keypath)
        if text == '':
            return []
        return json.loads(text)

    def save(self, force: bool = False, ignore_original: bool = False) -> None:
        if len(self.errors) == 0:
            logger.info('no error tweets')
            return
        if ignore_original:
            logger.info(f'ignore original json: {self.key}.json')
            original = []
        else:
            original = self._get_original_json(self.key)

        for outputFormat in self.formats:
            processor = create_errorpage_processor(outputFormat)
            original_tweets = [
                twitter.ParseErrorTweet.retrieve(d) for d in original
            ]
            merger = ErrorMerger()
            merged_errors = merger.merge(self.errors, original_tweets)
            if not force and merged_errors == original_tweets:
                logger.info('no new tweets to write to error page, skip')
                continue

            _, ext = outputFormat.value
            path = str(self.basepath / f'{self.key}.{ext}')
            stream = self.fileStorage.get_output_stream(path)
            processor.dump(merged_errors, stream)
            self.fileStorage.close_output_stream(stream)


class ErrorMerger:
    def _make_index(self, original: List[twitter.ParseErrorTweet]):
        s = set()
        for r in original:
            s.add(r.tweet_id)
        return s

    def merge(
        self,
        errors: List[twitter.ParseErrorTweet],
        original: List[twitter.ParseErrorTweet],
    ):
        logger.info('original error tweets: %d', len(original))
        merged = copy.deepcopy(original)
        index = self._make_index(merged)

        c = 0
        for err in errors:
            if err.tweet_id not in index:
                merged.append(err)
                c += 1
        merged.sort(key=lambda e: e.tweet_id)
        merged.reverse()
        logger.info('additional error tweets: %d', c)
        return merged


class ErrorPageProcessorSupport(Protocol):
    def dump(
        self,
        errors: List[twitter.ParseErrorTweet],
        stream: BinaryIO,
    ) -> None:
        ...


class JSONErrorPageProcessor:
    def dump(
        self,
        errors: List[twitter.ParseErrorTweet],
        stream: BinaryIO,
    ) -> None:
        data = [tw.as_dict() for tw in errors]
        s = json.dumps(
            data,
            ensure_ascii=False,
            default=json_serialize_helper,
        )
        stream.write(s.encode('UTF-8'))


class HTMLErrorPageProcessor:
    template_html = 'error_report.jinja2'

    def dump(
        self,
        errors: List[twitter.ParseErrorTweet],
        stream: BinaryIO,
    ) -> None:
        template = jinja2_env.get_template(self.template_html)
        html = template.render(tweets=errors)
        stream.write(html.encode('utf-8'))


def create_errorpage_processor(
        fmt: ErrorOutputFormat) -> ErrorPageProcessorSupport:

    if fmt == ErrorOutputFormat.JSON:
        return JSONErrorPageProcessor()
    elif fmt == ErrorOutputFormat.HTML:
        return HTMLErrorPageProcessor()

    raise ValueError(f'Unsupported format: {fmt}')
