import copy
import csv
import io
import json
import pathlib
from datetime import date, datetime, timedelta
from enum import Enum
from logging import getLogger
from operator import itemgetter
from typing import (
    cast, Any, BinaryIO, Dict, List, Protocol,
    Sequence, Set, Union,
)

from dateutil.relativedelta import relativedelta  # type: ignore
from jinja2 import (  # type: ignore
    Environment,
    PackageLoader,
    select_autoescape,
)

from . import (
    freequest,
    helper,
    model,
    storage,
    timezone,
    twitter,
)

logger = getLogger(__name__)
jinja2_env = Environment(
    loader=PackageLoader('chalicelib', 'templates'),
    autoescape=select_autoescape(['html'])
)
month_format = "%Y-%m"


class OutputFormat(Enum):
    JSON = ('json', 'json')
    CSV = ('csv', 'csv')
    USER_HTML = ('userhtml', 'html')
    DATE_HTML = ('datehtml', 'html')
    MONTH_HTML = ('monthhtml', 'html')
    FGO1HRUN_HTML = ('fgo1hrunhtml', 'html')
    QUEST_HTML = ('questhtml', 'html')
    USER_LIST_HTML = ('userlisthtml', 'html')
    QUEST_LIST_HTML = ('questlisthtml', 'html')
    FGO1HRUN_LIST_HTML = ('fgo1hrunlisthtml', 'html')


class ErrorOutputFormat(Enum):
    JSON = ('json', 'json')
    HTML = ('html', 'html')


class SupportPartitioningRule(Protocol):
    def dispatch(
        self,
        partitions: Dict[str, List[model.SupportDictConversible]],
        report: model.RunReport,
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
        partitions: Dict[str, List[model.SupportDictConversible]],
        report: model.RunReport,
    ) -> None:
        ...


class SupportSkipSaveRule(Protocol):
    def scan_report(self, report: model.RunReport) -> None:
        ...

    def match(self, key: str) -> bool:
        ...


class PartitioningRuleByDate:
    def dispatch(
        self,
        partitions: Dict[str, List[model.SupportDictConversible]],
        report: model.RunReport,
    ) -> None:

        date = report.timestamp.date().isoformat()
        if date not in partitions:
            partitions[date] = []
        partitions[date].append(report)


def get_week_start_day(target_date: date, start_day: int) -> date:
    """
        target_date よりも前で、最も近い開始曜日の日付を返す
    """
    delta = target_date.weekday() - start_day
    if delta < 0:
        delta += 7
    return target_date - timedelta(days=delta)


class PartitioningRuleBy1HRun:
    def __init__(self, start_day: int) -> None:
        """
            start_day: 0-6 (0: Monday, 6: Sunday)
            週の開始曜日
        """
        if start_day < 0 or start_day > 6:
            raise ValueError("start_day must be 0-6")
        self.start_day = start_day

    def dispatch(
        self,
        partitions: Dict[str, List[model.SupportDictConversible]],
        report: model.RunReport,
    ) -> None:

        if "#fgo_1h_run" not in report.note.lower().split():
            return

        target_date = report.timestamp.date()
        week_start = get_week_start_day(target_date, self.start_day)

        # 表示上は土曜にする (FGO_1H_run の実施日が土曜のため)
        # NOTE: たぶんこの実装だと start_day = 6 (SUN) のときバグりそう。
        # ただ、指定する可能性はほぼないので考えないことにする。
        display_date = week_start + timedelta(days=5 - self.start_day)
        display_date_str = display_date.isoformat()
        if display_date_str not in partitions:
            partitions[display_date_str] = []
        partitions[display_date_str].append(report)


class PartitioningRuleByMonth:
    def dispatch(
        self,
        partitions: Dict[str, List[model.SupportDictConversible]],
        report: model.RunReport,
    ) -> None:

        month = report.timestamp.date().strftime(month_format)
        if month not in partitions:
            partitions[month] = []
        partitions[month].append(report)


class PartitioningRuleByUser:
    def dispatch(
        self,
        partitions: Dict[str, List[model.SupportDictConversible]],
        report: model.RunReport,
    ) -> None:

        if report.reporter not in partitions:
            partitions[report.reporter] = []
        partitions[report.reporter].append(report)


class PartitioningRuleByQuest:
    def dispatch(
        self,
        partitions: Dict[str, List[model.SupportDictConversible]],
        report: model.RunReport,
    ) -> None:

        qid = report.quest_id
        if qid not in partitions:
            partitions[qid] = []
        partitions[qid].append(report)


class UserListElement:
    def __init__(self, uid):
        self.uid = uid

    def as_dict(self) -> Dict[str, Any]:
        """
            for model.SupportDictConversible
        """
        return {
            'id': self.uid
        }

    def get_id(self) -> Any:
        """
            for model.SupportDictConversible
        """
        return self.uid

    def equals(self, obj: Any) -> bool:
        """
            for model.SupportDictConversible
        """
        if isinstance(obj, dict):
            return self.uid == obj.get('id')
        if isinstance(obj, UserListElement):
            return self.uid == cast(UserListElement, obj).uid
        return False


class PartitioningRuleByUserList:
    """
        既存の PartitioningRule の枠組みを利用して
        user list を作る
    """
    def __init__(self) -> None:
        self.existing_reporters: Set[str] = set()

    def dispatch(
        self,
        partitions: Dict[str, List[model.SupportDictConversible]],
        report: model.RunReport,
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
        quest_id: str,
        chapter: str,
        place: str,
        timestamp: datetime,
        is_freequest: bool,
        count: int = 1,
    ):
        self.quest_id = quest_id
        self.chapter = chapter
        self.place = place
        self.since = timestamp
        self.latest = timestamp
        self.is_freequest = is_freequest
        self.count = count
        detector = freequest.defaultDetector
        try:
            self.quest_name = detector.get_quest_name(quest_id)
        except KeyError:
            _qid = detector.get_quest_id(chapter, place, timestamp.year)
            if quest_id != _qid:
                raise ValueError(
                    f'qid mismatch: {quest_id} != {_qid} ({chapter} {place})'
                )
            self.quest_name = detector.get_quest_name(quest_id)

    def countup(self, timestamp: datetime) -> None:
        self.count += 1
        if timestamp > self.latest:
            self.latest = timestamp

    def as_dict(self) -> Dict[str, Any]:
        """
            for model.SupportDictConversible
        """
        return {
            'id': self.quest_id,
            'name': self.quest_name,
            'is_freequest': self.is_freequest,
            'chapter': self.chapter,
            'place': self.place,
            'since': self.since.isoformat(),
            'latest': self.latest.isoformat(),
            'count': self.count,
        }

    def get_id(self) -> Any:
        """
            for model.SupportDictConversible
        """
        return self.quest_id

    def __str__(self) -> str:
        return f'<QuestListElement: {self.as_dict()}>'

    def __repr__(self) -> str:
        return f'<QuestListElement: {self.as_dict()}>'

    def equals(self, obj: Any) -> bool:
        """
            for model.SupportDictConversible
        """
        if isinstance(obj, dict):
            return self.as_dict() == obj
        if isinstance(obj, QuestListElement):
            return self.as_dict() == obj.as_dict()
        return False


class PartitioningRuleByQuestList:
    """
        既存の PartitioningRule の枠組みを利用して
        quest list を作る
    """
    def __init__(self, rebuild: bool = False):
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

        if text.strip() == "":
            quest_list = []
        else:
            quest_list = json.loads(text, object_hook=_load_hook)

        for q in quest_list:
            e = QuestListElement(
                q['id'],
                q['chapter'],
                q['place'],
                q['since'],
                q['is_freequest'],
            )
            if e.quest_id != q['id']:
                logger.error(f'json: {q}')
                raise ValueError('incorrect data: {}'.format(q['id']))
            # TODO JSON の後方互換性
            e.latest = q.get('latest', q['since'])
            e.count = q.get('count', 0)
            self.quest_dict[e.quest_id] = e

    def dispatch(
        self,
        partitions: Dict[str, List[model.SupportDictConversible]],
        report: model.RunReport,
    ) -> None:

        e = QuestListElement(
            report.quest_id,
            report.chapter,
            report.place,
            report.timestamp,
            report.is_freequest,
        )

        if e.quest_id not in self.quest_dict:
            self.quest_dict[e.quest_id] = e
            new_entry = True
        else:
            existing_e = self.quest_dict[e.quest_id]
            # より古いデータが見つかった場合は、その値で since を上書き
            if e.since < existing_e.since:
                existing_e.since = e.since

            existing_e.countup(e.latest)
            new_entry = False

        actual_e = self.quest_dict[e.quest_id]

        # パーティションは常に all のみ
        if 'all' not in partitions:
            partitions['all'] = [e for e in self.quest_dict.values()]
            # パーティション初期化時に self.quest_dict の中身をコピー
            # するが、この時点で e は partitions に登録済みであることが
            # 確実なので、以降の処理は必要ない。
            return

        ps = partitions['all']
        if new_entry:
            ps.append(actual_e)

        partitions['all'] = ps


class FGO1HRunWeekListElement:
    def __init__(self, week_start: date, display_date: date) -> None:
        self.week_start = week_start
        self.display_date = display_date

    def as_dict(self) -> Dict[str, Any]:
        """
            for model.SupportDictConversible
        """
        return {
            'id': self.display_date.isoformat(),
            'week_start': self.week_start.isoformat(),
            'display_date': self.display_date.isoformat(),
        }

    def get_id(self) -> Any:
        """
            for model.SupportDictConversible
        """
        return self.display_date.isoformat()

    def equals(self, obj: Any) -> bool:
        """
            for model.SupportDictConversible
        """
        if isinstance(obj, dict):
            return self.get_id() == obj.get('id')
        if isinstance(obj, FGO1HRunWeekListElement):
            return self.get_id() == cast(FGO1HRunWeekListElement, obj).get_id()
        return False


class PartitioningRuleBy1HRunWeekList:
    def __init__(self, start_day: int) -> None:
        self.existing_week: set[str] = set()

        if start_day < 0 or start_day > 6:
            raise ValueError("start_day must be 0-6")
        self.start_day = start_day

    def dispatch(
        self,
        partitions: Dict[str, List[model.SupportDictConversible]],
        report: model.RunReport,
    ) -> None:

        if "#fgo_1h_run" not in report.note.lower().split():
            return

        target_date = report.timestamp.date()
        week_start = get_week_start_day(target_date, self.start_day)
        if week_start.isoformat() in self.existing_week:
            return
        self.existing_week.add(week_start.isoformat())

        # 表示上は土曜にする (FGO_1H_run の実施日が土曜のため)
        # NOTE: たぶんこの実装だと start_day = 6 (SUN) のときバグりそう。
        # ただ、指定する可能性はほぼないので考えないことにする。
        display_date = week_start + timedelta(days=5 - self.start_day)

        e = FGO1HRunWeekListElement(week_start, display_date)

        # パーティションは常に all のみ
        if 'all' not in partitions:
            partitions['all'] = []

        partitions['all'].append(e)


class SkipSaveRuleNeverMatch:
    """
        どんな key とも match しないルール
    """
    def scan_report(self, report: model.RunReport) -> None:
        pass

    def match(self, key: str) -> bool:
        return False


class SkipSaveRuleByDate:
    """
        指定された日付より前だったら match するルール
    """
    def __init__(self, criteria: date):
        self.criteria = criteria

    def scan_report(self, report: model.RunReport) -> None:
        pass

    def match(self, key: str) -> bool:
        num_parts = len(key.split("-"))
        if num_parts == 3:
            # 日付 YYYY-MM-DD
            d = date.fromisoformat(key)
        elif num_parts == 2:
            # 月 YYYY-MM
            d = datetime.strptime(key, "%Y-%m").date()
        else:
            # 日付変換不可能なら unmatch
            return False

        return d < self.criteria


class SkipSaveRuleByDateRange:
    """
        指定された期間外だったら match するルール
    """
    def __init__(self, start_date: date, end_date: date):
        self.start_date = start_date
        self.end_date = end_date

    def scan_report(self, report: model.RunReport) -> None:
        pass

    def match(self, key: str) -> bool:
        num_parts = len(key.split("-"))
        if num_parts == 3:
            # 日付 YYYY-MM-DD
            d = date.fromisoformat(key)
        elif num_parts == 2:
            # 月 YYYY-MM
            d = datetime.strptime(key, "%Y-%m").date()
        else:
            # 日付変換不可能なら unmatch
            return False

        return d < self.start_date or d > self.end_date


class SkipSaveRuleByDateAndUser:
    """
        指定された日付以後の報告がない user に match するルール
    """
    def __init__(self, criteria: date):
        self.criteria = criteria
        self.unmatch_users: set[str] = set()

    def scan_report(self, report: model.RunReport) -> None:
        if report.timestamp.date() >= self.criteria:
            self.unmatch_users.add(report.reporter)

    def match(self, key: str) -> bool:
        return key not in self.unmatch_users


class SkipSaveRuleByDateAndQuest:
    """
        指定された日付以後の報告がない quest に match するルール
    """
    def __init__(self, criteria: date):
        self.criteria = criteria
        self.unmatch_quests: set[str] = set()

    def scan_report(self, report: model.RunReport) -> None:
        if report.timestamp.date() >= self.criteria:
            quest_id = report.quest_id
            self.unmatch_quests.add(quest_id)

    def match(self, key: str) -> bool:
        return key not in self.unmatch_quests


class Recorder:
    def __init__(
        self,
        partitioningRule: Union[
            SupportPartitioningRule,
            SupportStatefulPartitioningRule
        ],
        skipSaveRule: SupportSkipSaveRule,
        fileStorage: storage.SupportStorage,
        basedir: str,
        formats: Sequence[OutputFormat],
    ):
        self.partitions: Dict[str, List[model.SupportDictConversible]] = {}
        self.partitioningRule = partitioningRule
        self.skipSaveRule = skipSaveRule
        self.fileStorage = fileStorage
        self.basedir = basedir
        self.formats = formats
        self.counter: int = 0
        self.basepath = fileStorage.path_object(self.basedir)
        # for SupportStatefulPartitioningRule
        if hasattr(self.partitioningRule, 'setup'):
            statefulPartitioningRule = cast(
                SupportStatefulPartitioningRule,
                self.partitioningRule,
            )
            statefulPartitioningRule.setup(self.fileStorage, self.basepath)

    def add(self, report: model.RunReport) -> None:
        self.partitioningRule.dispatch(self.partitions, report)
        self.skipSaveRule.scan_report(report)
        self.counter += 1

    def add_all(self, reports: Sequence[model.RunReport]) -> None:
        for report in reports:
            self.add(report)

    def count(self) -> int:
        return self.counter

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

            if self.skipSaveRule.match(key):
                logger.info(
                    "key %s matched %s",
                    key,
                    self.skipSaveRule.__class__.__name__,
                )
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
                if force:
                    logger.info('force option is enabled')
                elif merged_reports == original:
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
    def _make_index(
        self,
        original: List[Dict[str, Any]],
        deepcopy: bool = False,
    ) -> Dict[Any, Dict[str, Any]]:

        d: Dict[Any, Dict[str, Any]] = {}
        for r in original:
            if deepcopy:
                d[r['id']] = copy.deepcopy(r)
            else:
                d[r['id']] = r
        return d

    def merge(
        self,
        additional_items: List[model.SupportDictConversible],
        original: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:

        logger.info('original reports: %d', len(original))
        merged_dict = self._make_index(original, deepcopy=True)
        index = self._make_index(original, deepcopy=False)

        additional_count = 0
        overriden_count = 0

        for item in additional_items:
            if item.get_id() not in index:
                merged_dict[item.get_id()] = item.as_dict()
                additional_count += 1
                continue

            origin = index[item.get_id()]
            if not item.equals(origin):
                logger.debug(
                    'item is not equal to origin\n  orig: %s, \n  item: %s',
                    origin,
                    item.as_dict(),
                )
                merged_dict[item.get_id()] = item.as_dict()
                overriden_count += 1

        merged_list = list(merged_dict.values())
        merged_list.sort(key=ReportMerger.marged_list_sorter, reverse=True)
        logger.info('additional reports: %d', additional_count)
        logger.info('overriden reports: %d', overriden_count)
        return merged_list

    @staticmethod
    def marged_list_sorter(e: Dict[str, Any]) -> Any:
        if 'timestamp' in e:
            return e['timestamp']
        return e['id']


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
            default=helper.json_serialize_helper,
        )
        stream.write(s.encode('UTF-8'))


def nvl(s: str | None, default: str = '') -> str:
    if s is None:
        return default
    return s


class CSVPageProcessor:
    def dump(
        self,
        merged_reports: List[Dict[str, Any]],
        stream: BinaryIO,
        **kwargs,
    ):
        sio = io.StringIO()
        header = [
            "報告ID",
            "ツイートID",
            "報告者ID",
            "報告者",
            "章",
            "場所",
            "周回数",
            "投稿時刻",
            "フリクエ",
            "ソース",
            "URL",
            "ドロップ",
        ]
        w = csv.writer(sio)
        w.writerow(header)
        for r in merged_reports:
            if r["source"] == "fgodrop":
                permalink = f"https://fgodrop.max747.org/reports/{r['report_id']}"
            else:
                permalink = f"https://twitter.com/{r['reporter']}/status/{r['id']}",

            row = [
                nvl(r["report_id"]),
                nvl(r["tweet_id"]),
                nvl(r["reporter_id"]),
                nvl(r["reporter"]),
                r["chapter"],
                r["place"],
                r["runcount"],
                r["timestamp"],
                r["freequest"],
                permalink,
            ]

            for k, v in r["items"].items():
                row.append(k)
                row.append(v)
            w.writerow(row)
        stream.write(sio.getvalue().encode('UTF-8'))


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


class MonthHTMLPageProcessor:
    template_html = "report_bymonth.jinja2"

    def dump(
        self,
        merged_reports: List[Dict[str, Any]],
        stream: BinaryIO,
        **kwargs,
    ):
        freequest_reports = [r for r in merged_reports if r['freequest']]
        event_reports = [r for r in merged_reports if not r['freequest']]
        this_month = kwargs['key']
        this_month_obj = datetime.strptime(this_month, month_format)
        prev_month = (this_month_obj + relativedelta(months=-1)).strftime(month_format)
        next_month = (this_month_obj + relativedelta(months=+1)).strftime(month_format)
        template = jinja2_env.get_template(self.template_html)
        html = template.render(
            freequest_reports=freequest_reports,
            event_reports=event_reports,
            prev_month=prev_month,
            this_month=this_month,
            next_month=next_month,
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
            questid=kwargs['key'],
        )
        stream.write(html.encode('UTF-8'))


class FGO1HRunHTMLPageProcessor:
    template_html = 'report_by1hrun.jinja2'

    def dump(
        self,
        merged_reports: List[Dict[str, Any]],
        stream: BinaryIO,
        **kwargs,
    ):
        today = kwargs['key']
        today_obj = date.fromisoformat(today)
        last_week = (today_obj + timedelta(days=-7)).isoformat()
        next_week = (today_obj + timedelta(days=+7)).isoformat()
        template = jinja2_env.get_template(self.template_html)
        html = template.render(
            reports=merged_reports,
            last_week=last_week,
            today=today,
            next_week=next_week,
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


class FGO1HRunListHTMLPageProcessor:
    template_html = 'all_1hrun.jinja2'

    def dump(
        self,
        merged_reports: List[Dict[str, Any]],
        stream: BinaryIO,
        **kwargs,
    ):
        template = jinja2_env.get_template(self.template_html)
        html = template.render(
            weeks=sorted(merged_reports, key=itemgetter('id'), reverse=True),
        )
        stream.write(html.encode('UTF-8'))


def create_processor(fmt: OutputFormat) -> PageProcessorSupport:
    if fmt == OutputFormat.JSON:
        return JSONPageProcessor()
    elif fmt == OutputFormat.CSV:
        return CSVPageProcessor()
    elif fmt == OutputFormat.DATE_HTML:
        return DateHTMLPageProcessor()
    elif fmt == OutputFormat.MONTH_HTML:
        return MonthHTMLPageProcessor()
    elif fmt == OutputFormat.USER_HTML:
        return UserHTMLPageProcessor()
    elif fmt == OutputFormat.QUEST_HTML:
        return QuestHTMLPageProcessor()
    elif fmt == OutputFormat.FGO1HRUN_HTML:
        return FGO1HRunHTMLPageProcessor()
    elif fmt == OutputFormat.USER_LIST_HTML:
        return UserListHTMLPageProcessor()
    elif fmt == OutputFormat.QUEST_LIST_HTML:
        return QuestListHTMLPageProcessor()
    elif fmt == OutputFormat.FGO1HRUN_LIST_HTML:
        return FGO1HRunListHTMLPageProcessor()

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


class LatestMonthPageBuilder:
    def __init__(
        self,
        fileStorage: storage.SupportStorage,
        basedir: str,
    ):
        self.fileStorage = fileStorage
        self.basedir = basedir
        self.basepath = fileStorage.path_object(basedir)

    def _find_latest_page(self, origin: datetime) -> str:
        # 10 は適当な数値。それだけさかのぼれば何かしらの
        # ファイルがあるだろうという期待の数値。
        # ふつうは当月か前月のデータが見つかるだろう。
        for i in range(10):
            target_month = origin - relativedelta(months=i)
            filename = '{}.html'.format(target_month.strftime(month_format))
            keypath = str(self.basepath / filename)
            if self.fileStorage.exists(keypath):
                return str(keypath)
        return ''

    def _latest_path(self):
        return str(self.basepath / 'latest.html')

    def build(self):
        """
            プログラム実行時点の日付で yyyy-MM.html を探す。
            なければ1か月前に戻る。これを繰り返して最新の
            yyyy-MM.html を特定する。特定できたらこれを
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

    def add_all(self, tweets: Sequence[twitter.ParseErrorTweet]) -> None:
        for tw in tweets:
            self.add_error(tw)

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
            _original_tweets = [
                twitter.ParseErrorTweet.retrieve(d) for d in original
            ]
            original_tweets: List[twitter.ParseErrorTweet] = [
                tw for tw in _original_tweets if tw is not None]
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
            default=helper.json_serialize_helper,
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
