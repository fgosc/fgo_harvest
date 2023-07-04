import json
from datetime import datetime
from logging import getLogger

from . import model
from . import storage
from . import twitter
from . import helper

logger = getLogger(__name__)


"""
NOTE
Twitter API から取得した報告データは TweetRepository にそのままの raw data として保存される。
理由は parse 済みのデータだけを保存してしまうと parse に問題があった場合などにリトライするのが
難しくなるため。生データを保存しておけば、何か問題があった場合には生データを再度 parse すればよい。

一方、新サイトの GraphQL から取得したデータはすでに parse 済みのデータである。
レンダリングの時点では両方のデータを統一的に扱う必要があるので、そのギャップを埋める必要がある。
"""

class FileNotFound(Exception):
    pass


class TweetRepository:
    def __init__(
        self,
        fileStorage: storage.SupportStorage,
        basedir: str,
    ):
        self.fileStorage = fileStorage
        self.basedir = basedir

    def put(self, key: str, tweets: list[twitter.TweetCopy]) -> None:
        """
        append_tweets との違い: 同名のファイルが存在する場合は、そのファイルを上書きする
        """
        s = json.dumps(
            [tw.as_dict() for tw in tweets],
            ensure_ascii=False,
            default=helper.json_serialize_helper,
        )
        basepath = self.fileStorage.path_object(self.basedir)
        keypath = str(basepath / key)
        stream = self.fileStorage.get_output_stream(keypath)
        stream.write(s.encode("UTF-8"))
        self.fileStorage.close_output_stream(stream)

    def append_tweets(self, key: str, tweets: list[twitter.TweetCopy]) -> None:
        """
        put との違い: 同名のファイルが存在する場合は、そのファイルに追記する
        """
        basepath = self.fileStorage.path_object(self.basedir)
        keypath = str(basepath / key)
        stream = self.fileStorage.get_output_stream(keypath, append=True)
        stream.seek(0)
        try:
            loaded = json.load(stream)
        except json.decoder.JSONDecodeError as e:
            logger.warning(e)
            logger.warning("use the blank list [] as alternative")
            loaded = []

        merged_tweets = [twitter.TweetCopy.retrieve(e) for e in loaded]
        merged_tweets.extend(tweets)

        s = json.dumps(
            [tw.as_dict() for tw in merged_tweets if tw is not None],
            ensure_ascii=False,
            default=helper.json_serialize_helper,
        )

        stream.seek(0)
        stream.write(s.encode("UTF-8"))
        self.fileStorage.close_output_stream(stream)

    def exists(self, key: str) -> bool:
        basepath = self.fileStorage.path_object(self.basedir)
        keypath = str(basepath / key)
        return self.fileStorage.exists(keypath)

    def readall(
        self, exclude_accounts: set[str]
    ) -> tuple[list[model.RunReport], list[twitter.ParseErrorTweet]]:
        reports: list[model.RunReport] = []
        parseErrorTweets: list[twitter.ParseErrorTweet] = []
        id_cache: set[int] = set()

        for stream in self.fileStorage.streams(self.basedir, suffix=".json"):
            loaded = json.load(stream)
            tweets = [twitter.TweetCopy.retrieve(e) for e in loaded]
            logger.info(f"{len(tweets)} tweets retrieved")
            for tw in tweets:
                if tw is None:
                    continue
                if tw.tweet_id in id_cache:
                    logger.warning("ignoring duplicate tweet: %s", tw.tweet_id)
                    continue
                elif tw.screen_name in exclude_accounts:
                    logger.warning(
                        "ignoring exclude account's tweet: %s",
                        tw.tweet_id,
                    )
                    continue
                try:
                    report = twitter.parse_tweet(tw)
                except twitter.TweetParseError as e:
                    error_tw = twitter.ParseErrorTweet(
                        tweet=tw, error_message=e.get_message()
                    )
                    parseErrorTweets.append(error_tw)

                reports.append(report)
                id_cache.add(tw.tweet_id)

        # 新しい順
        reports.sort(key=lambda e: e.timestamp, reverse=True)

        logger.info(
            f"total: {len(reports)} reports, {len(parseErrorTweets)} parse error tweets"
        )
        return reports, parseErrorTweets


class ReportRepository:
    def __init__(
        self,
        fileStorage: storage.SupportStorage,
        basedir: str,
    ):
        self.fileStorage = fileStorage
        self.basedir = basedir

    def put(self, key: str, reports: list[model.RunReport]) -> None:
        """
        append との違い: 同名のファイルが存在する場合は、そのファイルを上書きする
        """
        s = json.dumps(
            [r.as_dict() for r in reports],
            ensure_ascii=False,
            default=helper.json_serialize_helper,
        )
        basepath = self.fileStorage.path_object(self.basedir)
        keypath = str(basepath / key)
        stream = self.fileStorage.get_output_stream(keypath)
        stream.write(s.encode("UTF-8"))
        self.fileStorage.close_output_stream(stream)

    def append(self, key: str, reports: list[model.RunReport]) -> None:
        """
        put との違い: 同名のファイルが存在する場合は、そのファイルに追記する
        """
        basepath = self.fileStorage.path_object(self.basedir)
        keypath = str(basepath / key)
        stream = self.fileStorage.get_output_stream(keypath, append=True)
        stream.seek(0)
        try:
            loaded = json.load(stream)
        except json.decoder.JSONDecodeError as e:
            logger.warning(e)
            logger.warning("use the blank list [] as alternative")
            loaded = []

        merged_reports = [model.RunReport.retrieve(e) for e in loaded]
        merged_reports.extend(reports)

        s = json.dumps(
            [r.as_dict() for r in reports if r is not None],
            ensure_ascii=False,
            default=helper.json_serialize_helper,
        )

        stream.seek(0)
        stream.write(s.encode("UTF-8"))
        self.fileStorage.close_output_stream(stream)

    def exists(self, key: str) -> bool:
        basepath = self.fileStorage.path_object(self.basedir)
        keypath = str(basepath / key)
        return self.fileStorage.exists(keypath)

    def readall(self) -> list[model.RunReport]:
        all_reports: list[model.RunReport] = []

        for stream in self.fileStorage.streams(self.basedir, suffix=".json"):
            loaded = json.load(stream)
            reports = [model.RunReport.retrieve(e) for e in loaded]
            logger.info(f"{len(reports)} reports retrieved")
            all_reports.extend(reports)

        # 新しい順
        all_reports.sort(key=lambda e: e.timestamp, reverse=True)
        return all_reports


class LastReportTimeStamp:
    """
    取得済み最新レポートおよび、そのレポートの時刻を記録するもの。
    次回の polling で同じレポートを繰り返し取得しないようにするために用いる。
    """
    def __init__(self, fileStorage: storage.SupportStorage, basedir: str, key: str):
        self.fileStorage = fileStorage
        self.basedir = basedir
        self.key = key

    def _keypath(self) -> str:
        basepath = self.fileStorage.path_object(self.basedir)
        return str(basepath / self.key)

    def save(self, report_id: str, timestamp: datetime) -> None:
        d = {
            "report_id": report_id,
            "timestamp": timestamp.isoformat(),
        }
        text = json.dumps(d)

        keypath = self._keypath()
        out = self.fileStorage.get_output_stream(keypath)
        out.write(text.encode("UTF-8"))
        self.fileStorage.close_output_stream(out)

    def load(self) -> tuple[str, datetime]:
        keypath = self._keypath()
        if not self.fileStorage.exists(keypath):
            raise FileNotFound(keypath)

        text = self.fileStorage.get_as_text(keypath)
        d = json.loads(text)
        return d["report_id"], datetime.fromisoformat(d["timestamp"])

    def exists(self) -> bool:
        keypath = self._keypath()
        return self.fileStorage.exists(keypath)
