from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol, cast

from . import freequest
from . import timezone


class SupportDictConversible(Protocol):
    def as_dict(self) -> dict[str, Any]:
        ...

    def get_id(self) -> Any:
        ...

    def equals(self, obj: Any) -> bool:
        ...


class RunReport:
    """
    周回報告レポート
    report_id または tweet_id いずれかが必須
    """

    def __init__(
        self,
        # report_id は source: twitter の場合 empty
        report_id: str,
        # tweet_id は source: fgodrop の場合 None
        tweet_id: int | None,
        # twitter name or "anonymous"
        reporter: str,
        # reporter_id は source: twitter の場合 empty
        reporter_id: str,
        # reporter_name は source: twitter の場合 empty
        reporter_name: str,
        chapter: str,
        place: str,
        runcount: int,
        items: dict[str, str],
        note: str,
        timestamp: datetime,
        source: str,
    ):
        if report_id is None and tweet_id is None:
            raise ValueError("either report_id or tweet_id must be specified")

        self.report_id = report_id
        self.tweet_id = tweet_id
        self.reporter = reporter
        self.reporter_id = reporter_id
        self.reporter_name = reporter_name
        self.chapter = chapter
        self.place = place
        self.runcount = runcount
        self.items = items
        self.note = note
        self.timestamp = timestamp
        self.source = source

    def __str__(self) -> str:
        if self.tweet_id:
            return "{} https://twitter.com/{}/status/{} <{}/{}/{}周> {}".format(
                self.timestamp,
                self.reporter,
                self.tweet_id,
                self.chapter,
                self.place,
                self.runcount,
                self.items,
            )
        else:
            return "{} [{}] {} <{}/{}/{}周> {}".format(
                self.timestamp,
                self.reporter,
                self.report_id,
                self.chapter,
                self.place,
                self.runcount,
                self.items,
            )

    def as_dict(self) -> dict[str, Any]:
        """
        for reporting.SupportDictConversible
        """
        return dict(
            # NOTE: 既存データとの後方互換性のため id は残す
            id=self.get_id(),
            report_id=self.report_id,
            tweet_id=self.tweet_id,
            reporter=self.reporter,
            reporter_id=self.reporter_id,
            reporter_name=self.reporter_name,
            chapter=self.chapter,
            place=self.place,
            runcount=self.runcount,
            items=self.items,
            note=self.note,
            timestamp=self.timestamp,
            freequest=self.is_freequest,
            quest_id=self.quest_id,
            source=self.source,
        )

    def get_id(self) -> Any:
        """
        for reporting.SupportDictConversible
        """
        # report_id を優先する
        if self.report_id:
            return self.report_id
        return str(self.tweet_id)

    def equals(self, obj: Any) -> bool:
        """
        for reporting.SupportDictConversible
        """
        if isinstance(obj, dict):
            return self.as_dict() == obj
        if isinstance(obj, RunReport):
            return self.as_dict() == obj.as_dict()
        return False

    @property
    def is_freequest(self) -> bool:
        isfq = freequest.defaultDetector.is_freequest(self.chapter, self.place)
        if isfq:
            return True
        bestmatch = freequest.defaultDetector.search_bestmatch_freequest(
            f"{self.chapter} {self.place}".strip(),
        )
        if bestmatch:
            return True
        return False

    @property
    def quest_id(self) -> str:
        if not freequest.defaultDetector.is_freequest(
            self.chapter,
            self.place,
        ):
            bestmatch = freequest.defaultDetector.search_bestmatch_freequest(
                f"{self.chapter} {self.place}".strip(),
            )
            if bestmatch:
                return bestmatch

        return freequest.defaultDetector.get_quest_id(
            self.chapter,
            self.place,
            self.timestamp.year,
        )

    @staticmethod
    def retrieve(data: dict[str, Any]) -> RunReport:
        return RunReport(
            report_id=data["report_id"],
            tweet_id=data["tweet_id"],
            reporter=str(data["reporter"]),
            reporter_id=data["reporter_id"],
            # NOTE: 古いデータ形式だと reporter_name が存在しない可能性がある
            reporter_name=data.get("reporter_name", ""),
            chapter=str(data["chapter"]),
            place=str(data["place"]),
            runcount=int(data["runcount"]),
            items=cast(dict[str, str], data["items"]),
            note=str(data["note"]),
            timestamp=datetime.fromisoformat(str(data["timestamp"])).astimezone(timezone.Local),
            source=str(data["source"]),
        )
