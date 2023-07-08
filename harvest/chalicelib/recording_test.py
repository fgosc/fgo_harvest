import calendar
from datetime import datetime

from . import model
from . import recording
from . import timezone


def test_PartitioningRuleBy1HRun():
    report0 = model.RunReport(
        report_id="1",
        tweet_id=None,
        reporter="reporter",
        reporter_id="1",
        reporter_name="",
        chapter="キャメロット",
        place="隠れ村",
        runcount=10,
        items={"ランプ": "1", "鎖": "2"},
        note="test0 #FGO_1H_run",
        timestamp=datetime(2023, 7, 5, 23, 59, 59, tzinfo=timezone.Local),
        source="fgodrop",
    )
    report1 = model.RunReport(
        report_id="2",
        tweet_id=None,
        reporter="reporter",
        reporter_id="1",
        reporter_name="",
        chapter="キャメロット",
        place="隠れ村",
        runcount=20,
        items={"ランプ": "2", "鎖": "4"},
        note="#FGO_1H_RUN",
        timestamp=datetime(2023, 7, 6, 0, 0, 0, tzinfo=timezone.Local),
        source="fgodrop",
    )
    report2 = model.RunReport(
        report_id="3",
        tweet_id=None,
        reporter="reporter",
        reporter_id="1",
        reporter_name="",
        chapter="キャメロット",
        place="隠れ村",
        runcount=30,
        items={"ランプ": "3", "鎖": "6"},
        note="test2\n#FGO_1H_run",
        timestamp=datetime(2023, 7, 12, 23, 59, 59, tzinfo=timezone.Local),
        source="fgodrop",
    )
    report3 = model.RunReport(
        report_id="4",
        tweet_id=None,
        reporter="reporter",
        reporter_id="1",
        reporter_name="",
        chapter="キャメロット",
        place="隠れ村",
        runcount=40,
        items={"ランプ": "4", "鎖": "8"},
        note="#fgo_1h_run",
        timestamp=datetime(2023, 7, 13, 0, 0, 0, tzinfo=timezone.Local),
        source="fgodrop",
    )
    report4 = model.RunReport(
        report_id="4",
        tweet_id=None,
        reporter="reporter",
        reporter_id="1",
        reporter_name="",
        chapter="キャメロット",
        place="隠れ村",
        runcount=40,
        items={"ランプ": "4", "鎖": "8"},
        note="test4",
        timestamp=datetime(2023, 7, 13, 0, 0, 0, tzinfo=timezone.Local),
        source="fgodrop",
    )

    reports = [report0, report1, report2, report3, report4]
    rule = recording.PartitioningRuleBy1HRun(start_day=calendar.THURSDAY)
    partitions = {}
    for report in reports:
        rule.dispatch(partitions, report)

    assert len(partitions) == 3
    # key は土曜日基準
    assert list(partitions.keys()) == ["2023-07-01", "2023-07-08", "2023-07-15"]
    assert partitions["2023-07-01"] == [report0]
    assert partitions["2023-07-08"] == [report1, report2]
    assert partitions["2023-07-15"] == [report3]
