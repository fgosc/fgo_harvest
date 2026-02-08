#!/usr/bin/env python3

import argparse
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def _json_load_hook(d: dict[str, Any]) -> dict[str, Any]:
    if 'timestamp' in d:
        ts = d['timestamp']
        d['timestamp'] = datetime.fromisoformat(ts)
    return d


def filter_by_year(
    data: list[dict[str, Any]],
    year: int,
) -> list[dict[str, Any]]:
    return [d for d in data if d["timestamp"].year == year]


class StatUser:
    def __init__(self, name: str):
        self.name = name
        # 報告件数
        self.report_count_total = 0
        self.report_count_freequest = 0
        self.report_count_event = 0
        # 周回数
        self.run_count_total = 0
        self.run_count_freequest = 0
        self.run_count_event = 0
        # 最多周回数
        self.max_run_count = 0
        # 平均周回数
        self.avg_run_count = float(0)
        # 報告日数
        self.report_date_count = 0
        # 報告者ID
        self.reporter_id = ""
        # 最終報告日時
        self.last_report_timestamp: datetime | None = None

    def analyze(self, data: list[dict[str, Any]]):
        report_date_set = set()

        for r in data:
            runcount = r["runcount"]
            timestamp_date = r["timestamp"].date().isoformat()
            report_date_set.add(timestamp_date)
            self.report_count_total += 1
            self.run_count_total += runcount

            if "reporter_id" in r:
                if self.reporter_id == "":
                    self.reporter_id = r["reporter_id"]
                elif self.reporter_id != r["reporter_id"]:
                    logger.warning(f"報告者ID不一致: {self.name} {self.reporter_id} != {r['reporter_id']}")

            if r["freequest"]:
                self.report_count_freequest += 1
                self.run_count_freequest += runcount
            else:
                self.report_count_event += 1
                self.run_count_event += runcount

            if runcount > self.max_run_count:
                self.max_run_count = runcount

            if self.last_report_timestamp is None:
                self.last_report_timestamp = r["timestamp"]
            elif r["timestamp"] > self.last_report_timestamp:
                self.last_report_timestamp = r["timestamp"]

        if self.report_count_total == 0:
            self.avg_run_count = 0
        else:
            self.avg_run_count = round(self.run_count_total / self.report_count_total, 2)

        self.report_date_count = len(report_date_set)



class StatUsers:
    def __init__(self) -> None:
        self.users: list[StatUser] = []

    def add(self, stat: StatUser):
        self.users.append(stat)

    def print_all(self):
        header = [
            "名前",
            "報告回数",
            "報告回数フリクエ",
            "報告回数イベント",
            "周回数",
            "周回数フリクエ",
            "周回数イベント",
            "最多周回数",
            "平均周回数",
            "報告日数",
        ]
        print("\t".join(header))

        for u in self.users:
            row = [
                u.name,
                u.report_count_total,
                u.report_count_freequest,
                u.report_count_event,
                u.run_count_total,
                u.run_count_freequest,
                u.run_count_event,
                u.max_run_count,
                u.avg_run_count,
                u.report_date_count,
            ]
            print("\t".join([str(c) for c in row]))

    def merge_by_reporter_id(self) -> "StatUsers":
        reporters = {}
        for u in self.users:
            if u.reporter_id not in reporters:
                reporters[u.reporter_id] = u
            else:
                exist = reporters[u.reporter_id]
                exist.report_count_total += u.report_count_total
                exist.report_count_freequest += u.report_count_freequest
                exist.report_count_event += u.report_count_event
                exist.run_count_total += u.run_count_total
                exist.run_count_freequest += u.run_count_freequest
                exist.run_count_event += u.run_count_event
                if u.max_run_count > exist.max_run_count:
                    exist.max_run_count = u.max_run_count
                # 平均周回数は再計算が必要
                if exist.report_count_total == 0:
                    exist.avg_run_count = 0
                else:
                    exist.avg_run_count = round(exist.run_count_total / exist.report_count_total, 2)
                exist.report_date_count += u.report_date_count

                # 最終報告日時が新しい方の name を採用する
                if exist.last_report_timestamp is None:
                    exist.name = u.name
                    exist.last_report_timestamp = u.last_report_timestamp
                elif u.last_report_timestamp is None:
                    pass
                elif u.last_report_timestamp > exist.last_report_timestamp:
                    exist.name = u.name
                    exist.last_report_timestamp = u.last_report_timestamp

        new_stat_users = StatUsers()
        new_stat_users.users = list(reporters.values())
        return new_stat_users


def exec_user(args):
    target_dir = Path(args.target_directory)
    files = target_dir.glob("*.json")
    stat_users = StatUsers()
    for filepath in files:
        if filepath.name == "all.json":
            continue
        logger.info(f"processing {filepath.name}")
        with open(filepath) as fp:
            data = json.load(fp, object_hook=_json_load_hook)
        filtered = filter_by_year(data, args.year)
        if len(filtered) == 0:
            continue
        su = StatUser(filepath.stem)
        su.analyze(filtered)
        stat_users.add(su)
    # オプションが有効なら reporter でマージする
    if args.merge_reporter:
        stat_users = stat_users.merge_by_reporter_id()
    stat_users.print_all()


def build_parser():
    parser = argparse.ArgumentParser()

    def add_common_arguments(subparser):
        subparser.add_argument(
            '-l',
            '--loglevel',
            choices=('debug', 'info', 'warning'),
            default='info',
        )

    subparsers = parser.add_subparsers(dest='command')

    user_parser = subparsers.add_parser('user')
    add_common_arguments(user_parser)
    thisyear = datetime.now().year
    user_parser.add_argument('year', type=int, choices=range(2020, thisyear))
    user_parser.add_argument(
        '-t',
        '--target-directory',
        default='output/contents/user',
    )
    user_parser.add_argument('-m', '--merge-reporter', action='store_true')
    user_parser.set_defaults(func=exec_user)
    return parser


if __name__ == "__main__":
    parser = build_parser()
    args = parser.parse_args()

    if hasattr(args, 'func'):
        logging.basicConfig(
            level=args.loglevel.upper(),
            format='%(asctime)s [%(levelname)s] %(message)s',
        )
        args.func(args)
    else:
        parser.print_usage()
