#!/usr/bin/env python3

"""
    ローカル実行用のエントリポイント。
"""

import argparse
import calendar
import logging
import os
import pathlib
from datetime import date, datetime
from typing import cast

from chalicelib import (
    graphql,
    model,
    settings,
    static,
    storage,
    twitter,
    timezone,
    recording,
    repository,
)

logger = logging.getLogger(__name__)


def setup_tweet_repository(output_dir: str) -> repository.TweetRepository:
    storage_dir = os.path.join(output_dir, 'tweets')
    if not os.path.exists(storage_dir):
        os.makedirs(storage_dir)
    tweet_storage = repository.TweetRepository(
        fileStorage=storage.FilesystemStorage(),
        basedir=storage_dir,
    )
    return tweet_storage


def setup_report_repository(output_dir: str) -> repository.ReportRepository:
    storage_dir = os.path.join(output_dir, 'reports')
    if not os.path.exists(storage_dir):
        os.makedirs(storage_dir)
    report_storage = repository.ReportRepository(
        fileStorage=storage.FilesystemStorage(),
        basedir=storage_dir,
    )
    return report_storage


def setup_censored_accounts() -> twitter.CensoredAccounts:
    return twitter.CensoredAccounts(
        fileStorage=storage.FilesystemStorage(),
        filepath=settings.CensoredAccountsFile,
    )


def render_all(
    reports: list[model.RunReport],
    parse_error_tweets: list[twitter.ParseErrorTweet],
    output_dir: str,
    skip_target_date: date,
    rebuild: bool,
) -> None:
    recorders: list[recording.Recorder] = []

    contents_date_dir = os.path.join(output_dir, 'contents', 'date')
    if not os.path.exists(contents_date_dir):
        os.makedirs(contents_date_dir)
    recorder_bydate = recording.Recorder(
        partitioningRule=recording.PartitioningRuleByDate(),
        skipSaveRule=recording.SkipSaveRuleByDate(skip_target_date),
        fileStorage=storage.FilesystemStorage(),
        basedir=contents_date_dir,
        formats=(
            recording.OutputFormat.JSON,
            recording.OutputFormat.CSV,
            recording.OutputFormat.DATE_HTML,
        ),
    )
    recorders.append(recorder_bydate)

    contents_month_dir = os.path.join(output_dir, 'contents', 'month')
    if not os.path.exists(contents_month_dir):
        os.makedirs(contents_month_dir)
    recorder_bymonth = recording.Recorder(
        partitioningRule=recording.PartitioningRuleByMonth(),
        skipSaveRule=recording.SkipSaveRuleByDate(skip_target_date),
        fileStorage=storage.FilesystemStorage(),
        basedir=contents_month_dir,
        formats=(
            recording.OutputFormat.JSON,
            recording.OutputFormat.CSV,
            recording.OutputFormat.MONTH_HTML,
        )
    )
    recorders.append(recorder_bymonth)

    contents_user_dir = os.path.join(output_dir, 'contents', 'user')
    if not os.path.exists(contents_user_dir):
        os.makedirs(contents_user_dir)
    recorder_byuser = recording.Recorder(
        partitioningRule=recording.PartitioningRuleByUser(),
        skipSaveRule=recording.SkipSaveRuleByDateAndUser(skip_target_date),
        fileStorage=storage.FilesystemStorage(),
        basedir=contents_user_dir,
        formats=(
            recording.OutputFormat.JSON,
            recording.OutputFormat.CSV,
            recording.OutputFormat.USER_HTML,
        ),
    )
    recorders.append(recorder_byuser)

    contents_quest_dir = os.path.join(output_dir, 'contents', 'quest')
    if not os.path.exists(contents_quest_dir):
        os.makedirs(contents_quest_dir)
    recorder_byquest = recording.Recorder(
        partitioningRule=recording.PartitioningRuleByQuest(),
        skipSaveRule=recording.SkipSaveRuleByDateAndQuest(skip_target_date),
        fileStorage=storage.FilesystemStorage(),
        basedir=contents_quest_dir,
        formats=(
            recording.OutputFormat.JSON,
            recording.OutputFormat.CSV,
            recording.OutputFormat.QUEST_HTML,
        ),
    )
    recorders.append(recorder_byquest)

    contents_1hrun_dir = os.path.join(output_dir, 'contents', '1hrun')
    if not os.path.exists(contents_1hrun_dir):
        os.makedirs(contents_1hrun_dir)
    recorder_by1hrun = recording.Recorder(
        partitioningRule=recording.PartitioningRuleBy1HRun(
            start_day=calendar.THURSDAY,
        ),
        skipSaveRule=recording.SkipSaveRuleByDate(skip_target_date),
        fileStorage=storage.FilesystemStorage(),
        basedir=contents_1hrun_dir,
        formats=(
            recording.OutputFormat.JSON,
            recording.OutputFormat.CSV,
            recording.OutputFormat.FGO1HRUN_HTML,
        ),
    )
    recorders.append(recorder_by1hrun)

    # 出力先は contents_user_dir
    recorder_byuserlist = recording.Recorder(
        partitioningRule=recording.PartitioningRuleByUserList(),
        skipSaveRule=recording.SkipSaveRuleNeverMatch(),
        fileStorage=storage.FilesystemStorage(),
        basedir=contents_user_dir,
        formats=(
            recording.OutputFormat.JSON,
            recording.OutputFormat.USER_LIST_HTML,
        ),
    )
    recorders.append(recorder_byuserlist)

    # 出力先は contents_quest_dir
    recorder_byquestlist = recording.Recorder(
        # この partitioningRule は rebuild フラグを個別に渡す必要あり
        partitioningRule=recording.PartitioningRuleByQuestList(rebuild),
        skipSaveRule=recording.SkipSaveRuleNeverMatch(),
        fileStorage=storage.FilesystemStorage(),
        basedir=contents_quest_dir,
        formats=(
            recording.OutputFormat.JSON,
            recording.OutputFormat.QUEST_LIST_HTML,
        ),
    )
    recorders.append(recorder_byquestlist)

    # 出力先は contents_1hrun_dir
    recorder_by1hrunlist = recording.Recorder(
        partitioningRule=recording.PartitioningRuleBy1HRunWeekList(
            start_day=calendar.THURSDAY,
        ),
        skipSaveRule=recording.SkipSaveRuleNeverMatch(),
        fileStorage=storage.FilesystemStorage(),
        basedir=contents_1hrun_dir,
        formats=(
            recording.OutputFormat.JSON,
            recording.OutputFormat.FGO1HRUN_LIST_HTML,
        ),
    )
    recorders.append(recorder_by1hrunlist)

    contents_error_dir = os.path.join(output_dir, 'contents', 'errors')
    if not os.path.exists(contents_error_dir):
        os.makedirs(contents_error_dir)
    error_recorder = recording.ErrorPageRecorder(
        fileStorage=storage.FilesystemStorage(),
        basedir=contents_error_dir,
        key='error',
        formats=(
            recording.ErrorOutputFormat.JSON,
            recording.ErrorOutputFormat.HTML,
        )
    )

    for recorder in recorders:
        recorder.add_all(reports)

    error_recorder.add_all(parse_error_tweets)

    ignore_original = rebuild
    for recorder in recorders:
        recorder.save(ignore_original=ignore_original)
    error_recorder.save(ignore_original=ignore_original)

    # 出力先は contents_date_dir
    # 少なくとも bydate の HTML レンダリングが完了してからでないと実行できない。
    # したがってこの位置で実行する。
    latestDatePageBuilder = recording.LatestDatePageBuilder(
        fileStorage=storage.FilesystemStorage(),
        basedir=contents_date_dir,
    )
    latestDatePageBuilder.build()

    # 出力先は contents_month_dir
    # 少なくとも bymonth の HTML レンダリングが完了してからでないと実行できない。
    # したがってこの位置で実行する。
    latestMonthPageBuilder = recording.LatestMonthPageBuilder(
        fileStorage=storage.FilesystemStorage(),
        basedir=contents_month_dir,
    )
    latestMonthPageBuilder.build()


def command_static(args: argparse.Namespace) -> None:
    static_dir = os.path.join(args.output_dir, 'contents', 'static')
    if not os.path.exists(static_dir):
        os.makedirs(static_dir)
    renderer = static.StaticPagesRenderer(
        fileStorage=storage.FilesystemStorage(),
        basedir=static_dir,
    )
    renderer.render_all()


def command_rebuild(args: argparse.Namespace) -> None:
    tweet_repository = setup_tweet_repository(args.output_dir)
    report_reporitory = setup_report_repository(args.output_dir)
    censored_accounts = setup_censored_accounts()
    tweet_reports, parse_error_tweets = tweet_repository.readall(set(censored_accounts.list()))
    report_reports = report_reporitory.readall()

    # マージして新しい順に並べる
    reports = tweet_reports + report_reports
    reports.sort(key=lambda e: e.timestamp, reverse=True)

    render_all(reports, parse_error_tweets, args.output_dir, args.skip_target_date, rebuild=True)


def command_build(args: argparse.Namespace) -> None:
    last_report_ts_retriever = repository.LastReportTimeStamp(
        fileStorage=storage.FilesystemStorage(),
        basedir=".",
        key=args.last_report_time_file,
    )
    # --since が指定された場合はそれが最優先
    # --last-report-time-file が存在する場合は次点
    # いずれも満たさない場合は 2023-06-13 20:35:00 JST (Twitter Crawling の停止時刻)
    if args.since:
        last_report_id = ""
        since = int(args.since)
    elif last_report_ts_retriever.exists():
        last_report_id, last_report_time = last_report_ts_retriever.load()
        since = int(last_report_time.timestamp())
    else:
        last_report_id = ""
        since = 1686656100

    client = graphql.GraphQLClient(settings.GraphQLEndpoint, settings.GraphQLApiKey)
    reports = client.list_reports(timestamp=since)

    if len(reports) == 0:
        logger.info('no reports')
        return

    if len(reports) == 1 and last_report_id == reports[0].report_id:
        logger.info('no new reports')
        return

    report_repository = setup_report_repository(args.output_dir)
    key = '{}.json'.format(datetime.now(tz=timezone.Local).strftime('%Y%m%d_%H%M%S'))
    report_repository.put(key, reports)

    # fgodrop graphql から取得する結果はすでに RunReport 形式と互換であり parse error は発生しない
    parse_error_tweets: list[twitter.ParseErrorTweet] = []
    render_all(reports, parse_error_tweets, args.output_dir, args.skip_target_date, rebuild=False)

    newest_report = max(reports, key=lambda e: e.timestamp)
    logger.info(f'saving last report: id = {newest_report.report_id}, time = {newest_report.timestamp}')
    last_report_ts_retriever.save(newest_report.report_id, newest_report.timestamp)


def command_delete(args: argparse.Namespace) -> None:
    # TODO 実装
    pass


def date_type(date_str: str) -> date:
    return date.fromisoformat(date_str)


class StoreUnixTimeAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        t = datetime.strptime(values, "%Y%m%d%H%M%S")
        setattr(namespace, self.dest, int(t.timestamp()))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()

    def add_common_arguments(p):
        p.add_argument(
            '--output-dir',
            default='output'
        )
        p.add_argument(
            '-l', '--loglevel',
            choices=('debug', 'info', 'warning'),
            default='info',
        )

    subparsers = parser.add_subparsers(dest='command')

    build_parser = subparsers.add_parser('build')
    add_common_arguments(build_parser)
    build_parser.add_argument(
        "--since",
        action=StoreUnixTimeAction,
        help="since. format: YYYYMMDDHHMMSS",
    )
    build_parser.add_argument(
        "--last-report-time-file",
        default=settings.LastReportTimeFile,
        help=f"last report time file which the program fetched. default: {settings.LastReportTimeFile}"
    )
    build_parser.add_argument(
        "--skip-target-date",
        type=date_type,
        default=date(2000, 1, 1),
    )
    build_parser.set_defaults(func=command_build)

    rebuild_parser = subparsers.add_parser('rebuild')
    add_common_arguments(rebuild_parser)
    rebuild_parser.add_argument(
        "--skip-target-date",
        type=date_type,
        default=date(2000, 1, 1),
    )
    rebuild_parser.set_defaults(func=command_rebuild)

    static_parser = subparsers.add_parser('static')
    add_common_arguments(static_parser)
    static_parser.set_defaults(func=command_static)

    delete_parser = subparsers.add_parser('delete')
    add_common_arguments(delete_parser)
    delete_parser.set_defaults(func=command_delete)

    return parser


if __name__ == "__main__":
    parser = build_parser()
    parsed_args = parser.parse_args()
    if hasattr(parsed_args, 'func'):
        log_format = (
            '%(asctime)s [%(levelname)s] '
            '<%(module)s-L%(lineno)s> %(message)s'
        )
        logging.basicConfig(
            level=parsed_args.loglevel.upper(),
            format=log_format,
        )
        parsed_args.func(parsed_args)
    else:
        parser.print_usage()
