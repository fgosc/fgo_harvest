#!/usr/bin/env python3

"""
    ローカル実行用のエントリポイント。
"""

import argparse
import logging
import os
from datetime import date, datetime

from chalicelib import settings
from chalicelib import static
from chalicelib import storage
from chalicelib import twitter
from chalicelib import recording

logger = logging.getLogger(__name__)


def setup_tweet_repository(output_dir: str) -> recording.TweetRepository:
    storage_dir = os.path.join(output_dir, 'tweets')
    if not os.path.exists(storage_dir):
        os.makedirs(storage_dir)
    tweet_storage = recording.TweetRepository(
        fileStorage=storage.FilesystemStorage(),
        basedir=storage_dir,
    )
    return tweet_storage


def setup_censored_accounts() -> twitter.CensoredAccounts:
    return twitter.CensoredAccounts(
        fileStorage=storage.FilesystemStorage(),
        filepath=settings.CensoredAccountsFile,
    )


def render_all(
    tweets: list[twitter.TweetCopy],
    output_dir: str,
    skip_target_date: date,
    rebuild: bool,
) -> None:
    recorders = []

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
            recording.OutputFormat.DATEHTML,
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
            recording.OutputFormat.MONTHHTML,
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
            recording.OutputFormat.USERHTML,
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
            recording.OutputFormat.QUESTHTML,
        ),
    )
    recorders.append(recorder_byquest)

    # 出力先は contents_user_dir
    recorder_byuserlist = recording.Recorder(
        partitioningRule=recording.PartitioningRuleByUserList(),
        skipSaveRule=recording.SkipSaveRuleNeverMatch(),
        fileStorage=storage.FilesystemStorage(),
        basedir=contents_user_dir,
        formats=(
            recording.OutputFormat.JSON,
            recording.OutputFormat.USERLISTHTML,
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
            recording.OutputFormat.QUESTLISTHTML,
        ),
    )
    recorders.append(recorder_byquestlist)

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

    for tweet in tweets:
        try:
            report = twitter.parse_tweet(tweet)
            logger.info(report)
            for recorder in recorders:
                recorder.add(report)

        except twitter.TweetParseError as e:
            logger.error(e)
            logger.error(tweet)
            etw = twitter.ParseErrorTweet(
                tweet=tweet,
                error_message=e.get_message(),
            )
            error_recorder.add_error(etw)

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
    censored_accounts = setup_censored_accounts()
    tweets = tweet_repository.readall(set(censored_accounts.list()))
    render_all(tweets, args.output_dir, args.skip_target_date, rebuild=True)


def command_build(args: argparse.Namespace) -> None:
    agent = twitter.Agent(
        consumer_key=settings.TwitterConsumerKey,
        consumer_secret=settings.TwitterConsumerSecret,
        access_token=settings.TwitterAccessToken,
        access_token_secret=settings.TwitterAccessTokenSecret,
    )
    if args.tweet_id:
        tweet_dict = agent.get_multi(args.tweet_id)
        for tid, tw in tweet_dict.items():
            logger.info(f'id: {tid}, tw: {tw}')
            try:
                report = twitter.parse_tweet(tw)
                logger.info(report)
                logger.info('is_freequest: %s', report.is_freequest)

            except twitter.TweetParseError as e:
                logger.error(e)
        return

    tweet_repository = setup_tweet_repository(args.output_dir)
    censored_accounts = setup_censored_accounts()

    since_id = None
    if os.path.exists(settings.LatestTweetIDFile):
        with open(settings.LatestTweetIDFile) as fp:
            since_id = int(fp.read().strip())

    logger.info(f'since_id: {since_id}')
    tweets = agent.collect(
        max_repeat=args.max_repeat,
        since_id=since_id,
        censored=censored_accounts,
    )

    if len(tweets) == 0:
        return

    key = '{}.json'.format(datetime.now().strftime('%Y%m%d_%H%M%S'))
    tweet_repository.put(key, tweets)

    render_all(tweets, args.output_dir, args.skip_target_date, rebuild=False)

    censored_accounts.save()

    if len(tweets) == 0:
        return
    latest_tweet = tweets[0]
    logger.info(f'save latest tweet id: {latest_tweet.tweet_id}')
    with open(settings.LatestTweetIDFile, 'w') as fp:
        fp.write(str(latest_tweet.tweet_id))


def command_delete(args: argparse.Namespace) -> None:
    # TODO 実装
    pass


def date_type(date_str: str) -> date:
    return date.fromisoformat(date_str)


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
        '--max-repeat',
        type=int,
        default=10,
    )
    build_parser.add_argument(
        '-t', '--tweet-id',
        nargs='+',
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
