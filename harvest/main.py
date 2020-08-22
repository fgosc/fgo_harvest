#!/usr/bin/env python3

"""
    ローカル実行用のエントリポイント。
"""

import argparse
import logging
import os
from datetime import datetime

from chalicelib import settings, storage, twitter, recording

logger = logging.getLogger(__name__)


def main(args):
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

    storage_dir = os.path.join(args.output_dir, 'tweets')
    if not os.path.exists(storage_dir):
        os.makedirs(storage_dir)
    tweet_storage = recording.FilesystemTweetStorage(output_dir=storage_dir)

    censored_accounts = twitter.CensoredAccounts(
        fileStorage=storage.FilesystemStorage(),
        filepath=settings.CensoredAccountsFile,
    )

    if not args.rebuild:
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
        tweet_storage.put(key, tweets)

    else:
        tweets = tweet_storage.readall(set(censored_accounts.list()))

    recorders = []

    contents_date_dir = os.path.join(args.output_dir, 'contents', 'date')
    if not os.path.exists(contents_date_dir):
        os.makedirs(contents_date_dir)
    recorder_bydate = recording.Recorder(
        partitioningRule=recording.PartitioningRuleByDate(),
        fileStorage=storage.FilesystemStorage(),
        basedir=contents_date_dir,
        formats=(
            recording.OutputFormat.JSON,
            recording.OutputFormat.DATEHTML,
        ),
    )
    recorders.append(recorder_bydate)

    contents_user_dir = os.path.join(args.output_dir, 'contents', 'user')
    if not os.path.exists(contents_user_dir):
        os.makedirs(contents_user_dir)
    recorder_byuser = recording.Recorder(
        partitioningRule=recording.PartitioningRuleByUser(),
        fileStorage=storage.FilesystemStorage(),
        basedir=contents_user_dir,
        formats=(
            recording.OutputFormat.JSON,
            recording.OutputFormat.USERHTML,
        ),
    )
    recorders.append(recorder_byuser)

    contents_quest_dir = os.path.join(args.output_dir, 'contents', 'quest')
    if not os.path.exists(contents_quest_dir):
        os.makedirs(contents_quest_dir)
    recorder_byquest = recording.Recorder(
        partitioningRule=recording.PartitioningRuleByQuest(),
        fileStorage=storage.FilesystemStorage(),
        basedir=contents_quest_dir,
        formats=(
            recording.OutputFormat.JSON,
            recording.OutputFormat.QUESTHTML,
        ),
    )
    recorders.append(recorder_byquest)

    # 出力先は contents_user_dir
    recorder_byuserlist = recording.Recorder(
        partitioningRule=recording.PartitioningRuleByUserList(),
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
        partitioningRule=recording.PartitioningRuleByQuestList(args.rebuild),
        fileStorage=storage.FilesystemStorage(),
        basedir=contents_quest_dir,
        formats=(
            recording.OutputFormat.JSON,
            recording.OutputFormat.QUESTLISTHTML,
        ),
    )
    recorders.append(recorder_byquestlist)

    contents_error_dir = os.path.join(args.output_dir, 'contents', 'errors')
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

    ignore_original = args.rebuild
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

    if not args.rebuild:
        censored_accounts.save()

        if len(tweets) == 0:
            return
        latest_tweet = tweets[0]
        logger.info(f'save latest tweet id: {latest_tweet.tweet_id}')
        with open(settings.LatestTweetIDFile, 'w') as fp:
            fp.write(str(latest_tweet.tweet_id))


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--loglevel',
        choices=('debug', 'info', 'warning'),
        default='info',
    )
    parser.add_argument(
        '--max-repeat',
        type=int,
        default=10,
    )
    parser.add_argument(
        '--tweet-id',
        nargs='+',
    )
    parser.add_argument(
        '--rebuild',
        action='store_true',
    )
    parser.add_argument(
        '--output-dir',
        default='output'
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    log_format = '%(asctime)s [%(levelname)s] %(message)s'
    logging.basicConfig(level=args.loglevel.upper(), format=log_format)
    main(args)
