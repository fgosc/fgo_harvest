#!/usr/bin/env python3

"""
    ローカル実行用のエントリポイント。
"""

import argparse
import logging
import os
from datetime import datetime

from chalicelib import settings, twitter, recording

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
        return

    storage_dir = os.path.join(args.output_dir, 'tweets')
    if not os.path.exists(storage_dir):
        os.makedirs(storage_dir)
    storage = recording.FilesystemTweetStorage(output_dir=storage_dir)

    if not args.rebuild:
        since_id = None
        if os.path.exists(settings.LatestTweetIDFile):
            with open(settings.LatestTweetIDFile) as fp:
                since_id = int(fp.read().strip())

        logger.info(f'since_id: {since_id}')
        tweets = agent.collect(
            since_id=since_id,
            max_repeat=args.max_repeat,
            exclude_accounts=settings.ExcludeAccounts,
        )

        key = '{}.json'.format(datetime.now().strftime('%Y%m%d_%H%M%S'))
        storage.put(key, tweets)
    else:
        tweets = storage.readall()

    contents_date_dir = os.path.join(args.output_dir, 'contents', 'date')
    if not os.path.exists(contents_date_dir):
        os.makedirs(contents_date_dir)
    recorder_bydate = recording.FilesystemRecorder(
        rootdir=contents_date_dir,
        partitioningRule=recording.PartitioningRuleByDate(),
        formats=(recording.OutputFormat.JSON, recording.OutputFormat.DATEHTML),
    )

    contents_user_dir = os.path.join(args.output_dir, 'contents', 'user')
    if not os.path.exists(contents_user_dir):
        os.makedirs(contents_user_dir)
    recorder_byuser = recording.FilesystemRecorder(
        rootdir=contents_user_dir,
        partitioningRule=recording.PartitioningRuleByUser(),
        formats=(recording.OutputFormat.JSON, recording.OutputFormat.USERHTML),
    )

    contents_quest_dir = os.path.join(args.output_dir, 'contents', 'quest')
    if not os.path.exists(contents_quest_dir):
        os.makedirs(contents_quest_dir)
    recorder_byquest = recording.FilesystemRecorder(
        rootdir=contents_quest_dir,
        partitioningRule=recording.PartitioningRuleByQuest(),
        formats=(recording.OutputFormat.JSON, recording.OutputFormat.QUESTHTML),
    )

    for tweet in tweets:
        try:
            report = twitter.parse_tweet(tweet)
            logger.info(report)
            recorder_bydate.add(report)
            recorder_byuser.add(report)
            recorder_byquest.add(report)

        except twitter.ParseError as e:
            logger.error(e)
            # TODO エラーになったツイートもHTMLには入れたい...
            logger.error(tweet)

    ignore_original = args.rebuild
    recorder_bydate.save(ignore_original=ignore_original)
    recorder_byuser.save(ignore_original=ignore_original)
    recorder_byquest.save(ignore_original=ignore_original)

    if not args.rebuild:
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
