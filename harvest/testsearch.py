#!/usr/bin/env python3

import argparse
import logging

from chalicelib import settings
from chalicelib import twitter

logger = logging.getLogger(__name__)


def init_agent():
    return twitter.Agent(
        consumer_key=settings.TwitterConsumerKey,
        consumer_secret=settings.TwitterConsumerSecret,
        access_token=settings.TwitterAccessToken,
        access_token_secret=settings.TwitterAccessTokenSecret,
    )


def show_results(tweets):
    for tw in tweets:
        msg = (
            f'{tw.id} {tw.user.screen_name} {tw.user.name} '
            f'{tw.created_at} {tw.text}'
        )
        logger.info(msg)

    logger.info(f'total: {len(tweets)}')


def search(args):
    agent = init_agent()
    kwargs = {
        'q': args.query,
    }
    tweets = agent.api.search(**kwargs)
    show_results(tweets)


def user_timeline(args):
    agent = init_agent()
    kwargs = {
        'exclude_replies': True,
        'include_rts': False,
        'count': args.count,
    }
    if args.max_id:
        max_id = args.max_id
    else:
        max_id = None

    tweets = []
    for i in range(args.repeat):
        if max_id:
            kwargs['max_id'] = max_id
        logger.info('trying... %s', i)
        rs = agent.api.user_timeline(args.user, **kwargs)
        if len(rs) == 0:
            break
        tweets.extend(rs)
        max_id = rs[-1].id - 1
        logger.info(max_id)
    show_results(tweets)


def build_parser():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    parser_search = subparsers.add_parser('search')
    parser_search.add_argument('-q', '--query', required=True)
    parser_search.set_defaults(func=search)

    parser_user = subparsers.add_parser('user')
    parser_user.add_argument('user')
    parser_user.add_argument('-c', '--count', type=int, default=20)
    parser_user.add_argument('-m', '--max-id', type=int)
    parser_user.add_argument('-r', '--repeat', type=int, default=1)
    parser_user.set_defaults(func=user_timeline)

    return parser


if __name__ == "__main__":
    parser = build_parser()
    args = parser.parse_args()

    if hasattr(args, 'func'):
        logging.basicConfig(
            level=logging.INFO,
            format='[%(levelname)s] %(message)s',
        )
        args.func(args)
    else:
        parser.print_usage()
