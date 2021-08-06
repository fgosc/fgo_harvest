#!/usr/bin/env python3

"""
    S3 に補完されたツイートログを管理するコマンド。
"""

import argparse
import json
import logging
import pathlib
from operator import itemgetter
from typing import Any, Dict, List

import boto3  # type: ignore

from chalicelib import settings

logger = logging.getLogger(__name__)
s3 = boto3.resource('s3')
s3bucket = s3.Bucket(settings.S3Bucket)


def exec_pull(args):
    """
        S3 の JSON ファイルを一括でダウンロードする。
    """
    output_dir = pathlib.Path(args.output_dir)
    output_dir.mkdir(exist_ok=True)

    object_summary_iterator = s3bucket.objects.filter(
        Prefix=settings.TweetStorageDir,
    )

    for object_summary in object_summary_iterator:
        key = object_summary.key
        logger.info('s3: %s', key)

        name = key.replace(settings.TweetStorageDir + '/', '')
        output = output_dir / name
        logger.info(' --> %s', output)

        resp = object_summary.get()
        with open(output, 'wb') as fp:
            fp.write(resp['Body'].read())


def merge(files: List[pathlib.Path]) -> List[Dict[str, Any]]:
    merged_tweets = []
    for filepath in files:
        with open(filepath) as fp:
            tweets = json.load(fp)
        if len(tweets) == 0:
            continue
        merged_tweets.extend(tweets)
    logger.info('merged tweets: %s', len(merged_tweets))
    tweet_set = set([json.dumps(tw) for tw in merged_tweets])
    distinct_tweets = [json.loads(tw) for tw in tweet_set]
    logger.info('distinct tweets: %s', len(distinct_tweets))
    return sorted(distinct_tweets, key=itemgetter('id'))


def exec_merge(args):
    """
        JSON ファイルを日付単位でマージする。
    """
    target_dir = pathlib.Path(args.target_dir)
    output_dir = pathlib.Path(args.output_dir)
    output_dir.mkdir(exist_ok=True)

    files = target_dir.glob('*.json')
    partition: Dict[str, List[pathlib.Path]] = {}

    for filepath in files:
        date = filepath.name[:8]
        logger.info('%s %s', filepath.name, date)
        if date not in partition:
            partition[date] = []
        partition[date].append(filepath)

    for date, files in partition.items():
        logger.info(date)
        merged = merge(files)
        filename = f'{date}.json'
        filepath = output_dir / filename
        logger.info(f'save tweets to {filepath}')
        with open(filepath, 'w') as fp:
            json.dump(merged, fp, ensure_ascii=False)


def checksum(filepath):
    import hashlib
    with open(filepath, 'rb') as fp:
        data = fp.read()
    return hashlib.sha1(data).hexdigest()


def exec_push(args):
    """
        JSON ファイルを S3 にアップロードする。
    """
    target_dir = pathlib.Path(args.target_dir)
    files = target_dir.glob('*.json')
    basepath = pathlib.PurePosixPath(settings.TweetStorageDir)

    test_dir = pathlib.Path(args.test_dir)

    for filepath in files:
        testfile = test_dir / filepath.name
        if testfile.exists() and checksum(filepath) == checksum(testfile):
            logger.info(
                'skip uploading %s (probably already exists on S3)',
                filepath,
            )
            continue

        key = str(basepath / filepath.name)
        src = str(filepath)
        logger.info(f'{src} -> s3: {key}')
        if args.dry_run:
            logger.info('skip uploading (--dry-run)')
            continue
        s3bucket.upload_file(
            src,
            key,
            ExtraArgs={'ContentType': 'application/json'},
        )


def exec_clean(args):
    """
        target_dir にある JSON ファイルと同名のファイルを S3 から削除する。

        以下の場合は削除しない:
        - yyyyMMdd.json
        - yyyyMMdd.json が存在しないときの yyyyMMdd_HHMMSS.json
          (つまりサマリされていない場合はサマリ元は消さない)
    """
    object_summary_iterator = s3bucket.objects.filter(
        Prefix=settings.TweetStorageDir,
    )
    output_dir = pathlib.Path(args.target_dir)

    summary_files = set()
    target_object_summaries = []

    # 最初にサマリファイルだけを探索する
    for object_summary in object_summary_iterator:
        key = object_summary.key

        name = key.replace(settings.TweetStorageDir + '/', '')
        summary_name = pathlib.Path(name).stem
        if len(summary_name) == 8:
            logger.debug('s3: %s', key)
            logger.info('skip deleting a summary file: %s', name)
            summary_files.add(summary_name)
            continue
        target_object_summaries.append(object_summary)

    # サマリファイル以外を処理する
    for object_summary in target_object_summaries:
        key = object_summary.key
        logger.debug('s3: %s', key)

        name = key.replace(settings.TweetStorageDir + '/', '')
        stem = pathlib.Path(name).stem
        ymd, _ = stem.split('_')
        if ymd not in summary_files:
            logger.info('skip deleting a non-summarized file: %s', name)
            continue

        output = output_dir / name
        logger.debug('local: %s, exists: %s', output, output.exists())

        if output.exists():
            if args.dry_run:
                logger.info('(fake) deleted: %s', key)
            else:
                resp = object_summary.delete()
                logger.debug(resp)
                logger.info('deleted: %s', key)
        else:
            logger.info('skip deleting (should not be deleted): %s', key)


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

    pull_parser = subparsers.add_parser('pull')
    pull_parser.add_argument('-o', '--output-dir', default='output/s3tweets')
    add_common_arguments(pull_parser)
    pull_parser.set_defaults(func=exec_pull)

    merge_parser = subparsers.add_parser('merge')
    merge_parser.add_argument('-d', '--target-dir', default='output/s3tweets')
    merge_parser.add_argument(
        '-o',
        '--output-dir',
        default='output/mergedtweets',
    )

    add_common_arguments(merge_parser)
    merge_parser.set_defaults(func=exec_merge)
    push_parser = subparsers.add_parser('push')
    push_parser.add_argument(
        '-d',
        '--target-dir',
        default='output/mergedtweets',
    )
    push_parser.add_argument(
        '--test-dir',
        default='output/s3tweets',
    )
    push_parser.add_argument('--dry-run', action='store_true')
    add_common_arguments(push_parser)
    push_parser.set_defaults(func=exec_push)

    clean_parser = subparsers.add_parser('clean')
    clean_parser.add_argument(
        '-d',
        '--target-dir',
        default='output/s3tweets',
    )
    clean_parser.add_argument('--dry-run', action='store_true')
    add_common_arguments(clean_parser)
    clean_parser.set_defaults(func=exec_clean)
    return parser


if __name__ == '__main__':
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
