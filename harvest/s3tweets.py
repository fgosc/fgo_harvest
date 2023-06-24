#!/usr/bin/env python3

"""
    S3 に補完されたツイートログを管理するコマンド。
"""

import argparse
import json
import logging
import pathlib
from datetime import datetime, timedelta
from operator import itemgetter
from typing import Any, Dict, List

import boto3  # type: ignore

from chalicelib import settings

logger = logging.getLogger(__name__)


def get_s3bucket(profile):
    if profile:
        session = boto3.Session(profile_name=profile)
        return session.resource('s3').Bucket(settings.S3Bucket)
    return boto3.resource('s3').Bucket(settings.S3Bucket)


def exec_pull(args):
    """
        S3 の JSON ファイルを一括でダウンロードする。
    """
    s3bucket = get_s3bucket(args.profile)
    output_dir = pathlib.Path(args.output_dir)
    output_dir.mkdir(exist_ok=True)

    if args.days:
        threshold_date = datetime.today() - timedelta(args.days)
        threshold = (threshold_date).strftime('%Y%m%d')
        logger.info('try to get files that are created after %s', threshold)
    else:
        threshold = None

    object_summary_iterator = s3bucket.objects.filter(
        Prefix=settings.TweetStorageDir,
    )

    for object_summary in object_summary_iterator:
        key = object_summary.key
        logger.info('s3: %s', key)

        name = key.replace(settings.TweetStorageDir + '/', '')

        if threshold and name < threshold:
            logger.info('  skip downloading')
            continue

        output = output_dir / name
        logger.info(' --> %s', output)

        resp = object_summary.get()
        with open(output, 'wb') as fp:
            fp.write(resp['Body'].read())


def all_tweets_in_id_set(tweets: list[dict[str, Any]], id_set: set[int]) -> bool:
    return all([int(tw["id"]) in id_set for tw in tweets])


def exec_scan(args: argparse.Namespace) -> None:
    """
        ファイルが不要かどうか調べる。
        そのファイルに収録された id がすべて上位ファイルに含まれていれば、そのファイルは不要と判断できる。
        - あるファイルが時間単位ファイルである -> 月単位ファイルがあればそれを、なければ日単位ファイルを参照。日単位ファイルもなければそのファイルは削除不可
        - あるファイルが日単位ファイルである -> 月単位ファイルを参照。月単位ファイルがなければそのファイルは削除不可
    """
    target_dir = pathlib.Path(args.target_dir)
    files = target_dir.glob("*.json")

    month_files: list[pathlib.Path] = []
    month_dict: dict[str, set[int]] = {}
    date_files: list[pathlib.Path] = []
    date_dict: dict[str, set[int]] = {}
    time_files: list[pathlib.Path] = []

    for filepath in files:
        name_length = len(filepath.name)
        if name_length == 6 + 5:
            month_files.append(filepath)

            s = set()
            with open(filepath) as fp:
                tweets = json.load(fp)
            for tw in tweets:
                s.add(tw["id"])
            month_dict[filepath.stem] = s

        elif name_length == 8 + 5:
            date_files.append(filepath)

            s = set()
            with open(filepath) as fp:
                tweets = json.load(fp)
            for tw in tweets:
                s.add(tw["id"])
            date_dict[filepath.stem] = s

        elif name_length == 8 + 1 + 6 + 5:
            time_files.append(filepath)
        else:
            logger.warning("does not match any pattern: %s", filepath)

    logger.info(sorted(month_dict.keys()))
    logger.info(sorted(date_dict.keys()))

    for filepath in time_files:
        if filepath.stem[:6] in month_dict:
            id_set = month_dict[filepath.stem[:6]]

            with open(filepath) as fp:
                tweets = json.load(fp)

            included = all_tweets_in_id_set(tweets, id_set)
            if included:
                logger.debug("all elements are included in month file: %s", filepath)
                if args.dry_run:
                    logger.info("(fake) deleted: %s", filepath)
                else:
                    filepath.unlink()
                    logger.info("deleted: %s", filepath)
                continue

        if filepath.stem[:8] in date_dict:
            id_set = date_dict[filepath.stem[:8]]

            with open(filepath) as fp:
                tweets = json.load(fp)

            included = all_tweets_in_id_set(tweets, id_set)
            if included:
                logger.debug("all elements are included in date file: %s", filepath)
                if args.dry_run:
                    logger.info("(fake) deleted: %s", filepath)
                else:
                    filepath.unlink()
                    logger.info("deleted: %s", filepath)
                continue

        logger.info("some tweets are not include in month/date files: %s", filepath)

    for filepath in date_files:
        if filepath.stem[:6] in month_dict:
            id_set = month_dict[filepath.stem[:6]]

            with open(filepath) as fp:
                tweets = json.load(fp)

            included = all_tweets_in_id_set(tweets, id_set)
            if included:
                logger.debug("all elements are included in month file: %s", filepath)
                if args.dry_run:
                    logger.info("(fake) deleted: %s", filepath)
                else:
                    filepath.unlink()
                    logger.info("deleted: %s", filepath)
                continue

        logger.info("some tweets are not include in month files: %s", filepath)


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


def exec_merge(args: argparse.Namespace) -> None:
    """
        JSON ファイルを日付または月単位でマージする。
    """
    target_dir = pathlib.Path(args.target_dir)
    output_dir = pathlib.Path(args.output_dir)
    output_dir.mkdir(exist_ok=True)
    today = datetime.today().strftime('%Y%m%d')
    this_month = datetime.today().strftime('%Y%m')

    files = target_dir.glob('*.json')
    partition: Dict[str, List[pathlib.Path]] = {}

    for filepath in files:
        month = filepath.name[:6]
        # 当月は月単位マージの対象外
        if month < this_month:
            if month not in partition:
                partition[month] = []
            partition[month].append(filepath)
            continue

        date = filepath.name[:8]
        if date >= today:
            logger.info(
                'skip merging: %s equals or later than %s',
                filepath.name,
                today,
            )
        logger.info('%s %s', filepath.name, date)
        if date not in partition:
            partition[date] = []
        partition[date].append(filepath)

    for date, filepaths in partition.items():
        logger.info(date)
        merged = merge(filepaths)
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


def exec_push(args: argparse.Namespace) -> None:
    """
        JSON ファイルを S3 にアップロードする。
    """
    s3bucket = get_s3bucket(args.profile)
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


def exec_clean(args: argparse.Namespace) -> None:
    """
        target_dir にある JSON ファイルと同名のファイルを S3 から削除する。

        以下の場合は削除しない:
        - yyyyMMdd.json
        - yyyyMMdd.json が存在しないときの yyyyMMdd_HHMMSS.json
          (つまりサマリされていない場合はサマリ元は消さない)

        --month が指定された場合は削除しない対象を以下に変更:
        - yyyyMM.json
        - yyyyMM.json が存在しないときの yyyyMMdd.json, yyyyMMdd_HHMMSS.json
    """
    s3bucket = get_s3bucket(args.profile)
    object_summary_iterator = s3bucket.objects.filter(
        Prefix=settings.TweetStorageDir,
    )
    output_dir = pathlib.Path(args.target_dir)

    logger.info('target directory: %s', output_dir)

    if args.month:
        mask_length = 6
    else:
        mask_length = 8

    summary_files = set()
    target_object_summaries = []

    # 最初にサマリファイルだけを探索する
    for object_summary in object_summary_iterator:
        key = object_summary.key

        name = key.replace(settings.TweetStorageDir + '/', '')
        summary_name = pathlib.Path(name).stem
        if len(summary_name) == mask_length:
            logger.debug('s3: %s', key)
            logger.info('skip deleting a summary file: %s', name)
            summary_files.add(summary_name)
            continue
        target_object_summaries.append(object_summary)

    logger.info('summary files: %s', sorted(summary_files))

    # サマリファイル以外を処理する
    for object_summary in target_object_summaries:
        key = object_summary.key
        logger.debug('s3: %s', key)

        name = key.replace(settings.TweetStorageDir + '/', '')
        stem = pathlib.Path(name).stem[:mask_length]

        if stem not in summary_files:
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
        subparser.add_argument("--profile")

    subparsers = parser.add_subparsers(dest='command')

    pull_parser = subparsers.add_parser('pull')
    pull_parser.add_argument('-o', '--output-dir', default='output/s3tweets')
    pull_parser.add_argument('--days', type=int)
    add_common_arguments(pull_parser)
    pull_parser.set_defaults(func=exec_pull)

    scan_parser = subparsers.add_parser("scan")
    scan_parser.add_argument('-d', '--target-dir', default='output/s3tweets')
    scan_parser.add_argument('--dry-run', action='store_true')
    add_common_arguments(scan_parser)
    scan_parser.set_defaults(func=exec_scan)

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
    g = clean_parser.add_mutually_exclusive_group()
    g.add_argument('--month', action='store_true')
    g.add_argument('--day', action='store_true')
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
