#!/usr/bin/env python3

import argparse
import io
import json
import logging
from typing import List

import boto3  # type: ignore

from chalicelib import settings

logger = logging.getLogger(__name__)
s3 = boto3.resource('s3')
s3bucket = s3.Bucket(settings.S3Bucket)


def getdata(key: str) -> List[str]:
    bio = io.BytesIO()
    s3bucket.download_fileobj(key, bio)
    logger.info('finished to download a file')
    return json.loads(bio.getvalue().decode('utf-8'))


def putdata(key: str, data: List[str]) -> None:
    bio = io.BytesIO(json.dumps(data).encode('utf-8'))
    s3bucket.upload_fileobj(
        bio,
        key,
        ExtraArgs={'ContentType': 'application/json'},
    )
    logger.info('finished to upload a file')


def main(args: argparse.Namespace):
    target_key = settings.SettingsDir + '/' + settings.CensoredAccountsFile

    existing_accounts = getdata(target_key)
    logger.info('existing accounts: %s', existing_accounts)
    with open(settings.CensoredAccountsFile, 'w') as fp:
        fp.write(json.dumps(existing_accounts, indent=4))

    if not args.accounts_file and not args.safe_account and not args.add_account:
        logger.info('skipping merge process')
    else:
        new_accounts = []
        if args.accounts_file:
            new_accounts = args.accounts_file.read().strip().split()
            logger.info('new accounts: %s', new_accounts)

        if args.add_account:
            if args.add_account in existing_accounts:
                logger.info("account %s already exists", args.add_account)
            else:
                new_accounts.append(args.add_account)
                logger.info('new account added: %s', args.add_account)

        if args.safe_account in existing_accounts:
            logger.info('safe account found: %s', args.safe_account)
            existing_accounts.remove(args.safe_account)
            logger.info('removed safe account: %s', args.safe_account)

        merged_accounts = existing_accounts + new_accounts
        logger.info('merged accounts: %s', merged_accounts)

        putdata(target_key, merged_accounts)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-l', '--loglevel',
        choices=('debug', 'info'),
        default='info',
    )
    parser.add_argument('--accounts-file', type=argparse.FileType('r'))
    parser.add_argument('--add-account')
    parser.add_argument('--safe-account')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    logging.basicConfig(
        level=args.loglevel.upper(),
        format='%(asctime)s [%(levelname)s] %(message)s',
    )
    main(args)
