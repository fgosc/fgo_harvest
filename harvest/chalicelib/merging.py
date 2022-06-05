import json
from datetime import date
from logging import getLogger
from operator import itemgetter
from typing import Any, BinaryIO, Iterator

from . import storage

logger = getLogger(__name__)


def _merge(readers: Iterator[BinaryIO]) -> list[dict[str, Any]]:
    merged_tweets = []

    for reader in readers:
        tweets = json.load(reader)
        if len(tweets) == 0:
            continue
        merged_tweets.extend(tweets)

    tweet_set = set([json.dumps(tw) for tw in merged_tweets])
    distinct_tweets = [json.loads(tw) for tw in tweet_set]
    return sorted(distinct_tweets, key=itemgetter("id"))


def merge_into_datefile(
    fileStorage: storage.SupportStorage,
    basedir: str,
    target_date: date,
) -> None:
    """
    JSON ファイルを日付単位でマージする。
    """
    basepath = fileStorage.path_object(basedir)
    date_str = target_date.strftime("%Y%m%d")
    suffix = ".json"
    key = str(basepath / date_str) + suffix
    prefix = date_str + "_"

    if fileStorage.exists(key):
        logger.warning(f"key {key} already exists")
        return

    streams = fileStorage.streams(basedir, prefix, suffix)

    merged = _merge(streams)
    js = json.dumps(merged, ensure_ascii=False)
    out = fileStorage.get_output_stream(key)
    logger.info("merge tweets into %s", key)
    out.write(js.encode("utf-8"))
    fileStorage.close_output_stream(out)

    parts = fileStorage.list(basedir, prefix, suffix)

    for part in parts:
        # 自身とマッチしてしまうのを回避（ないはずだが、念のため）
        if part == key:
            continue
        logger.info("delete: %s", part)
        fileStorage.delete(part)


def merge_into_monthfile(
    fileStorage: storage.SupportStorage,
    basedir: str,
    target_month: str,
) -> None:
    """
    JSON ファイルを日付単位でマージする。
    target_month は YYYYMM 形式。
    """
    basepath = fileStorage.path_object(basedir)
    suffix = ".json"
    key = str(basepath / target_month) + suffix

    if fileStorage.exists(f"{key}"):
        logger.warning(f"key {key} already exists")
        return

    streams = fileStorage.streams(basedir, target_month, suffix)

    merged = _merge(streams)
    js = json.dumps(merged, ensure_ascii=False)
    out = fileStorage.get_output_stream(key)
    logger.info("merging tweets into %s", key)
    out.write(js.encode("utf-8"))

    parts = fileStorage.list(basedir, target_month, suffix)

    for part in parts:
        # 自身とマッチしてしまうのを回避
        if part == key:
            continue
        logger.info("delete: %s", part)
        # TODO 最初の動作確認後に削除を有効にする
        # fileStorage.delete(part)
