#!/usr/bin/env python3

import argparse
import csv
import json
import logging
import sys
from operator import itemgetter
from typing import Iterable, Dict, List

logger = logging.getLogger(__name__)


harvest_chapter_map: Dict[str, str] = {
    '剣の修練場': '00a',
    '弓の修練場': '00b',
    '槍の修練場': '00c',
    '騎の修練場': '00d',
    # 過去データとの互換性のため e は飛ばす
    '術の修練場': '00f',
    '殺の修練場': '00g',
    '狂の修練場': '00h',
    '冬木': '10a',
    'オルレアン': '10b',
    'セプテム': '10c',
    'オケアノス': '10d',
    'ロンドン': '10e',
    '北米': '10f',
    'キャメロット': '10g',
    'バビロニア': '10h',
    '新宿': '15a',
    'アガルタ': '15b',
    '下総国': '15c',
    'セイレム': '15d',
    'アナスタシア': '20a',
    'ゲッテルデメルング': '20b',
    'シン': '20c',
    'ユガ・クシェートラ': '20d',
    'アトランティス': '20e',
    'オリュンポス': '20f',
    '平安京': '20g',
    'アヴァロン': '20h',
    'トラオム': '20i',
    'ナウイ・ミクトラン': '20j',
    'オーディール・コール': '25a',
    'ペーパームーン': '25b',
}


def build_syurenquest_dict(reader: csv.DictReader) -> Iterable[Dict[str, str]]:
    fq_list: List[Dict[str, str]] = []
    prev_chapter: str = ''
    counter: int = 1

    for row in reader:
        current_chapter, place = row['shortname'].split()
        if prev_chapter != current_chapter:
            counter = 1
        id_prefix = harvest_chapter_map[current_chapter]
        d = {
            'id': f'{id_prefix}{counter:0>2}',
            'internal_id': row['id'],
            'chapter': current_chapter,
            'place': place,
            'quest': '',  # quest は設定しない
        }
        fq_list.append(d)
        prev_chapter = current_chapter
        counter += 1

    return sorted(fq_list, key=itemgetter('id'))


def build_freequest_dict(reader: csv.DictReader) -> Iterable[Dict[str, str]]:
    fq_list: List[Dict[str, str]] = []
    prev_chapter: str = ''
    counter: int = 1

    for row in reader:
        current_chapter = row['chapter']
        if prev_chapter != current_chapter:
            counter = 1
        id_prefix = harvest_chapter_map[current_chapter]
        d = {
            'id': f'{id_prefix}{counter:0>2}',
            'internal_id': row['id'],
            'chapter': row['chapter'],
            'place': row['place'],
            'quest': row['quest'],
        }
        fq_list.append(d)
        prev_chapter = current_chapter
        counter += 1

    return sorted(fq_list, key=itemgetter('id'))


def main(args: argparse.Namespace) -> None:
    freequest_reader = csv.DictReader(args.freequest_csv)
    syuren_reader = csv.DictReader(args.syurenquest_csv)

    all_list: List[Dict[str, str]] = []
    syuren_list = build_syurenquest_dict(syuren_reader)
    all_list.extend(syuren_list)

    fq_list = build_freequest_dict(freequest_reader)
    all_list.extend(fq_list)

    json.dump(all_list, args.output, ensure_ascii=False, indent=2)
    args.output.write("\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-fc',
        '--freequest_csv',
        type=argparse.FileType('r', encoding='utf-8-sig'),
        required=True,
    )
    parser.add_argument(
        '-sc',
        '--syurenquest_csv',
        type=argparse.FileType('r', encoding='utf-8-sig'),
        required=True,
    )
    parser.add_argument(
        '-o',
        '--output',
        type=argparse.FileType('w'),
        default=sys.stdout,
    )
    parser.add_argument(
        '-l',
        '--loglevel',
        choices=('debug', 'info', 'warn'),
        default='info',
    )
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    logging.basicConfig(
        level=args.loglevel.upper(),
        format='%(asctime)s [%(levelname)s] %(message)s',
    )
    main(args)
