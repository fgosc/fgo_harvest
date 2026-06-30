#!/usr/bin/env python3

import argparse
import csv
import json
import logging
import sys
from operator import itemgetter
from typing import Iterable

logger = logging.getLogger(__name__)


harvest_chapter_map: dict[str, str] = {
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
    'イド': '25c',
    'アーキタイプ・インセプション': '25d',
    'トリニティ・メタトロニオス': '25e',
    '冠位戴冠戦': '26a',
    'パスト・カルデア': '30a',
}


def build_syurenquest_dict(reader: csv.DictReader) -> Iterable[dict[str, str]]:
    fq_list: list[dict[str, str]] = []
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


def number_to_suffix(num: int) -> str:
    if num < 1 or num > 26:
        raise ValueError('num must be in range 1-26')
    return chr(96 + num)


def build_freequest_dict(reader: csv.DictReader) -> Iterable[dict[str, str]]:
    fq_list: list[dict[str, str]] = []
    prev_chapter: str = ''
    prev_place: str = ''
    counter: int = 0
    subcounter_dict: dict[str, int] = {
        'オーディール・コール': 0,
        '冠位戴冠戦': 0,
    }

    for row in reader:
        current_chapter = row['chapter']
        current_place = row['place']
        current_quest = row['quest']

        # chapter が切り替わったらカウンターをリセット
        if prev_chapter != current_chapter:
            counter = 1

        # オーディール・コールと冠位戴冠戦のみ特別な処理
        if current_chapter not in ('オーディール・コール', '冠位戴冠戦'):
            suffix = ''
        elif current_chapter == 'オーディール・コール':
            subcounter = subcounter_dict[current_chapter]
            # place が切り替わったら subcounter をリセット
            if prev_place != current_place:
                subcounter = 1
            else:
                subcounter += 1
                # subcounter を増やすときは counter は止める
                counter -= 1
            suffix = number_to_suffix(subcounter)
            # 書き戻す
            subcounter_dict[current_chapter] = subcounter

        elif current_chapter == '冠位戴冠戦':
            subcounter = subcounter_dict[current_chapter]
            # freequest.csv の冠位戴冠戦記載ルールが harvest と非互換であるため、強引に合わせる
            # ex) 冠位研鑽戦〔セイバー〕 Ⅰ
            # place と quest にフルネームでクエスト名を入れる
            current_place = row['scName']
            current_quest = row['scName']
            # カウンター切り替え用の place
            # ex) 冠位研鑽戦〔セイバー〕 Ⅰ → 冠位研鑽戦〔セイバー〕
            # 何も考えずに連番で採番してしまうと後から各クラスのクエストが増えた場合に対応難易度が上がるのと
            # 数字2桁を超える可能性があるので、クラス単位で連番をまとめておきたいという意図。
            place_for_grand_duel = current_place.split()[0]
            # place_for_grand_duel が切り替わったら subcounter をリセット
            if prev_place != place_for_grand_duel:
                subcounter = 1
            else:
                subcounter += 1
                # subcounter を増やすときは counter は止める
                counter -= 1
            suffix = number_to_suffix(subcounter)
            # 書き戻す
            subcounter_dict[current_chapter] = subcounter

        id_prefix = harvest_chapter_map[current_chapter]

        d = {
            'id': f'{id_prefix}{counter:0>2}{suffix}',
            'internal_id': row['id'],
            'chapter': row['chapter'],
            'place': current_place,
            'quest': current_quest,
        }
        fq_list.append(d)
        logger.debug(d)

        counter += 1
        prev_chapter = current_chapter
        if current_chapter == '冠位戴冠戦':
            prev_place = place_for_grand_duel
        else:
            prev_place = current_place

    return sorted(fq_list, key=itemgetter('id'))


def main(args: argparse.Namespace) -> None:
    freequest_reader = csv.DictReader(args.freequest_csv)
    syuren_reader = csv.DictReader(args.syurenquest_csv)

    all_list: list[dict[str, str]] = []
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
