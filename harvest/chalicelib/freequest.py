import json
import os
from base64 import urlsafe_b64encode
from hashlib import md5
from logging import getLogger
from typing import Dict, Iterable, List, Optional, Set, Tuple

logger = getLogger(__name__)


# 同じ chapter, place で quest が 2 つ存在する特殊なケース。
quests_in_same_place = (
    ('オケアノス', '群島', '静かな入り江'),
    ('オケアノス', '群島', '隠された島'),
    ('下総国', '裏山', '名もなき霊峰'),
    ('下総国', '裏山', '戦戦恐恐'),
)
prior_in_same_place = (
    '静かな入り江',
    '名もなき霊峰',
)
posterior_in_same_place = (
    '隠された島',
    '戦戦恐恐',
)


class Detector:
    def __init__(self, freequests: List[Dict[str, str]]):
        self.freequest_db: Dict[str, str] = _build_db(freequests)
        self.freequest_chapter_db: Set[str] = _build_chapter_db(freequests)
        self.freequest_place_index: Dict[str, str] = \
            _build_place_index(freequests)
        self.eventquest_cache: Dict[str, str] = {}
        self.quest_reverse_index: Dict[str, str] = \
            _build_reverse_index(freequests)

    def is_freequest(self, chapter: str, place: str) -> bool:
        """
            渡された chapter, place がフリークエストかどうかを判定する。
            かなり多様なパターンに対応している。実際にどのようなパターンで
            True と判定されるかは freequest_test.py の例を見るとよい。
        """
        key_for_freequest = f'{chapter}\t{place}'
        return key_for_freequest in self.freequest_db

    def get_quest_id(self, chapter: str, place: str, year: int) -> str:
        key_for_freequest = f'{chapter}\t{place}'
        key_for_eventquest = f'{chapter}\t{place}\t{year}'

        if key_for_freequest in self.freequest_db:
            return self.freequest_db[key_for_freequest]

        elif key_for_eventquest in self.eventquest_cache:
            return self.eventquest_cache[key_for_eventquest]

        encoded_key = key_for_eventquest.encode('utf-8')
        b64digest = urlsafe_b64encode(md5(encoded_key).digest())
        qid = b64digest[:12].decode('utf-8')
        self.eventquest_cache[key_for_eventquest] = qid
        self.quest_reverse_index[qid] = f'[{year}] {chapter} {place}'
        return qid

    def get_quest_name(self, qid: str) -> str:
        return self.quest_reverse_index[qid]

    def find_freequest(self, expr: str) -> Optional[Tuple[str, str]]:
        """
            バビロニア高原
            シャーロットゴールドラッシュ
            新宿二丁目レインボータウン

            のようにスペースなしで周回場所が投稿された場合に、
            それがフリークエストかどうかを判定する。

            シャーロット
            ゴールドラッシュ

            のように場所あるいはクエスト名が単独で記述される場合は
            そもそも is_freequest() で True と判定されるため、
            このメソッドでフリークエストかどうかを調べる必要がない。
        """
        chapter_candidates = [
            ch for ch in self.freequest_chapter_db
            if expr.startswith(ch)
        ]
        if len(chapter_candidates) > 0:
            msg = 'chapter candidate found: %s (orig: %s)'
            logger.debug(msg, chapter_candidates, expr)

        if len(chapter_candidates) > 1:
            # 複数マッチはバグ以外では考えにくい
            raise ValueError(
                'matched multiple chapters '
                f'{chapter_candidates}: {expr}'
            )

        if len(chapter_candidates) == 1:
            # chapter 候補で実際に切ってみて、フリクエだと判定できるなら
            # フリクエとみなし、分解に成功した chapter, place を返す。
            chapter_candidate = chapter_candidates[0]
            place_candidate = expr[len(chapter_candidate):]
            if self.is_freequest(chapter_candidate, place_candidate):
                return chapter_candidate, place_candidate

        # chapter ではマッチしないケース
        place_candidates = [
            pq for pq in self.freequest_place_index
            if expr.startswith(pq)
        ]
        if len(place_candidates) == 1:
            qid = self.freequest_place_index[place_candidates[0]]
            chapter_place_quest = self.quest_reverse_index[qid]
            _, place, quest = chapter_place_quest.split()
            return place, quest

        elif len(place_candidates) > 1:
            # 複数マッチはバグ以外では考えにくい
            raise ValueError(
                'matched multiple places or quests '
                f'{place_candidates}: {expr}'
            )
        else:
            return None


def _build_db(freequests: List[Dict[str, str]]) -> Dict[str, str]:
    d: Dict[str, str] = {}

    for fq in freequests:
        qid = fq['id']
        chapter = fq['chapter']
        place = fq['place']
        alt_place = place.replace('・', '') if '・' in place else ''
        quest = fq['quest']
        alt_quest = quest.replace('・', '') if '・' in quest else ''

        # 修練場のようにクエスト名がないパターンがあるので
        # 存在チェックが必要。
        if quest:
            d[f'{chapter}\t{quest}'] = qid
            if alt_quest:
                d[f'{chapter}\t{alt_quest}'] = qid

            d[f'{chapter} {place}\t{quest}'] = qid
            if alt_place:
                d[f'{chapter} {alt_place}\t{quest}'] = qid
                if alt_quest:
                    d[f'{chapter} {alt_place}\t{alt_quest}'] = qid

            d[(f'{place}\t{quest}')] = qid
            if alt_place:
                d[(f'{alt_place}\t{quest}')] = qid
                if alt_quest:
                    d[(f'{alt_place}\t{alt_quest}')] = qid

        if (chapter, place, quest) not in quests_in_same_place \
                or quest in prior_in_same_place:

            if f'{chapter}\t{place}' in d:
                raise KeyError(
                    f'key "{chapter} {place}" has already been registered'
                )
            d[f'{chapter}\t{place}'] = qid
            if alt_place:
                d[f'{chapter}\t{alt_place}'] = qid

            # 場所だけの投稿
            d[f'{place}\t'] = qid
            if alt_place:
                d[f'{alt_place}\t'] = qid

        # クエスト名だけの投稿
        d[f'{quest}\t'] = qid
        if alt_quest:
            d[f'{alt_quest}\t'] = qid

    # 周回カウンタに登録されているクエスト名が特殊
    d['オルレアン\tティエール(刃物の町)'] = d['オルレアン\tティエール']
    d['セプテム\tゲルマニア(黒い森)'] = d['セプテム\tゲルマニア']

    return d


def _build_reverse_index(
    freequests: Iterable[Dict[str, str]]
) -> Dict[str, str]:
    d: Dict[str, str] = {}

    for fq in freequests:
        qid = fq['id']
        chapter = fq['chapter']
        place = fq['place']
        quest = fq['quest']

        d[qid] = ' '.join([chapter, place, quest])

    return d


def _build_chapter_db(freequests: Iterable[Dict[str, str]]) -> Set[str]:
    s = set()

    for fq in freequests:
        if fq['chapter'] not in s:
            s.add(fq['chapter'])
        # 歴史的事情を考慮
        if fq['chapter'] == '北米':
            if fq['place'] in s:
                msg = f'key {fq["place"]} has already been registered'
                raise KeyError(msg)
            s.add(fq['place'])

    return s


def _build_place_index(
    freequests: Iterable[Dict[str, str]]
) -> Dict[str, str]:
    d: Dict[str, str] = {}

    for fq in freequests:
        qid = fq['id']
        place = fq['place']
        quest = fq['quest']

        # 群島、裏山のような複数クエストある場所の場合は優先権のあるほうだけを採用
        if place and quest not in posterior_in_same_place:
            d[place] = qid

    return d


with open(os.path.join(os.path.dirname(__file__), 'freequest.json')) as fp:
    defaultDetector = Detector(json.load(fp))
