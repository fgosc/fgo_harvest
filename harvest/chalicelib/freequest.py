import json
import os
from base64 import urlsafe_b64encode
from difflib import SequenceMatcher
from hashlib import md5
from logging import getLogger
from typing import Iterable

logger = getLogger(__name__)


# 同じ chapter, place で quest が 2 つ以上存在するケース。
quests_in_same_place = (
    ('オケアノス', '群島', '静かな入り江'),
    ('オケアノス', '群島', '隠された島'),
    ('下総国', '裏山', '名もなき霊峰'),
    ('下総国', '裏山', '戦戦恐恐'),
    ('オーディール・コール', 'ハワイエリア', '常夏の休暇'),
    ('オーディール・コール', 'ハワイエリア', '常夏即売会場'),
    ('オーディール・コール', '北大西洋エリア', '光糸導く迷宮'),
    ('オーディール・コール', '北大西洋エリア', '久遠の微笑'),
    ('オーディール・コール', 'アラビアエリア', '賞金稼ぎに幾光年'),
    ('オーディール・コール', 'アラビアエリア', '月光採掘場'),
    ('イド', '学校', 'しずかな放課後'),
    ('イド', '学校', 'ななふしぎ調査'),
    ('イド', '学校', 'とつぜんの呼び出し'),
    ('イド', '学校', 'いのこり特訓'),
    ('イド', '西新宿', 'であいの交差点'),
    ('イド', '西新宿', 'たたずむ摩天楼'),
)
prior_in_same_place = (
    '静かな入り江',
    '名もなき霊峰',
    '常夏の休暇',
    '光糸導く迷宮',
    '賞金稼ぎに幾光年',
    'しずかな放課後',
    'であいの交差点',
)
posterior_in_same_place = (
    '隠された島',
    '戦戦恐恐',
    '常夏即売会場',
    '久遠の微笑',
    '月光採掘場',
    'ななふしぎ調査',
    'とつぜんの呼び出し',
    'いのこり特訓',
    'たたずむ摩天楼',
)
ambigious_place = (
    '剣の修練場',
    '弓の修練場',
    '槍の修練場',
    '騎の修練場',
    '術の修練場',
    '殺の修練場',
    '狂の修練場',
    '初級',
    '中級',
    '上級',
    '超級',
    '極級',
    '不夜城',
    '新宿御苑',
)
ambigious_quest = (
    '不夜城',
)


class Detector:
    def __init__(self, freequests: list[dict[str, str]]):
        self.freequest_db: dict[str, str] = _build_db(freequests)
        self.freequest_chapter_db: set[str] = _build_chapter_db(freequests)
        self.freequest_place_index: dict[str, str] = \
            _build_place_index(freequests)
        self.eventquest_cache: dict[str, str] = {}
        self.quest_reverse_index: dict[str, str] = \
            _build_reverse_index(freequests)

        # search_bestmatch_freequest() 内で毎回 replace() するコストを
        # 下げるため、事前に変換しておく。
        self.freequest_db_byspace: dict[str, str] = {}
        for k, v in self.freequest_db.items():
            self.freequest_db_byspace[k.replace('\t', ' ')] = v

    def is_freequest(self, chapter: str, place: str) -> bool:
        """
            渡された chapter, place がフリークエストかどうかを判定する。
            かなり多様なパターンに対応している。実際にどのようなパターンで
            True と判定されるかは freequest_test.py の例を見るとよい。
        """
        first_key = f'{chapter}\t{place}'
        if first_key in self.freequest_db:
            return True

        second_key = f'{place}\t'
        return second_key in self.freequest_db

    def get_quest_id(self, chapter: str, place: str, year: int) -> str:
        key_for_freequest_1st = f'{chapter}\t{place}'
        key_for_freequest_2nd = f'{place}\t'
        key_for_eventquest = f'{chapter}\t{place}\t{year}'

        if key_for_freequest_1st in self.freequest_db:
            return self.freequest_db[key_for_freequest_1st]
        elif key_for_freequest_2nd in self.freequest_db:
            return self.freequest_db[key_for_freequest_2nd]
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

    def search_bestmatch_freequest(self, expr: str) -> str | None:
        for title in self.freequest_db_byspace:
            # 投稿場所は正しいが前後に余計な情報がついているケースを
            # これでカバーできる。
            if title in expr:
                logger.debug('title: %s, expr: %s', title, expr)
                return self.freequest_db_byspace[title]

        logger.debug('cannot find a candidate')
        return None

    def find_freequest(self, expr: str) -> tuple[str, str] | None:
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
        # あいまいな名前は正しい推測ができないことが確実なので、
        # 最初に除外する。
        if expr in ambigious_place or expr in ambigious_quest:
            return None

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
            # place 候補で実際に切ってみて、残り部分が quest name と近いかどうかを見る
            place_candidate = place_candidates[0]
            quest_candidate = expr[len(place_candidate):].strip()
            qid = self.freequest_place_index[place_candidate]
            chapter_place_quest = self.quest_reverse_index[qid]
            _, place, quest = chapter_place_quest.split()
            # expr が場所のみの場合は、以降のチェックは不要。
            # quest name がないのだから類似度判定自体ができない。
            if expr == place:
                return place, quest

            sm = SequenceMatcher(isjunk=None, a=quest_candidate, b=quest)
            quest_radio = sm.ratio()
            logger.debug(
                f"quest_candidate = {quest_candidate}, quest = {quest}, "
                f"ratio = {quest_radio}"
            )
            # フリクエの quest name が書かれていると推測できるならフリクエとして扱う
            if quest_radio > 0.7:
                return place, quest
            return None

        elif len(place_candidates) > 1:
            # 複数マッチはバグ以外では考えにくい
            raise ValueError(
                'matched multiple places or quests '
                f'{place_candidates}: {expr}'
            )
        else:
            return None


def _build_db(freequests: list[dict[str, str]]) -> dict[str, str]:
    d: dict[str, str] = {}

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

            # クエスト名だけの投稿
            # あいまいなクエスト（クエストだけで一意に決まらない）は登録しない
            if quest not in ambigious_quest:
                d[f'{quest}\t'] = qid
                if alt_quest:
                    d[f'{alt_quest}\t'] = qid

        # クエスト名と場所が一致する場合、以降のキー登録は不要。
        # 登録しようとしてもキー重複とみなされ KeyError になる。
        # クエスト名と場所が一致するクエスト: アヴァロン ドーバーハウス
        if place == quest:
            logger.debug(f"skip: place {place} matches quest {quest}")
            continue

        if (chapter, place, quest) not in quests_in_same_place \
                or quest in prior_in_same_place:

            if f'{chapter}\t{place}' in d:
                raise KeyError(
                    f'key "{chapter}<tab>{place}" has already been registered'
                )
            d[f'{chapter}\t{place}'] = qid
            if alt_place:
                d[f'{chapter}\t{alt_place}'] = qid

            # 場所だけの投稿
            # あいまいな場所（場所だけで一意に決まらない）は登録しない
            if place not in ambigious_place:
                if f'{place}\t' in d:
                    raise KeyError(
                        f'key "{place}<tab>" has already been registered'
                    )
                d[f'{place}\t'] = qid
                if alt_place:
                    d[f'{alt_place}\t'] = qid

    # 周回カウンタに登録されているクエスト名が特殊
    d['オルレアン\tティエール(刃物の町)'] = d['オルレアン\tティエール']
    d['セプテム\tゲルマニア(黒い森)'] = d['セプテム\tゲルマニア']

    return d


def _build_reverse_index(
    freequests: Iterable[dict[str, str]]
) -> dict[str, str]:
    d: dict[str, str] = {}

    for fq in freequests:
        qid = fq['id']
        chapter = fq['chapter']
        place = fq['place']
        quest = fq['quest']

        d[qid] = ' '.join([chapter, place, quest])

    return d


def _build_chapter_db(freequests: Iterable[dict[str, str]]) -> set[str]:
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
    freequests: Iterable[dict[str, str]]
) -> dict[str, str]:
    d: dict[str, str] = {}

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
