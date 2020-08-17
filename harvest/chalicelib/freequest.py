import json
import os
from base64 import urlsafe_b64encode
from hashlib import md5
from typing import Dict, List, Set


class Detector:
    def __init__(self, freequests: List[Dict[str, str]]):
        self.freequest_db: Dict[str, str] = _build_db(freequests)
        self.freequest_chapter_db: Set[str] = _build_chapter_db(freequests)
        self.eventquest_cache: Dict[str, str] = {}
        self.quest_reverse_index: Dict[str, str] = \
            _build_reverse_index(freequests)

    def is_freequest(self, chapter: str, place: str) -> bool:
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

    def match_freequest_chapter(self, expr) -> str:
        """
            与えられた文字列がフリークエストの章名で始まるかどうかを調べる。
            もし何らかの章名で始まっている場合は、その章名を返す。
            ただし北米のみ例外的に地名でもマッチする。
            いずれの章ともマッチしなければ、空文字列 '' を返す。
        """
        for chapter in self.freequest_chapter_db:
            if expr.startswith(chapter):
                return chapter
        return ''


def _build_db(freequests: List[Dict[str, str]]) -> Dict[str, str]:
    # 同じ chapter, place で quest が 2 つ存在する特殊なケース。
    quests_in_same_place = (
        ('オケアノス', '群島', '静かな入り江'),
        ('オケアノス', '群島', '隠された島'),
        ('下総国', '裏山', '名もなき霊峰'),
        ('下総国', '裏山', '戦戦恐恐'),
    )

    d = {}

    for fq in freequests:
        qid = fq['id']
        chapter = fq['chapter']
        place = fq['place']
        quest = fq['quest']

        # 修練場のようにクエスト名がないパターンがあるので
        # 存在チェックが必要。
        if quest:
            d[f'{chapter}\t{quest}'] = qid
            d[f'{chapter} {place}\t{quest}'] = qid
            d[(f'{place}\t{quest}')] = qid

        if (chapter, place, quest) in quests_in_same_place:
            # quests_in_same_place のクエストたちに限っては
            # キー衝突回避のため、すでに chapter/place のキー
            # が登録済みなら上書き登録しない。
            if f'{chapter}\t{place}' in d:
                continue

        if f'{chapter}\t{place}' in d:
            raise KeyError(
                f'key "{chapter} {place}" has already been registered'
            )
        d[f'{chapter}\t{place}'] = qid

    # 周回カウンタに登録されているクエスト名が特殊
    d['オルレアン\tティエール(刃物の町)'] = d['オルレアン\tティエール']
    d['セプテム\tゲルマニア(黒い森)'] = d['セプテム\tゲルマニア']

    return d


def _build_reverse_index(freequests: List[Dict[str, str]]) -> Dict[str, str]:
    d = {}

    for fq in freequests:
        qid = fq['id']
        chapter = fq['chapter']
        place = fq['place']
        quest = fq['quest']

        d[qid] = ' '.join([chapter, place, quest])

    return d


def _build_chapter_db(freequests: List[Dict[str, str]]) -> Set[str]:
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


with open(os.path.join(os.path.dirname(__file__), 'freequest.json')) as fp:
    defaultDetector = Detector(json.load(fp))
