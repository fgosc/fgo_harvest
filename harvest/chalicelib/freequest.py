import json
import os
from base64 import urlsafe_b64encode
from hashlib import md5
from typing import Dict, List


class Detector:
    def __init__(self, freequests: List[Dict[str, str]]):
        self.freequest_db: Dict[str, str] = _build_db(freequests)
        self.eventquest_cache: Dict[str, str] = {}
        self.quest_reverse_index: Dict[str, str] = \
            _build_reverse_index(freequests)

    def is_freequest(self, chapter: str, place: str) -> bool:
        key = f'{chapter}\t{place}'
        return key in self.freequest_db

    def get_quest_id(self, chapter: str, place: str) -> str:
        key = f'{chapter}\t{place}'

        if key in self.freequest_db:
            return self.freequest_db[key]

        elif key in self.eventquest_cache:
            return self.eventquest_cache[key]

        b64digest = urlsafe_b64encode(md5(key.encode('utf-8')).digest())
        qid = b64digest[:8].decode('utf-8')
        self.eventquest_cache[key] = qid
        self.quest_reverse_index[qid] = key.replace('\t', ' ')
        return qid

    def get_quest_name(self, qid: str) -> str:
        return self.quest_reverse_index[qid]


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

        # 歴史的理由により北米のみ【chapter place】ではなく
        # 【place quest】形式で投稿されることが多い。
        if chapter == '北米':
            d[(f'{place}\t{quest}')] = qid

        if (chapter, place, quest) in quests_in_same_place:
            # 通常は chapter, place をキーにするが、これらのケースでは
            # chapter, place 以外のパターンでも判定できるようにしておく。
            d[f'{chapter}\t{quest}'] = qid
            d[f'{chapter} {place}\t{quest}'] = qid
            # すでに chapter/place のキーが登録済みなら、
            # 上書き登録しない。
            if f'{chapter}\t{place}' in d:
                continue

        if f'{chapter}\t{place}' in d:
            raise KeyError(f'key "{chapter} {place}" has already registered')
        d[f'{chapter}\t{place}'] = qid

        # 修練場のようにクエスト名がないパターンがあるので
        # 存在チェックが必要。
        if quest:
            if f'{chapter}\t{quest}' in d:
                # これらは登録済みで正しい
                if (chapter, place, quest) in quests_in_same_place:
                    continue
                # それ以外が二重に登録されるのはDBの設定ミス
                else:
                    raise KeyError(
                        f'key "{chapter} {quest}" has already registered'
                    )
            d[f'{chapter}\t{quest}'] = qid

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


with open(os.path.join(os.path.dirname(__file__), 'freequest.json')) as fp:
    defaultDetector = Detector(json.load(fp))
