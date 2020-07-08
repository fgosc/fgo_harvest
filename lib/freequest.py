import json
import os
from typing import Dict, List, Set


class Detector:
    def __init__(self, freequests: List[Dict[str, str]]):
        self.fq_set: Set[str] = _build_set(freequests)

    def is_freequest(self, chapter: str, place: str) -> bool:
        key = f'{chapter}\t{place}'
        return key in self.fq_set


def _build_set(freequests: List[Dict[str, str]]) -> Set[str]:
    d = set()

    for fq in freequests:
        chapter = fq['chapter']
        place = fq['place']
        quest = fq['quest']

        # 同じ chapter, place で quest が 2 つ存在する特殊なケース
        if (chapter, place, quest) == ('下総国', '裏山', '戦戦恐恐'):
            # 下総国 戦戦恐恐
            d.add(f'{chapter}\t{quest}')
            # 下総国 裏山 戦戦恐恐
            d.add(f'{chapter} {place}\t{quest}')
            continue
        if (chapter, place, quest) == ('下総国', '裏山', '名もなき霊峰'):
            # 下総国 名もなき霊峰
            d.add(f'{chapter}\t{quest}')
            # 下総国 裏山 名もなき霊峰
            d.add(f'{chapter} {place}\t{quest}')
            continue

        d.add(f'{chapter}\t{place}')

        # 歴史的理由により北米のみ【chapter place】ではなく
        # 【place quest】形式で投稿されることが多い。
        if chapter == '北米':
            d.add(f'{place}\t{quest}')

    return d


with open(os.path.join(os.path.dirname(__file__), 'freequest.json')) as fp:
    defaultDetector = Detector(json.load(fp))
