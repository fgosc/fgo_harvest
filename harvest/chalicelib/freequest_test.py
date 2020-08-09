import pytest  # type: ignore

from . import freequest


testdata_is_freequest = [
    ('オケアノス', '群島', True),
    ('オケアノス', '静かな入り江', True),
    ('オケアノス', '隠された島', True),
    ('オケアノス 群島', '隠された島', True),
    ('下総国', '裏山', True),
    ('下総国', '名もなき霊峰', True),
    ('下総国', '戦戦恐恐', True),
    ('下総国 裏山', '戦戦恐恐', True),
    ('下総国 荒川の原', '古戦場', True),
    ('オルレアン', 'ティエール(刃物の町)', True),
    ('セプテム', 'ゲルマニア(黒い森)', True),
]


@pytest.mark.parametrize('chapter,place,expected', testdata_is_freequest)
def test_get_freequest(chapter, place, expected):
    assert freequest.defaultDetector.is_freequest(chapter, place) == expected


testdata_get_quest_id = [
    ('オケアノス', '群島', '10d10'),
    ('オケアノス', '静かな入り江', '10d10'),
    ('オケアノス', '隠された島', '10d11'),
    ('オケアノス 群島', '隠された島', '10d11'),
    ('下総国', '裏山', '15c07'),
    ('下総国', '名もなき霊峰', '15c07'),
    ('下総国', '戦戦恐恐', '15c09'),
    ('下総国 裏山', '戦戦恐恐', '15c09'),
    ('下総国 荒川の原', '古戦場', '15c08'),
    ('オルレアン', 'ティエール(刃物の町)', '10b07'),
    ('セプテム', 'ゲルマニア(黒い森)', '10c06'),
    ('自動防衛装置・ハント', '典位+級', 'hc80ypM4YIKh')
]


@pytest.mark.parametrize('chapter,place,expected', testdata_get_quest_id)
def test_get_quest_id(chapter, place, expected):
    assert freequest.defaultDetector.get_quest_id(
        chapter, place, 2020) == expected
