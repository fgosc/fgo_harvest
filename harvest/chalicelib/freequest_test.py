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
    ('新宿二丁目', 'レインボータウン', True),
    # クエスト名だけ
    ('西七条', '', True),
    ('静かな入り江', '', True),
    ('隠された島', '', True),
    # 場所だけ
    ('シャーロット', '', True),
    ('裏山', '', True),
    # 中点 (・) 欠け
    ('アナスタシア', 'ヤガスィチョーフカ', True),
    # 中点 (・) 欠け、かつ場所だけ
    ('ヤガスィチョーフカ', '', True),
    # フリクエではない
    ('本戦', 'ガーデン級', False),
    # バビロニア ウル と誤認しないこと
    ('ウルトラヘビー級', '', False),
]


@pytest.mark.parametrize('chapter,place,expected', testdata_is_freequest)
def test_get_freequest(chapter, place, expected):
    assert freequest.defaultDetector.is_freequest(chapter, place) == expected


testdata_find_freequest = [
    ('バビロニア高原', ('バビロニア', '高原')),
    ('シャーロットゴールドラッシュ', ('シャーロット', 'ゴールドラッシュ')),
    ('新宿二丁目レインボータウン', ('新宿二丁目', 'レインボータウン')),
    # バビロニア ウル と誤認しないこと
    ('ウルトラヘビー級', None),
]


@pytest.mark.parametrize('candidate,expected', testdata_find_freequest)
def test_find_freequest(candidate, expected):
    assert freequest.defaultDetector.find_freequest(candidate) == expected


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
    ('自動防衛装置・ハント', '典位+級', 'hc80ypM4YIKh'),
    ('新宿二丁目', 'レインボータウン', '15a10'),
    ('スプラッシュレイク', '', '_NDE_UDOVE_P'),
]


@pytest.mark.parametrize('chapter,place,expected', testdata_get_quest_id)
def test_get_quest_id(chapter, place, expected):
    assert freequest.defaultDetector.get_quest_id(
        chapter, place, 2020) == expected


testdata_search_bestmatch_freequest = [
    ('大江山 鬼の住み処', '20g12'),
    ('地獄界曼荼羅 平安京 三条三坊 鬼の遊び場', '20g13'),
]


@pytest.mark.parametrize(
    'candidate,expected',
    testdata_search_bestmatch_freequest,
)
def test_search_bestmatch_freequest(candidate, expected):
    assert freequest.defaultDetector.\
        search_bestmatch_freequest(candidate) == expected
