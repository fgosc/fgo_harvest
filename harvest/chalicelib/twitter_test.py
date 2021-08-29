from collections import namedtuple
from datetime import datetime
from unittest import mock

import pytest

from . import timezone
from . import twitter

MockTweet = namedtuple('MockTweet', ['id', 'user', 'full_text', 'created_at'])
MockUser = namedtuple('MockUser', ['screen_name'])


def test_parse_tweet1():
    text = """【シャーロット ゴールドラッシュ】1000周
塵643-証487
弓輝106-槍輝105-術輝68
槍モ57-狂モ138
槍ピNaN-狂ピ288
123ダイス5-あ1いNaN
カード(x3)123-カード(x5)456-カード78(x4)
QP(+194千)50-QP(+195千)58
#FGO周回カウンタ http://aoshirobo.net/fatego/rc/
"""
    user = MockUser('testuser')
    original_tweet = MockTweet(
        1234567890,
        user,
        text,
        datetime(2020, 1, 2, 3, 4, 5),
    )

    tw = twitter.TweetCopy(original_tweet)

    parsed = twitter.parse_tweet(tw)
    assert parsed.tweet_id == 1234567890
    assert parsed.reporter == 'testuser'
    assert parsed.chapter == 'シャーロット'
    assert parsed.place == 'ゴールドラッシュ'
    assert parsed.is_freequest is True
    tz = timezone.Local
    assert parsed.timestamp == datetime(2020, 1, 2, 12, 4, 5, tzinfo=tz)
    assert parsed.items == {
        '塵': '643',
        '証': '487',
        '弓輝': '106',
        '槍輝': '105',
        '術輝': '68',
        '槍モ': '57',
        '狂モ': '138',
        '槍ピ': 'NaN',
        '狂ピ': '288',
        '123ダイス': '5',
        'あ1い': 'NaN',
        'カード(x3)': '123',
        'カード(x5)': '456',
        'カード': '78',
        'QP(+194千)': '50',
        'QP(+195千)': '58',
    }


def test_parse_tweet2():
    text = """【上級】100周
礼装0
結氷16-蛇玉13
弓魔9-弓輝12
弓ピ18
バンテージ2470-バナナ1737-ガム753
#FGO周回カウンタ https://aoshirobo.net/fatego/rc/
 https://fgosccalc.appspot.com
増加礼装なし。
なんの成果もありませんでした！"""

    user = MockUser('testuser')
    original_tweet = MockTweet(
        1234567890,
        user,
        text,
        datetime(2020, 1, 2, 3, 4, 5),
    )

    tw = twitter.TweetCopy(original_tweet)

    parsed = twitter.parse_tweet(tw)
    assert parsed.tweet_id == 1234567890
    assert parsed.reporter == 'testuser'
    assert parsed.chapter == '上級'
    assert parsed.place == ''
    assert parsed.is_freequest is False
    tz = timezone.Local
    assert parsed.timestamp == datetime(2020, 1, 2, 12, 4, 5, tzinfo=tz)
    assert parsed.items == {
        '礼装': '0',
        '結氷': '16',
        '蛇玉': '13',
        '弓魔': '9',
        '弓輝': '12',
        '弓ピ': '18',
        'バンテージ': '2470',
        'バナナ': '1737',
        'ガム': '753',
    }


runreport0 = """【大江山 鬼の住み処】100周
鬼灯11-狂骨38-狂の秘石3-狂の輝石33-叡智の猛火4
#FGO周回カウンタ http://aoshirobo.net/fatego/rc/
"""

runreport1 = """【地獄界曼荼羅　平安京　三条三坊　鬼の遊び場】340周
鬼炎鬼灯42-糸玉75
#FGO周回カウンタ http://aoshirobo.net/fatego/rc/
"""

runreport2 = """【ドレッドノート級】50周
礼装2
ランプ11-塵18-証19
術魔5-狂魔5-術輝7
術モ15
鋭歯1305-ヒレ1340-ウニ2751
#FGO周回カウンタ https://aoshirobo.net/fatego/rc/
"""

testdata_runreport = [
    (runreport0, '大江山', '鬼の住み処', True, '20g12'),
    (runreport1, '地獄界曼荼羅 平安京 三条三坊', '鬼の遊び場', True, '20g13'),
    (runreport2, 'ドレッドノート級', '', False, 'M0IrmeBMeC6A'),
]


@pytest.mark.parametrize(
    'text,chapter,place,is_freequest,qid',
    testdata_runreport,
)
def test_runreport(
    text: str,
    chapter: str,
    place: str,
    is_freequest: bool,
    qid: str,
):
    user = MockUser('testuser')
    original_tweet = MockTweet(
        1234567890,
        user,
        text,
        datetime(2020, 1, 2, 3, 4, 5),
    )

    tw = twitter.TweetCopy(original_tweet)
    parsed = twitter.parse_tweet(tw)
    assert parsed.chapter == chapter
    assert parsed.place == place
    assert parsed.is_freequest == is_freequest
    assert parsed.quest_id == qid


testdata_appropriate = [
    ('', ('#OKTagA', "#FGO周回カウンタ"), True),
    ('', ('#NGTagA', '#FGO周回カウンタ'), False),
    ('', ('#NGTagB', '#FGO周回カウンタ'), False),
    ('NGWordA', ('#OKTagA', "#FGO周回カウンタ"), False),
    ('NGWordB', ('#OKTagA', '#FGO周回カウンタ'), False),
    ('NGWordA', ('#NGTagB', '#FGO周回カウンタ'), False),
]


@pytest.mark.parametrize(
    'username,hashtags,expected',
    testdata_appropriate,
)
@mock.patch('chalicelib.settings.NGWords', new=('NGWordA', 'NGWordB'))
@mock.patch('chalicelib.settings.NGTags', new=('#NGTagA', '#NGTagB'))
def test_appropriate_tweet(username, hashtags, expected):
    assert twitter.appropriate_tweet(username, hashtags) == expected
