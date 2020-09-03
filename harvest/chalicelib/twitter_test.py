from collections import namedtuple
from datetime import datetime

from . import timezone
from . import twitter

MockTweet = namedtuple('MockTweet', ['id', 'user', 'full_text', 'created_at'])
MockUser = namedtuple('MockUser', ['screen_name'])


def test_parse_tweet():
    text = """【シャーロット ゴールドラッシュ】1000周
塵643-証487
弓輝106-槍輝105-術輝68
槍モ57-狂モ138
槍ピNaN-狂ピ288
123ダイス5-あ1いNaN
カード(x3)123-カード(x5)456
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
        'QP(+194千)': '50',
        'QP(+195千)': '58',
    }
