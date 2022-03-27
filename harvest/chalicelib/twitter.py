from __future__ import annotations

import copy
import json
import re
import unicodedata
from datetime import datetime
from logging import getLogger
from typing import (
    cast, Any, Dict, List, Optional, Sequence, Union,
)

import tweepy  # type: ignore

from . import freequest, settings, storage, timezone

logger = getLogger(__name__)

RE_INDEPENDENT_RUNCOUNT = re.compile(r'[1-9０-９][0-9０-９]*周')
RE_RUNCOUNT = re.compile(r'[0-9０-９]+$')
RE_ITEMTRAIL = re.compile(r'([(（][^(（)）]+[)）])$')
RE_ITEMCOUNT = re.compile(r'^(?P<item>.*[^0-9０-９]+)(?P<count>[0-9０-９]+)$')


class CensoredAccounts:
    def __init__(
        self,
        fileStorage: storage.SupportStorage,
        filepath: str,
    ):
        self.fileStorage = fileStorage
        self.filepath = filepath
        self.accounts: List[str] = []
        text = fileStorage.get_as_text(filepath)
        if text:
            self.accounts = cast(List[str], json.loads(text))

    def save(self) -> None:
        stream = self.fileStorage.get_output_stream(self.filepath)
        js = json.dumps(self.accounts, indent=2)
        stream.write(js.encode('utf-8'))
        self.fileStorage.close_output_stream(stream)

    def exists(self, account: str) -> bool:
        return account in self.accounts

    def add(self, account: str) -> None:
        if self.exists(account):
            return
        self.accounts.append(account)

    def list(self) -> List[str]:
        return copy.deepcopy(self.accounts)


class TweetCopy:
    """
        ツイートから周回報告に必要なデータを取り出したもの。
    """
    def __init__(self, tweet: Optional[Any]):
        if tweet:
            self.tweet_id: int = tweet.id
            self.screen_name: str = tweet.user.screen_name
            self.full_text: str = tweet.full_text
            self.created_at: datetime = tweet.created_at

    def __str__(self) -> str:
        return '{} {} {}'.format(
            self.created_at,
            self.url(),
            self.full_text.split('\n')[0],
        )

    def __repr__(self) -> str:
        return f'<TweetCopy {self.tweet_id} {self.created_at}>'

    def url(self) -> str:
        return 'https://twitter.com/{}/status/{}'.format(
            self.screen_name,
            self.tweet_id,
        )

    def as_dict(self) -> Dict[str, Union[int, str]]:
        return dict(
            id=self.tweet_id,
            screen_name=self.screen_name,
            full_text=self.full_text,
            created_at=self.created_at.isoformat(),
        )

    @property
    def short_text(self):
        s = self.full_text[:25]
        if len(self.full_text) > 25:
            s += '...'
        return s

    @property
    def timestamp(self):
        # tweepy で取得した時刻は UTC かつタイムゾーン情報が付加されていない。
        return self.created_at.replace(tzinfo=timezone.UTC)\
            .astimezone(timezone.Local)

    @staticmethod
    def retrieve(data: Dict[str, Union[int, str]]) -> Optional[TweetCopy]:
        full_text = cast(str, data['full_text'])

        # 復元時にも censored tweets の簡易チェックをする。
        # display name は保全していないので、簡易チェックでは見ない。
        hashtags = [e for e in full_text.split() if e.startswith("#")]
        if not appropriate_tweet('', hashtags):
            logger.warning('cannot retrieve inappropriate tweet: %s', data)
            return None

        tw = TweetCopy(None)
        tw.tweet_id = int(data['id'])
        tw.screen_name = cast(str, data['screen_name'])
        tw.full_text = full_text
        created_at = cast(str, data['created_at'])
        tw.created_at = datetime.fromisoformat(created_at)
        return tw


class ParseErrorTweet:
    """
        TweetCopy とほぼ同じ構造を持つが、エラーメッセージを保持する
        ことができる。
        TweetCopy とメソッドや構造はほぼ同じであるが、用途が違う上、
        また微妙に共通化が難しいためあえて別々に分けている。
    """
    def __init__(
        self,
        tweet: Optional[TweetCopy],
        error_message: Optional[str],
    ):
        if tweet:
            self.tweet_id = tweet.tweet_id
            self.screen_name = tweet.screen_name
            self.full_text = tweet.full_text
            self.created_at = tweet.created_at
        if error_message:
            self.error_message = error_message

    def __str__(self) -> str:
        return '{} {} {} {}'.format(
            self.created_at,
            self.url(),
            self.full_text.split('\n')[0],
            self.error_message,
        )

    def __repr__(self) -> str:
        return f'<ParseErrorTweet {self.tweet_id} {self.created_at}>'

    def url(self) -> str:
        return 'https://twitter.com/{}/status/{}'.format(
            self.screen_name,
            self.tweet_id,
        )

    def as_dict(self) -> Dict[str, Union[int, str]]:
        return dict(
            id=self.tweet_id,
            screen_name=self.screen_name,
            full_text=self.full_text,
            error_message=self.error_message,
            created_at=self.created_at.isoformat(),
        )

    @property
    def timestamp(self):
        # tweepy で取得した時刻にはタイムゾーン情報が付加されていない。
        return self.created_at.replace(tzinfo=timezone.UTC)\
            .astimezone(timezone.Local)

    @property
    def short_text(self):
        s = self.full_text[:25]
        if len(self.full_text) > 25:
            s += '...'
        return s

    @staticmethod
    def retrieve(
        data: Dict[str, Union[int, str]],
    ) -> Optional[ParseErrorTweet]:

        full_text = cast(str, data['full_text'])

        # 復元時にも censored tweets の簡易チェックをする。
        # display name は保全していないので、簡易チェックでは見ない。
        hashtags = [e for e in full_text.split() if e.startswith("#")]
        if not appropriate_tweet('', hashtags):
            logger.warning('cannot retrieve inappropriate tweet: %s', data)
            return None

        tw = ParseErrorTweet(tweet=None, error_message=None)
        tw.tweet_id = int(data['id'])
        tw.screen_name = cast(str, data['screen_name'])
        tw.full_text = full_text
        tw.error_message = cast(str, data['error_message'])
        created_at = cast(str, data['created_at'])
        tw.created_at = datetime.fromisoformat(created_at)
        return tw


def appropriate_tweet(username: str, hashtags: Sequence[str]) -> bool:
    # 特定の NG タグを含むツイートは宣伝目的のツイートとみなし、除外する。
    if len(hashtags) > 1 and any(
        [True for tag in hashtags if tag in settings.NGTags]
    ):
        return False

    # display name が NG ワードを含む場合は宣伝目的アカウントとみなし、除外する。
    if any([True for word in settings.NGWords if word in username]):
        return False

    return True


class Agent:
    def __init__(
        self,
        consumer_key: str,
        consumer_secret: str,
        access_token: str,
        access_token_secret: str,
    ):
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        self.api = tweepy.API(auth)

    def collect(
        self,
        fetch_count: int = 100,
        max_repeat: int = 10,
        since_id: Optional[int] = None,
        censored: Optional[CensoredAccounts] = None,
    ) -> List[TweetCopy]:

        max_id: Optional[int] = None
        objects: List[TweetCopy] = []

        # 周回報告の投稿頻度を考えれば、100件ずつ取得している限り
        # ループすることはほとんどないだろう。
        # 初期データの取得時のみ大きめの値が必要になるかもしれないが、
        # 運用に乗ってしまえばポーリングは15分間隔なので、1回のクエリで
        # 取得できるのは高々5件程度だろう。
        for i in range(max_repeat):
            q = '#FGO周回カウンタ -filter:retweets'
            kwargs = {
                'q': q,
                'count': fetch_count,
                'tweet_mode': 'extended',
            }
            if max_id:
                kwargs['max_id'] = max_id - 1
            if since_id:
                kwargs['since_id'] = since_id

            logger.info('>>> search args: %s', kwargs)
            tweets = self.api.search_tweets(**kwargs)
            logger.info('>>> fetched %s tweets', len(tweets))

            wrapped = []
            for tw in tweets:
                screen_name = tw.user.screen_name
                if censored and censored.exists(screen_name):
                    logger.warning(
                        "censored account's tweet: %s",
                        tw.id,
                    )
                    continue

                display_name = tw.user.name
                hashtags = tw.entities.get('hashtags', '')

                if not appropriate_tweet(display_name, hashtags):
                    logger.warning('inappropriate tweet: %s', tw.id)
                    if not censored:
                        continue

                    censored.add(screen_name)
                    logger.warning(
                        'account %s has been added the censored list',
                        screen_name,
                    )
                    continue

                wrapped.append(TweetCopy(tw))
            objects.extend(wrapped)

            # 取得数が取得可能件数より少ないので、もうフェッチ可能な
            # データはないと判断できる。
            if len(tweets) < fetch_count:
                return objects

            max_id = tweets[-1].id

        # ここまで到達するということは、総取得件数よりもフェッチ可能な
        # データが多く、フェッチできなかったデータがいくらか存在すると
        # いうこと。
        logger.warning('>>> could not retrieve all available data')
        return objects

    def get(self, tweet_id: int) -> Optional[TweetCopy]:
        """
            ツイートIDを指定し、そのツイートを取得する。
        """
        logger.info('>>> get) lookup_statuses: %s', tweet_id)
        tweets = self.api.lookup_statuses(
            [tweet_id],
            include_entities=True,
            tweet_mode='extended',
        )
        logger.info('>>> fetched %s tweets', len(tweets))
        if len(tweets) == 0:
            return None

        tw = tweets[0]
        display_name = tw.user.name
        hashtags = tw.entities.get('hashtags', '')

        if not appropriate_tweet(display_name, hashtags):
            logger.info('inappropriate tweet: %s', tw.id)
            return None
        return TweetCopy(tweets[0])

    def get_multi(self, tweet_id_list: List[int]) -> Dict[int, TweetCopy]:
        """
            ツイートIDを複数指定し、それらのツイートを取得する。
            最大100件。
            結果は辞書 {tweet_id: Tweet} の形式で返す。
        """
        if len(tweet_id_list) > 100:
            raise ValueError('length of tweet_id_list must be lower than 100')

        logger.info('>>> get_multi) lookup_statuses: %s', tweet_id_list)
        tweets = self.api.lookup_statuses(
            tweet_id_list,
            include_entities=True,
            tweet_mode='extended',
        )
        logger.debug(tweets)
        logger.info('>>> fetched %s tweets', len(tweets))
        return {
            tw.id: TweetCopy(tw) for tw in tweets if appropriate_tweet(
                tw.user.name,
                tw.entities.get('hashtags', ''),
            )
        }


class RunReport:
    """
        周回報告レポート
    """
    def __init__(
        self,
        tweet_id: int,
        reporter: str,
        chapter: str,
        place: str,
        runcount: int,
        items: Dict[str, str],
        timestamp: datetime,
    ):
        self.tweet_id = tweet_id
        self.reporter = reporter
        self.chapter = chapter
        self.place = place
        self.runcount = runcount
        self.items = items
        self.timestamp = timestamp

    def __str__(self) -> str:
        return '{} https://twitter.com/{}/status/{} <{}/{}/{}周> {}'.format(
            self.timestamp,
            self.reporter,
            self.tweet_id,
            self.chapter,
            self.place,
            self.runcount,
            self.items,
        )

    def as_dict(self) -> Dict[str, Any]:
        """
            for reporting.SupportDictConversible
        """
        return dict(
            id=self.tweet_id,
            timestamp=self.timestamp,
            reporter=self.reporter,
            chapter=self.chapter,
            place=self.place,
            runcount=self.runcount,
            items=self.items,
            freequest=self.is_freequest,
            quest_id=self.quest_id,
        )

    def get_id(self) -> Any:
        """
            for reporting.SupportDictConversible
        """
        return self.tweet_id

    def equals(self, obj: Any) -> bool:
        """
            for reporting.SupportDictConversible
        """
        if isinstance(obj, dict):
            return self.as_dict() == obj
        if isinstance(obj, RunReport):
            return self.as_dict() == obj.as_dict()
        return False

    @property
    def is_freequest(self) -> bool:
        isfq = freequest.defaultDetector.is_freequest(self.chapter, self.place)
        if isfq:
            return True
        bestmatch = freequest.defaultDetector.search_bestmatch_freequest(
            f'{self.chapter} {self.place}'.strip(),
        )
        if bestmatch:
            return True
        return False

    @property
    def quest_id(self) -> str:
        if not freequest.defaultDetector.is_freequest(
            self.chapter,
            self.place,
        ):
            bestmatch = freequest.defaultDetector.search_bestmatch_freequest(
                f'{self.chapter} {self.place}'.strip(),
            )
            if bestmatch:
                return bestmatch

        return freequest.defaultDetector.get_quest_id(
            self.chapter, self.place, self.timestamp.year,
        )

    @staticmethod
    def retrieve(data: dict[str, Any]) -> RunReport:
        return RunReport(
            tweet_id=int(data["id"]),
            reporter=str(data["reporter"]),
            chapter=str(data["chapter"]),
            place=str(data["place"]),
            runcount=int(data["runcount"]),
            items=cast(dict[str, str], data["items"]),
            timestamp=datetime.fromisoformat(str(data["timestamp"])),
        )


class TweetURLParseError(Exception):
    pass


class TweetParseError(Exception):
    message = ''

    def get_message(self) -> str:
        return self.message


class HeaderNotFoundError(TweetParseError):
    message = 'ヘッダー行が見つかりません。'


class HeaderEndBracketNotFoundError(TweetParseError):
    message = 'ヘッダーの終端文字 "】" が見つかりません。'


class LocationNotFoundError(TweetParseError):
    message = '周回場所を検出できません。'


class RunCountNotFoundError(TweetParseError):
    message = '周回数を検出できません。'


class DuplicatedItemsError(TweetParseError):
    message = '報告されている素材に重複があります。'


class RunCountZeroError(TweetParseError):
    message = '周回数が 0 です。'


def parse_tweet(tweet: TweetCopy) -> RunReport:
    """
        周回報告ツイートを周回報告オブジェクトに変換する。
        変換できない場合 TweetParseError を投げる。
    """
    logger.debug('tweet id: %s', tweet.tweet_id)

    dirtylines = [ln.strip() for ln in tweet.full_text.split('\n')]
    logger.debug('dirtylines: %s', dirtylines)

    header = ''
    header_found = False
    header_linenum = 0
    lines = []
    for i, line in enumerate(dirtylines):
        if '#FGO周回カウンタ' in line:
            # タグを検出したら終了
            break
        if line.find('【') > -1:
            header = line
            header_found = True
            header_linenum = i
            continue
        if not header_found:
            # ヘッダー行を検出するまではどのような行も無視
            continue
        if line == '':
            continue

        # 数値または NaN で終わる行はアイテム行とみなす
        if len(line) > 3 and line[-3:] == 'NaN':
            lines.append(line)
        # 数値の後に (x4) のような付帯情報がつくことがある。これを無視する
        _line = RE_ITEMTRAIL.sub('', line)
        if _line and _line[-1].isdigit():
            lines.append(line)

        # 【周回場所】
        # 100周
        # のように2行に割れているヘッダーを拾う
        if header_found \
            and i == header_linenum + 1 \
                and RE_INDEPENDENT_RUNCOUNT.match(line):
            header += line

    if not header_found:
        raise HeaderNotFoundError('header not found')

    logger.debug('header: %s', header)

    loc_start_pos = header.find('【')
    loc_end_pos = header.find('】')
    if loc_end_pos == -1:
        raise HeaderEndBracketNotFoundError(
            f'symbol "】" not found in header: {header}'
        )
    location = header[loc_start_pos + 1:loc_end_pos].strip()
    logger.debug('location: %s', location)

    # 全角スペースや全角カッコなどはここで正規化されて半角になる
    normalized_location = unicodedata.normalize('NFKC', location)
    logger.debug('normalized location: %s', normalized_location)

    if ' ' in normalized_location:
        location_tokens: Sequence[str] = normalized_location.split(' ')
    else:
        # chapter と place の間にスペースなし
        # フリクエの場所やクエスト名だけで投稿している可能性があるため、
        # その可能性を探る。
        candidate = freequest.defaultDetector.find_freequest(
            normalized_location)

        if candidate:
            location_tokens = candidate

        else:
            # 該当するフリクエが見つからなかった。
            # place が空文字列の location として扱う。
            location_tokens = [normalized_location, '']

    if len(location_tokens) == 2:
        chapter = location_tokens[0]
        place = location_tokens[1]
    elif location_tokens[-1].startswith('(') \
            and location_tokens[-1].endswith(')'):
        # 第二階層 極光の間 (裏) のようにカッコ書き部分の前に
        # スペースが混入するケースの救済措置
        chapter = ' '.join(location_tokens[:-2])
        # カッコ () 直前のスペースは除去
        place = ''.join(location_tokens[-2:])
    else:
        chapter = ' '.join(location_tokens[:-1])
        place = location_tokens[-1]

    runcount_pos = header.rfind('周')
    if runcount_pos == -1:
        raise RunCountNotFoundError(f'symbol "周" not found: {header}')
    if loc_end_pos + 1 >= runcount_pos:
        raise RunCountNotFoundError(f'could not extract runcount: {header}')

    # 【下総国 里】追加100周
    # のように "】" の直後が数値でないケースも考慮する。
    mo = RE_RUNCOUNT.search(header[loc_end_pos+1:runcount_pos])
    if not mo:
        raise RunCountNotFoundError(f'could not extract runcount: {header}')

    runcount = int(mo.group())
    if runcount == 0:
        raise RunCountZeroError()

    logger.debug('chapter: %s', chapter)
    logger.debug('place: %s', place)
    logger.debug('runcount: %s', runcount)

    items_with_counts: List[str] = []
    for line in lines:
        tokens = line.strip().split('-')
        items_with_counts.extend(tokens)

    logger.debug('items_with_counts: %s', items_with_counts)
    item_dict: Dict[str, str] = {}

    for token in items_with_counts:
        if token == '':
            continue

        if token.endswith('NaN'):
            item = token[:-3]
            item_dict[item] = 'NaN'
            logger.debug(f'{item}: NaN')
            continue

        # 末尾の () 表記はカットし、なかったものとして扱う。
        # たとえば "カード12(+4)" は {"カード": 12} と解釈する。
        _token = RE_ITEMTRAIL.sub('', token)
        mo = RE_ITEMCOUNT.match(_token)
        if not mo:
            # 個数が取得できない場合、報告情報ではないとみなして無視する
            logger.debug('token %s is not an item', token)
            continue
        d = mo.groupdict()
        item_dict[d['item']] = d['count']

    logger.debug('item_dict: %s', item_dict)

    return RunReport(
        tweet_id=tweet.tweet_id,
        reporter=tweet.screen_name,
        chapter=chapter,
        place=place,
        runcount=runcount,
        items=item_dict,
        timestamp=tweet.timestamp,
    )


class StatusTweetURLParser:
    def __init__(self):
        expr = (
            r"^https://twitter.com/"
            r"(?P<user>[A-Za-z0-9_]{2,15})/status/(?P<tweet_id>[0-9]+)$"
        )
        self.pattern = re.compile(expr)

    def parse(self, url: str) -> tuple[str, int]:
        m = self.pattern.match(url)
        if not m:
            raise TweetURLParseError(f"url {url} does not match the pattern")
        return m.group("user"), int(m.group("tweet_id"))

    def parse_multi(self, urls: list[str]) -> dict[str, list[int]]:
        d: dict[str, list[int]] = {}

        for url in urls:
            user, tweet_id = self.parse(url)
            if user not in d:
                d[user] = [tweet_id]
            else:
                d[user].append(tweet_id)

        return d
