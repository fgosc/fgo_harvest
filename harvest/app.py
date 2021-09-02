import io
import os
import random
import time
from datetime import datetime
from logging import getLogger
from typing import List, Tuple

import boto3  # type: ignore
import botocore.exceptions  # type: ignore
from chalice import Chalice, Rate  # type: ignore

from chalicelib import settings
from chalicelib import static
from chalicelib import storage
from chalicelib import twitter
from chalicelib import recording


logger = getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))

app = Chalice(app_name='harvest')
cloudfront = boto3.client('cloudfront')


def setup_twitter_agent():
    return twitter.Agent(
        consumer_key=settings.TwitterConsumerKey,
        consumer_secret=settings.TwitterConsumerSecret,
        access_token=settings.TwitterAccessToken,
        access_token_secret=settings.TwitterAccessTokenSecret,
    )


def render_contents(app, tweets, ignore_original=False):
    recorders: List[Tuple[recording.Recorder, bool]] = []

    outdir_bydate = f'{settings.ProcessorOutputDir}/date'
    recorder_bydate = recording.Recorder(
        partitioningRule=recording.PartitioningRuleByDate(),
        fileStorage=storage.AmazonS3Storage(settings.S3Bucket),
        basedir=outdir_bydate,
        formats=(
            recording.OutputFormat.JSON,
            recording.OutputFormat.DATEHTML,
        ),
    )
    recorders.append((recorder_bydate, False))

    outdir_byuser = f'{settings.ProcessorOutputDir}/user'
    recorder_byuser = recording.Recorder(
        partitioningRule=recording.PartitioningRuleByUser(),
        fileStorage=storage.AmazonS3Storage(settings.S3Bucket),
        basedir=outdir_byuser,
        formats=(
            recording.OutputFormat.JSON,
            recording.OutputFormat.USERHTML,
        ),
    )
    recorders.append((recorder_byuser, False))

    outdir_byquest = f'{settings.ProcessorOutputDir}/quest'
    recorder_byquest = recording.Recorder(
        partitioningRule=recording.PartitioningRuleByQuest(),
        fileStorage=storage.AmazonS3Storage(settings.S3Bucket),
        basedir=outdir_byquest,
        formats=(
            recording.OutputFormat.JSON,
            recording.OutputFormat.QUESTHTML,
        ),
    )
    recorders.append((recorder_byquest, False))

    # 出力先は outdir_byuser
    recorder_byuserlist = recording.Recorder(
        partitioningRule=recording.PartitioningRuleByUserList(),
        fileStorage=storage.AmazonS3Storage(settings.S3Bucket),
        basedir=outdir_byuser,
        formats=(
            recording.OutputFormat.JSON,
            recording.OutputFormat.USERLISTHTML,
        )
    )
    recorders.append((recorder_byuserlist, False))

    # outdir_byquest
    recorder_byquestlist = recording.Recorder(
        # この partitioningRule は rebuild フラグを個別に渡す必要あり
        partitioningRule=recording.PartitioningRuleByQuestList(
            ignore_original,
        ),
        fileStorage=storage.AmazonS3Storage(settings.S3Bucket),
        basedir=outdir_byquest,
        formats=(
            recording.OutputFormat.JSON,
            recording.OutputFormat.QUESTLISTHTML,
        )
    )
    # quest list だけはリストの増減がない場合でも数値の countup を
    # 再描画する必要があるので強制上書きが必要
    recorders.append((recorder_byquestlist, True))

    outdir_error = f'{settings.ProcessorOutputDir}/errors'
    error_recorder = recording.ErrorPageRecorder(
        fileStorage=storage.AmazonS3Storage(settings.S3Bucket),
        basedir=outdir_error,
        key='error',
        formats=(
            recording.ErrorOutputFormat.JSON,
            recording.ErrorOutputFormat.HTML,
        )
    )

    app.log.info('starting to parse tweets...')
    for tweet in tweets:
        try:
            report = twitter.parse_tweet(tweet)
            for recorder, _ in recorders:
                recorder.add(report)

        except twitter.TweetParseError as e:
            app.log.error(e)
            app.log.error(tweet)
            etw = twitter.ParseErrorTweet(
                tweet=tweet,
                error_message=e.get_message(),
            )
            error_recorder.add_error(etw)

    app.log.info('starting to render pages...')
    for recorder, force_save in recorders:
        if recorder.count():
            recorder.save(force=force_save, ignore_original=ignore_original)

    error_recorder.save(ignore_original=ignore_original)

    # 出力先は outdir_bydate
    # 少なくとも bydate の HTML レンダリングが完了してからでないと実行できない。
    # したがってこの位置で実行する。
    latestDatePageBuilder = recording.LatestDatePageBuilder(
        fileStorage=storage.AmazonS3Storage(settings.S3Bucket),
        basedir=outdir_bydate,
    )
    latestDatePageBuilder.build()
    app.log.info('done')


@app.schedule(Rate(30, unit=Rate.MINUTES))
def collect_tweets(event):
    agent = setup_twitter_agent()

    tweet_repository = recording.TweetRepository(
        fileStorage=storage.AmazonS3Storage(settings.S3Bucket),
        basedir=settings.TweetStorageDir,
    )

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(settings.S3Bucket)
    latest_tweet_id_file_key = \
        f'{settings.SettingsDir}/{settings.LatestTweetIDFile}'
    bio = io.BytesIO()
    app.log.info('checking latest_tweet_id file: %s', latest_tweet_id_file_key)
    try:
        bucket.download_fileobj(latest_tweet_id_file_key, bio)
        latest_tweet_id_str = bio.getvalue().decode('utf-8')
    except botocore.exceptions.ClientError as e:
        app.log.warning(e)
        latest_tweet_id_str = ''

    censored_accounts = twitter.CensoredAccounts(
        fileStorage=storage.AmazonS3Storage(settings.S3Bucket),
        filepath=f'{settings.SettingsDir}/{settings.CensoredAccountsFile}',
    )

    try:
        since_id = int(latest_tweet_id_str)
    except ValueError:
        since_id = None
    app.log.info(f'since_id: {since_id}')

    tweets = agent.collect(
        max_repeat=5,
        since_id=since_id,
        censored=censored_accounts,
    )
    app.log.info('collected %s tweets', len(tweets))

    if len(tweets) == 0:
        return

    tweet_log_file = '{}.json'.format(datetime.now().strftime('%Y%m%d_%H%M%S'))
    app.log.info('tweet_log: %s', tweet_log_file)
    tweet_repository.put(tweet_log_file, tweets)

    render_contents(app, tweets)

    latest_tweet = tweets[0]
    app.log.info('saving the latest tweet id: %s', latest_tweet.tweet_id)
    latest_tweet_id_bytes = str(latest_tweet.tweet_id).encode('utf-8')
    latest_tweet_id_stream = io.BytesIO(latest_tweet_id_bytes)
    bucket.upload_fileobj(latest_tweet_id_stream, latest_tweet_id_file_key)

    censored_accounts.save()


@app.lambda_function()
def rebuild_outputs(event, context):
    tweet_repository = recording.TweetRepository(
        fileStorage=storage.AmazonS3Storage(settings.S3Bucket),
        basedir=settings.TweetStorageDir,
    )

    censored_accounts = twitter.CensoredAccounts(
        fileStorage=storage.AmazonS3Storage(settings.S3Bucket),
        filepath=f'{settings.SettingsDir}/{settings.CensoredAccountsFile}',
    )

    tweets = tweet_repository.readall(set(censored_accounts.list()))
    app.log.info('retrieved %s tweets', len(tweets))

    render_contents(app, tweets, ignore_original=True)
    app.log.info('finished rebuilding outputs')


@app.lambda_function()
def build_static_contents(event, context):
    renderer = static.StaticPagesRenderer(
        fileStorage=storage.AmazonS3Storage(settings.S3Bucket),
        basedir=f'{settings.ProcessorOutputDir}/static',
    )
    renderer.render_all()
    app.log.info('finished building static contents')


def generate_caller_reference():
    # unique であればよい
    t = time.time()
    r = random.randint(0, 1048576)
    return f'harvest-{t}-{r}'


# @app.s3_event() を使うと複数のトリガーを設定できない。
# @app.lambda_function() で Lambda を定義して S3 トリガーは手動で設定する。
@app.lambda_function()
def invalidate_cloudfront_cache(event, context):
    logger.info(event)

    object_size = event['Records'][0]['s3']['object']['size']
    item = '/' + event['Records'][0]['s3']['object']['key']

    if not item.endswith('/index.html'):
        logger.info('ignore: %s', item)
        return

    # ad hoc な方法ではあるが、現状これ以上良い解が見つかっていない
    if object_size < 800:
        logger.info('ignore cache invalidation: probably blank html (size: %s)', object_size)
        return

    items = []
    # .../index.html を invalidate する代わりに
    # .../ を invalidate する。
    items.append(item[:item.rfind('/')+1])

    logger.info('cache invalidation: %s', items)

    cloudfront.create_invalidation(
        DistributionId=settings.CloudfrontDistributionId,
        InvalidationBatch={
            'Paths': {
                'Quantity': len(items),
                'Items': items,
            },
            'CallerReference': generate_caller_reference(),
        }
    )
