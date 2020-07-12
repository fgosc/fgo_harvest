import io
from datetime import datetime

import boto3  # type: ignore
import botocore.exceptions  # type: ignore
from chalice import Chalice, Rate  # type: ignore

from chalicelib import settings, storage, twitter, recording

app = Chalice(app_name='harvest')


def setup_twitter_agent():
    return twitter.Agent(
        consumer_key=settings.TwitterConsumerKey,
        consumer_secret=settings.TwitterConsumerSecret,
        access_token=settings.TwitterAccessToken,
        access_token_secret=settings.TwitterAccessTokenSecret,
    )


def render_contents(app, tweets, ignore_original=False):
    recorders = []

    outdir_bydate = f'{settings.ProcessorOutputDir}/date'
    recorder_bydate = recording.Recorder(
        partitioningRule=recording.PartitioningRuleByDate(),
        fileStorage=storage.AmazonS3Storage(settings.S3Bucket),
        basedir=outdir_bydate,
        formats=(recording.OutputFormat.JSON, recording.OutputFormat.DATEHTML),
    )
    recorders.append(recorder_bydate)

    outdir_byuser = f'{settings.ProcessorOutputDir}/user'
    recorder_byuser = recording.Recorder(
        partitioningRule=recording.PartitioningRuleByUser(),
        fileStorage=storage.AmazonS3Storage(settings.S3Bucket),
        basedir=outdir_byuser,
        formats=(recording.OutputFormat.JSON, recording.OutputFormat.USERHTML),
    )
    recorders.append(recorder_byuser)

    outdir_byquest = f'{settings.ProcessorOutputDir}/quest'
    recorder_byquest = recording.Recorder(
        partitioningRule=recording.PartitioningRuleByQuest(),
        fileStorage=storage.AmazonS3Storage(settings.S3Bucket),
        basedir=outdir_byquest,
        formats=(recording.OutputFormat.JSON, recording.OutputFormat.QUESTHTML),
    )
    recorders.append(recorder_byquest)

    for tweet in tweets:
        try:
            report = twitter.parse_tweet(tweet)
            for recorder in recorders:
                recorder.add(report)

        except twitter.ParseError as e:
            app.log.error(e)
            # TODO エラーになったツイートもHTMLには入れたい...
            app.log.error(tweet)

    for recorder in recorders:
        recorder.save(ignore_original=ignore_original)


@app.schedule(Rate(15, unit=Rate.MINUTES))
def collect_tweets(event):
    agent = setup_twitter_agent()

    tweet_storage = recording.AmazonS3TweetStorage(
        bucket=settings.S3Bucket,
        output_dir=settings.TweetStorageDir,
    )
    
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(settings.S3Bucket)
    latest_tweet_id_file_key = f'{settings.SettingsDir}/{settings.LatestTweetIDFile}'
    bio = io.BytesIO()
    app.log.info('checking latest_tweet_id file: %s', latest_tweet_id_file_key)
    try:
        bucket.download_fileobj(latest_tweet_id_file_key, bio)
        latest_tweet_id_str = bio.getvalue().decode('utf-8')
    except botocore.exceptions.ClientError as e:
        app.log.warning(e)
        latest_tweet_id_str = ''
    
    try:
        since_id = int(latest_tweet_id_str)
    except ValueError:
        since_id = None
    app.log.info(f'since_id: {since_id}')

    tweets = agent.collect(
        since_id=since_id,
        max_repeat=5,
        exclude_accounts=settings.ExcludeAccounts,
    )
    app.log.info(f'collected %s tweets', len(tweets))

    tweet_log_file = '{}.json'.format(datetime.now().strftime('%Y%m%d_%H%M%S'))
    app.log.info(f'tweet_log: {tweet_log_file}')
    tweet_storage.put(tweet_log_file, tweets)

    render_contents(app, tweets)

    if len(tweets) == 0:
        return

    latest_tweet = tweets[0]
    app.log.info(f'saving the latest tweet id: {latest_tweet.tweet_id}')
    latest_tweet_id_stream = io.BytesIO(str(latest_tweet.tweet_id).encode('utf-8'))
    bucket.upload_fileobj(latest_tweet_id_stream, latest_tweet_id_file_key)


@app.lambda_function()
def rebuild_outputs(event, context):
    tweet_storage = recording.AmazonS3TweetStorage(
        bucket=settings.S3Bucket,
        output_dir=settings.TweetStorageDir,
    )

    tweets = tweet_storage.readall()
    app.log.info(f'retrieved %s tweets', len(tweets))

    render_contents(app, tweets, ignore_original=True)
    app.log.info('finished rebuilding outputs')
