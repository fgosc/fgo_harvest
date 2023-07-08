import calendar
import concurrent.futures
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from logging import getLogger

import boto3  # type: ignore
from chalice import (  # type: ignore
    Chalice,
    CORSConfig,
    Cron,
    Rate,
)

from chalicelib import (
    graphql,
    merging,
    model,
    settings,
    static,
    storage,
    timezone,
    twitter,
    recording,
    repository,
)

logger = getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))

app = Chalice(app_name='harvest')
app.log.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))
cloudfront = boto3.client('cloudfront')

cors_config = CORSConfig(
    allow_origin=settings.CORSAllowOrigin,
    allow_headers=['X-Special-Header'],
    max_age=180,
    expose_headers=['X-Special-Header'],
    allow_credentials=True,
)


def setup_graphql_client() -> graphql.GraphQLClient:
    return graphql.GraphQLClient(
        endpoint=settings.GraphQLEndpoint,
        api_key=settings.GraphQLApiKey,
    )


def render_date_contents(
    reports: list[model.RunReport],
    skip_target_date: date,
    force_save: bool = False,
    ignore_original: bool = False,
) -> None:
    outdir = f'{settings.ProcessorOutputDir}/date'
    recorder = recording.Recorder(
        partitioningRule=recording.PartitioningRuleByDate(),
        skipSaveRule=recording.SkipSaveRuleByDate(skip_target_date),
        fileStorage=storage.AmazonS3Storage(settings.S3Bucket),
        basedir=outdir,
        formats=(
            recording.OutputFormat.JSON,
            recording.OutputFormat.CSV,
            recording.OutputFormat.DATE_HTML,
        ),
    )
    recorder.add_all(reports)

    if recorder.count():
        recorder.save(force=force_save, ignore_original=ignore_original)

    latestDatePageBuilder = recording.LatestDatePageBuilder(
        fileStorage=storage.AmazonS3Storage(settings.S3Bucket),
        basedir=outdir,
    )
    latestDatePageBuilder.build()


def render_user_contents(
    reports: list[model.RunReport],
    skip_target_date: date,
    force_save: bool = False,
    ignore_original: bool = False,
) -> None:
    outdir = f'{settings.ProcessorOutputDir}/user'
    recorder = recording.Recorder(
        partitioningRule=recording.PartitioningRuleByUser(),
        skipSaveRule=recording.SkipSaveRuleByDateAndUser(skip_target_date),
        fileStorage=storage.AmazonS3Storage(settings.S3Bucket),
        basedir=outdir,
        formats=(
            recording.OutputFormat.JSON,
            recording.OutputFormat.CSV,
            recording.OutputFormat.USER_HTML,
        ),
    )
    recorder.add_all(reports)

    if recorder.count():
        recorder.save(force=force_save, ignore_original=ignore_original)

    recorder_byuserlist = recording.Recorder(
        partitioningRule=recording.PartitioningRuleByUserList(),
        skipSaveRule=recording.SkipSaveRuleNeverMatch(),
        fileStorage=storage.AmazonS3Storage(settings.S3Bucket),
        basedir=outdir,
        formats=(
            recording.OutputFormat.JSON,
            recording.OutputFormat.USER_LIST_HTML,
        )
    )
    recorder_byuserlist.add_all(reports)

    if recorder_byuserlist.count():
        recorder_byuserlist.save(force=force_save, ignore_original=ignore_original)


def render_quest_contents(
    reports: list[model.RunReport],
    skip_target_date: date,
    force_save: bool = False,
    ignore_original: bool = False,
) -> None:
    outdir = f'{settings.ProcessorOutputDir}/quest'
    recorder = recording.Recorder(
        partitioningRule=recording.PartitioningRuleByQuest(),
        skipSaveRule=recording.SkipSaveRuleByDateAndQuest(skip_target_date),
        fileStorage=storage.AmazonS3Storage(settings.S3Bucket),
        basedir=outdir,
        formats=(
            recording.OutputFormat.JSON,
            recording.OutputFormat.CSV,
            recording.OutputFormat.QUEST_HTML,
        ),
    )
    recorder.add_all(reports)

    if recorder.count():
        recorder.save(force=force_save, ignore_original=ignore_original)

    recorder_byquestlist = recording.Recorder(
        # この partitioningRule は rebuild フラグを個別に渡す必要あり
        partitioningRule=recording.PartitioningRuleByQuestList(
            ignore_original,
        ),
        skipSaveRule=recording.SkipSaveRuleNeverMatch(),
        fileStorage=storage.AmazonS3Storage(settings.S3Bucket),
        basedir=outdir,
        formats=(
            recording.OutputFormat.JSON,
            recording.OutputFormat.QUEST_LIST_HTML,
        )
    )
    recorder_byquestlist.add_all(reports)
    # quest list だけはリストの増減がない場合でも数値の countup を
    # 再描画する必要があるので強制上書きが必要
    recorder_byquestlist.save(force=True, ignore_original=ignore_original)


def render_1hrun_contents(
    reports: list[model.RunReport],
    skip_target_date: date,
    force_save: bool = False,
    ignore_original: bool = False,
) -> None:
    outdir = f'{settings.ProcessorOutputDir}/1hrun'
    recorder = recording.Recorder(
        partitioningRule=recording.PartitioningRuleBy1HRun(calendar.THURSDAY),
        skipSaveRule=recording.SkipSaveRuleByDate(skip_target_date),
        fileStorage=storage.AmazonS3Storage(settings.S3Bucket),
        basedir=outdir,
        formats=(
            recording.OutputFormat.JSON,
            recording.OutputFormat.CSV,
            recording.OutputFormat.FGO1HRUN_HTML,
        )
    )
    recorder.add_all(reports)

    if recorder.count():
        recorder.save(force=force_save, ignore_original=ignore_original)

    recorder_by1hrunlist = recording.Recorder(
        partitioningRule=recording.PartitioningRuleBy1HRunWeekList(
            start_day=calendar.THURSDAY,
        ),
        skipSaveRule=recording.SkipSaveRuleNeverMatch(),
        fileStorage=storage.AmazonS3Storage(settings.S3Bucket),
        basedir=outdir,
        formats=(
            recording.OutputFormat.JSON,
            recording.OutputFormat.FGO1HRUN_LIST_HTML,
        )
    )
    recorder_by1hrunlist.add_all(reports)
    recorder_by1hrunlist.save(force=force_save, ignore_original=ignore_original)


def render_error_contents(
    errors: list[twitter.ParseErrorTweet],
    force_save: bool = False,
    ignore_original: bool = False,
) -> None:
    outdir = f'{settings.ProcessorOutputDir}/errors'
    recorder = recording.ErrorPageRecorder(
        fileStorage=storage.AmazonS3Storage(settings.S3Bucket),
        basedir=outdir,
        key='error',
        formats=(
            recording.ErrorOutputFormat.JSON,
            recording.ErrorOutputFormat.HTML,
        )
    )
    recorder.add_all(errors)
    recorder.save(force=force_save, ignore_original=ignore_original)


def render_contents(
    app,
    reports: list[model.RunReport],
    errors: list[twitter.ParseErrorTweet],
    skip_target_date: date,
    ignore_original: bool = False,
) -> None:
    render_date_contents(reports, skip_target_date, ignore_original=ignore_original)
    render_user_contents(reports, skip_target_date, ignore_original=ignore_original)
    render_quest_contents(reports, skip_target_date, ignore_original=ignore_original)
    render_1hrun_contents(reports, skip_target_date, ignore_original=ignore_original)

    render_error_contents(errors, ignore_original=ignore_original)
    app.log.info('done')


def render_month_contents(
    reports: list[model.RunReport],
    skip_target_date: date,
    force_save: bool = False,
):
    # month のレンダリングを render_contents() に含めないのは、日々の
    # collect tweets のアクションで追記していくには month のサイズが
    # 大きすぎるから。さすがに read/write のコストが無視できない。
    outdir = f'{settings.ProcessorOutputDir}/month'

    # 現在月を month のレンダリング対象にしないための工夫
    today = datetime.now(tz=timezone.Local).date()
    last_day_of_prev_month = date(today.year, today.month, 1) - timedelta(days=1)
    app.log.info("SkipSaveRuleByDateRange: start = %s, end = %s", skip_target_date, last_day_of_prev_month)

    recorder = recording.Recorder(
        partitioningRule=recording.PartitioningRuleByMonth(),
        skipSaveRule=recording.SkipSaveRuleByDateRange(skip_target_date, last_day_of_prev_month),
        fileStorage=storage.AmazonS3Storage(settings.S3Bucket),
        basedir=outdir,
        formats=(
            recording.OutputFormat.JSON,
            recording.OutputFormat.CSV,
            recording.OutputFormat.MONTH_HTML,
        ),
    )
    recorder.add_all(reports)

    if recorder.count():
        # original への追記はせず、常に上書き
        recorder.save(force=force_save, ignore_original=True)

    # month の HTML レンダリングが完了してからでないと実行できない。
    # したがってこの位置で実行する。
    latestMonthPageBuilder = recording.LatestMonthPageBuilder(
        fileStorage=storage.AmazonS3Storage(settings.S3Bucket),
        basedir=outdir,
    )
    latestMonthPageBuilder.build()


@app.schedule(Rate(20, unit=Rate.MINUTES))
def collect_reports_scheduled(event):
    collect_reports(event)


@app.lambda_function()
def collect_reports_manually(event, context):
    collect_reports(event)


def collect_reports(event):
    agent = setup_graphql_client()

    report_repository = repository.ReportRepository(
        fileStorage=storage.AmazonS3Storage(settings.S3Bucket),
        basedir=settings.ReportStorageDir,
    )

    last_report_ts_retriever = repository.LastReportTimeStamp(
        fileStorage=storage.AmazonS3Storage(settings.S3Bucket),
        basedir=settings.SettingsDir,
        key=settings.LastReportTimeFile,
    )

    if last_report_ts_retriever.exists():
        last_report_id, since = last_report_ts_retriever.load()
        app.log.info(f"load last_report_ts from S3: report_id = {last_report_id}, timestamp = {since}")
    else:
        last_report_id = ""
        # Twitter Crawling の停止時刻
        since = datetime(2023, 6, 13, 20, 35, 0, tzinfo=timezone.Local)

    since_timestamp = int(since.timestamp())
    app.log.info(f'since: {since}, timestamp: {since_timestamp}')

    reports = agent.list_reports(since_timestamp)
    app.log.info('fetched %s reports', len(reports))

    if len(reports) == 0:
        return

    if len(reports) == 1 and reports[0].report_id == last_report_id:
        app.log.info('no new reports')
        return

    report_log_file = '{}.json'.format(datetime.now(tz=timezone.Local).strftime('%Y%m%d_%H%M%S'))
    app.log.info('report log: %s', report_log_file)
    report_repository.put(report_log_file, reports)

    newest_report = max(reports, key=lambda r: r.timestamp)
    app.log.info(
        "saving newest report id and timestamp: "
        f"id = {newest_report.report_id}, time = {newest_report.timestamp}"
    )
    last_report_ts_retriever.save(newest_report.report_id, newest_report.timestamp)

    # 定期収集において bydate の render を制限する必要はない
    skip_target_date = date(2000, 1, 1)
    render_date_contents(reports, skip_target_date)
    render_user_contents(reports, skip_target_date)
    render_quest_contents(reports, skip_target_date)
    render_1hrun_contents(reports, skip_target_date)

    app.log.info('done')


@app.lambda_function()
def rebuild_outputs(event, context):
    skip_target_date_str = event.get("skipTargetDate")
    if skip_target_date_str:
        skip_target_date = date.fromisoformat(skip_target_date_str)
    else:
        skip_target_date = date(2000, 1, 1)

    skip_build_date = event.get("skipBuildDate", False)
    skip_build_user = event.get("skipBuildUser", False)
    skip_build_quest = event.get("skipBuildQuest", False)
    skip_build_month = event.get("skipBuildMonth", False)
    skip_build_1hrun = event.get("skipBuild1HRun", False)

    app.log.info("skip rebuilding before the target date: %s", skip_target_date)

    tweet_repository = repository.TweetRepository(
        fileStorage=storage.AmazonS3Storage(settings.S3Bucket),
        basedir=settings.TweetStorageDir,
    )

    censored_accounts = twitter.CensoredAccounts(
        fileStorage=storage.AmazonS3Storage(settings.S3Bucket),
        filepath=f'{settings.SettingsDir}/{settings.CensoredAccountsFile}',
    )

    report_repository = repository.ReportRepository(
        fileStorage=storage.AmazonS3Storage(settings.S3Bucket),
        basedir=settings.ReportStorageDir,
    )

    twitter_reports, errors = tweet_repository.readall(set(censored_accounts.list()))
    app.log.info(f'retrieved {len(twitter_reports)} reports, {len(errors)} parse error tweets from twitter archive')

    fgodrop_reports = report_repository.readall()
    app.log.info(f'retrieved {len(fgodrop_reports)} reports from fgodrop archive')

    # マージして新しい順に並べる
    reports = twitter_reports + fgodrop_reports
    reports.sort(key=lambda e: e.timestamp, reverse=True)

    procs = []

    with ThreadPoolExecutor() as executor:
        if skip_build_date:
            app.log.info("skip building date contents")
        else:
            ft = executor.submit(
                render_date_contents,
                reports,
                skip_target_date,
                ignore_original=True,
            )
            procs.append(ft)

        if skip_build_user:
            app.log.info("skip building user contents")
        else:
            ft = executor.submit(
                render_user_contents,
                reports,
                skip_target_date,
                ignore_original=True,
            )
            procs.append(ft)

        if skip_build_quest:
            app.log.info("skip building quest contents")
        else:
            ft = executor.submit(
                render_quest_contents,
                reports,
                skip_target_date,
                ignore_original=True,
            )
            procs.append(ft)

        if skip_build_month:
            app.log.info("skip building month contents")
        else:
            ft = executor.submit(
                render_month_contents,
                reports,
                skip_target_date,
            )
            procs.append(ft)

        if skip_build_1hrun:
            app.log.info("skip building 1HRun contents")
        else:
            ft = executor.submit(
                render_1hrun_contents,
                reports,
                skip_target_date,
                ignore_original=True,
            )
            procs.append(ft)

        ft = executor.submit(
            render_error_contents,
            errors,
            ignore_original=True,
        )
        procs.append(ft)

        done, not_done = concurrent.futures.wait(procs)
        app.log.info("done: %s", done)
        app.log.info("not_done: %s", not_done)

    app.log.info('finished rebuilding outputs')


@app.schedule(Cron(10, 1, '*', '*', '?', '*'))  # JST 10:10 everyday
def merge_tweets_into_datefile(event):
    yesterday = (datetime.utcnow() - timedelta(days=1)).date()
    app.log.info("target date: %s", yesterday)

    merging.merge_into_datefile(
        fileStorage=storage.AmazonS3Storage(settings.S3Bucket),
        basedir=settings.ReportStorageDir,
        target_date=yesterday,
    )


@app.schedule(Cron(10, 2, 1, '*', '?', '*'))  # JST 11:10 every 1st day of the month
def merge_tweets_into_monthfile(event):
    # 月初に動かすので 1 日前は先月のはず
    yesterday = (datetime.utcnow() - timedelta(days=1)).date()
    target_month = yesterday.strftime("%Y%m")
    app.log.info("target month: %s", target_month)

    merging.merge_into_monthfile(
        fileStorage=storage.AmazonS3Storage(settings.S3Bucket),
        basedir=settings.ReportStorageDir,
        target_month=target_month,
    )


@app.lambda_function()
def merge_tweets_into_monthfile_manually(event, context):
    target_month = event["targetMonth"]
    app.log.info("target month: %s", target_month)

    merging.merge_into_monthfile(
        fileStorage=storage.AmazonS3Storage(settings.S3Bucket),
        basedir=settings.ReportStorageDir,
        target_month=target_month,
    )


@app.schedule(Cron(10, 3, 1, '*', '?', '*'))  # JST 12:10 every 1st day of the month
def rebuild_month_summary(event):
    # 32 日前にすれば確実に 1 か月分を覆うことができる
    target_date = (datetime.utcnow() - timedelta(days=32)).date()

    d = event.to_dict()
    d["skipTargetDate"] = target_date.isoformat()
    d["skipBuildDate"] = True
    d["skipBuildUser"] = True
    d["skipBuildQuest"] = True

    rebuild_outputs(d, None)


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


# NOTE: 本来ここに実装すべきものではない。可能なら別リポジトリに切り出すべき。
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

    logger.info("file size: %s", object_size)

    # ad hoc な方法ではあるが、現状これ以上良い解が見つかっていない。
    # blank html のサイズが 455, error page のサイズが 624 なので、その間の値にする。
    if object_size < 600:
        logger.info('ignore cache invalidation: probably blank html')
        return

    items = []
    # .../index.html を invalidate する代わりに
    # .../ を invalidate する。
    items.append(item[:item.rfind('/') + 1])

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
