import io
import pathlib
import shutil
from logging import getLogger
from typing import BinaryIO, Iterator, Protocol

import boto3  # type: ignore
import botocore.exceptions  # type: ignore

logger = getLogger(__name__)


class SupportStorage(Protocol):
    def list(
        self,
        basedir: str,
        prefix: str = '',
        suffix: str = '',
    ) -> Iterator[str]:
        ...

    def exists(self, path: str) -> bool:
        ...

    def get_as_text(self, path: str) -> str:
        ...

    def get_as_binary(self, path: str) -> bytes:
        ...

    def get_output_stream(self, path: str, append: bool = False) -> BinaryIO:
        ...

    def close_output_stream(self, stream: BinaryIO) -> None:
        ...

    def path_object(self, basedir: str) -> pathlib.PurePath:
        ...

    def copy(self, src: str, dest: str) -> None:
        ...

    def streams(
        self,
        basedir: str,
        prefix: str = '',
        suffix: str = '',
    ) -> Iterator[BinaryIO]:
        ...

    def delete(self, path: str) -> None:
        ...


class FilesystemStorage:
    def list(
        self,
        basedir: str,
        prefix: str = '',
        suffix: str = '',
    ) -> Iterator[str]:
        entries = pathlib.Path(basedir).glob(prefix + '*' + suffix)
        for entry in entries:
            if entry.is_file():
                yield str(entry)

    def exists(self, path: str) -> bool:
        return pathlib.Path(path).exists()

    def get_as_text(self, path: str) -> str:
        if not pathlib.Path(path).exists():
            return ''
        with open(path) as fp:
            return fp.read()

    def get_as_binary(self, path: str) -> bytes:
        if not pathlib.Path(path).exists():
            return b''
        with open(path, 'rb') as fp:
            return fp.read()

    def get_output_stream(self, path: str, append: bool = False) -> BinaryIO:
        if append:
            return open(path, 'ab')
        else:
            return open(path, 'wb')

    def close_output_stream(self, stream: BinaryIO) -> None:
        stream.close()

    def path_object(self, basedir: str) -> pathlib.PurePath:
        return pathlib.Path(basedir)

    def copy(self, src: str, dest: str) -> None:
        shutil.copyfile(src, dest)

    def streams(
        self,
        basedir: str,
        prefix: str = '',
        suffix: str = '',
    ) -> Iterator[BinaryIO]:
        entries = pathlib.Path(basedir).glob(prefix + '*' + suffix)
        for entry in entries:
            if entry.is_file():
                logger.info('read %s', entry.name)
                with open(entry, 'rb') as fp:
                    yield fp

    def delete(self, path: str) -> None:
        pathlib.Path(path).unlink(missing_ok=True)


class AmazonS3Storage:
    def __init__(
        self,
        bucket: str,
    ):
        self.s3 = boto3.resource('s3')
        self.s3client = boto3.client('s3')
        self.bucket = self.s3.Bucket(bucket)
        self.key_stream_pairs: dict[str, BinaryIO] = {}

    def list(
        self,
        basedir: str,
        prefix: str = '',
        suffix: str = '',
    ) -> Iterator[str]:
        prefix = f"{basedir}/{prefix}"
        object_summaries = self.bucket.objects.filter(Prefix=prefix)

        for entry in object_summaries:
            if entry.key.endswith(suffix):
                yield entry.key

    def exists(self, path: str) -> bool:
        try:
            self.s3client.head_object(
                Bucket=self.bucket.name,
                Key=path,
            )
            return True

        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            # Unexpceted Error
            raise

    def _get_object(self, path: str) -> bytes:
        logger.info(f'get s3://{self.bucket.name}/{path}')
        if not self.exists(path):
            return b''

        bio = io.BytesIO()
        self.bucket.download_fileobj(path, bio)
        return bio.getvalue()

    def get_as_text(self, path: str) -> str:
        return self._get_object(path).decode('utf-8')

    def get_as_binary(self, path: str) -> bytes:
        return self._get_object(path)

    def get_output_stream(self, path: str, append: bool = False) -> BinaryIO:
        bio = io.BytesIO()

        if append:
            existent_data = self.get_as_binary(path)
            bio.write(existent_data)

        # この時点で key を記憶しておかないと後で stream を渡された
        # ときに対応する key を復元できなくなる。
        self.key_stream_pairs[path] = bio
        return bio

    def close_output_stream(self, stream: BinaryIO) -> None:
        for s3key, bio in self.key_stream_pairs.items():
            if bio is not stream:
                continue
            obj = self.bucket.Object(s3key)
            if s3key.endswith('.json'):
                content_type = 'application/json'
            elif s3key.endswith('.html'):
                content_type = 'text/html'
            elif s3key.endswith('.txt'):
                content_type = 'text/plain'
            elif s3key.endswith('.csv'):
                content_type = 'text/csv'
            else:
                content_type = 'application/octet-stream'
            logger.info(
                f'put s3://{self.bucket.name}/{s3key}, '
                f'content_type={content_type}'
            )
            bio.seek(0)
            obj.upload_fileobj(
                stream,
                ExtraArgs={'ContentType': content_type},
            )
            stream.close()
            return
        raise ValueError('could not put a stream object to S3')

    def path_object(self, basedir: str) -> pathlib.PurePath:
        return pathlib.PurePosixPath(basedir)

    def copy(self, src: str, dest: str) -> None:
        source = {'Bucket': self.bucket.name, 'Key': src}
        self.bucket.copy(source, dest)

    def streams(
        self,
        basedir: str,
        prefix: str = '',
        suffix: str = '',
    ) -> Iterator[BinaryIO]:
        prefix = f"{basedir}/{prefix}"
        object_summaries = self.bucket.objects.filter(Prefix=prefix)

        for entry in object_summaries:
            if entry.key.endswith(suffix):
                logger.info(f'get s3://{self.bucket.name}/{entry.key}')
                resp = entry.get()
                yield resp['Body']

    def delete(self, path: str) -> None:
        self.bucket.Object(path).delete()
