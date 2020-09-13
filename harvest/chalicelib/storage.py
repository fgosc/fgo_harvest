import io
import os
import pathlib
import shutil
from logging import getLogger
from typing import BinaryIO, Dict, Protocol

import boto3  # type: ignore
import botocore.exceptions  # type: ignore

logger = getLogger(__name__)


class SupportStorage(Protocol):
    def exists(self, path: str) -> bool:
        ...

    def get_as_text(self, path: str) -> str:
        ...

    def get_output_stream(self, path: str) -> BinaryIO:
        ...

    def close_output_stream(self, stream: BinaryIO) -> None:
        ...

    def path_object(self, basedir: str) -> pathlib.PurePath:
        ...

    def copy(self, src: str, dest: str) -> None:
        ...


class FilesystemStorage:
    def exists(self, path: str) -> bool:
        return os.path.exists(path)

    def get_as_text(self, path: str) -> str:
        if not os.path.exists(path):
            return ''
        with open(path) as fp:
            return fp.read()

    def get_output_stream(self, path: str) -> BinaryIO:
        return open(path, 'wb')

    def close_output_stream(self, stream: BinaryIO) -> None:
        stream.close()

    def path_object(self, basedir: str) -> pathlib.PurePath:
        return pathlib.Path(basedir)

    def copy(self, src: str, dest: str) -> None:
        shutil.copyfile(src, dest)


class AmazonS3Storage:
    def __init__(
        self,
        bucket: str,
    ):
        self.s3 = boto3.resource('s3')
        self.s3client = boto3.client('s3')
        self.bucket = self.s3.Bucket(bucket)
        self.key_stream_pairs: Dict[str, BinaryIO] = {}

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

    def get_as_text(self, path: str) -> str:
        logger.info(f'get s3://{self.bucket.name}/{path}')
        if not self.exists(path):
            return ''

        bio = io.BytesIO()
        self.bucket.download_fileobj(path, bio)
        return bio.getvalue().decode('utf-8')

    def get_output_stream(self, path: str) -> BinaryIO:
        bio = io.BytesIO()
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
