import concurrent.futures
import hashlib
import mimetypes
import os
import re
from dataclasses import dataclass
from functools import cached_property, partial

import boto3
from boto3.s3.transfer import S3TransferConfig

import click

from . import __version__
from .constants import (
    DEFAULT_CACHE_CONTROL,
    HASHED_CACHE_CONTROL,
    MAX_WORKERS_PARALLEL_UPLOADS,
)
from .utils import StopWatch, fmt_size, iterdir, log


hashed_filename_regex = re.compile(r"\.[a-f0-9]{8,32}\.")


@dataclass
class Totals:
    """Class for keeping track of some useful totals."""

    skipped: int = 0
    uploaded_files: int = 0
    uploaded_redirects: int = 0
    uploaded_files_size: int = 0


class DisplayProgress:
    def __init__(self, total_count, show_progress_bar=True):
        if show_progress_bar:
            self.progress_bar = click.progressbar(
                fill_char="▋",
                show_pos=True,
                show_eta=False,
                show_percent=True,
                length=total_count,
            )
        else:
            self.progress_bar = None

    def __enter__(self):
        if self.progress_bar:
            # Forces the progress-bar to show-up immediately.
            self.progress_bar.update(0)
            self.progress_bar.__enter__()
        return self

    def __exit__(self, *exc_args):
        if self.progress_bar:
            return self.progress_bar.__exit__(*exc_args)
        return False

    def update(self, task):
        if self.progress_bar:
            self.progress_bar.update(1)
        else:
            log.success(f"  {task}")


class UploadTask:
    """
    Base class for indicating the common interface for all upload tasks.
    """

    error = None
    skipped = False
    is_redirect = False

    def upload(self):
        raise NotImplementedError()


class UploadFileTask(UploadTask):
    """
    Class for file upload tasks.
    """

    def __init__(self, file_path, key):
        self.key = key
        self.file_path = file_path

    def __repr__(self):
        return f"UploadFileTask({self.file_path}, {self.key})"

    def __str__(self):
        return self.key

    @property
    def size(self):
        return self.file_path.stat().st_size

    @property
    def etag(self):
        """
        Calculates and returns a value equivalent to the file's AWS ETag value.
        """
        md5s = []
        chunksize = S3TransferConfig().multipart_chunksize

        with self.file_path.open("rb") as f:
            for chunk in iter(partial(f.read, chunksize), b""):
                md5s.append(hashlib.md5(chunk))

        if not md5s:
            return '"{}"'.format(hashlib.md5().hexdigest())

        if len(md5s) == 1:
            return '"{}"'.format(md5s[0].hexdigest())

        digests_md5 = hashlib.md5(b"".join(m.digest() for m in md5s))
        return '"{}-{}"'.format(digests_md5.hexdigest(), len(md5s))

    @property
    def mime_type(self):
        return mimetypes.guess_type(str(self.file_path))[0] or "binary/octet-stream"

    @property
    def is_hashed(self):
        return hashed_filename_regex.search(self.file_path.name)

    @property
    def cache_control(self):
        if self.file_path.name == "service-worker.js":
            return "no-cache"

        if self.is_hashed:
            cache_control_seconds = HASHED_CACHE_CONTROL
        else:
            cache_control_seconds = DEFAULT_CACHE_CONTROL

        return f"max-age={cache_control_seconds}, public"

    def upload(self, bucket_manager):
        bucket_manager.client.upload_file(
            str(self.file_path),
            bucket_manager.bucket_name,
            self.key,
            ExtraArgs={
                "ACL": "public-read",
                "ContentType": self.mime_type,
                "CacheControl": self.cache_control,
            },
        )


class UploadRedirectTask(UploadTask):
    """
    Class for redirect upload tasks.
    """

    is_redirect = True

    def __init__(self, redirect_from_key, redirect_to_key):
        self.key = redirect_from_key
        self.to_key = redirect_to_key

    def __repr__(self):
        return f"UploadRedirectTask({self.key}, {self.to_key})"

    def __str__(self):
        return f"{self.key} -> {self.to_key}"

    @property
    def cache_control(self):
        return f"max-age={HASHED_CACHE_CONTROL}, public"

    def upload(self, bucket_manager):
        bucket_manager.client.put_object(
            Body=b"",
            Key=self.key,
            ACL="public-read",
            CacheControl=self.cache_control,
            WebsiteRedirectLocation=self.to_key,
            Bucket=bucket_manager.bucket_name,
        )


class BucketManager:
    def __init__(self, bucket_name, bucket_prefix):
        self.bucket_name = bucket_name
        self.bucket_prefix = bucket_prefix

    @property
    def key_prefix(self):
        if self.bucket_prefix:
            return f"{self.bucket_prefix.lower()}/"
        return ""

    @cached_property
    def client(self):
        return boto3.client("s3")

    def get_key(self, build_directory, file_path):
        return f"{self.key_prefix}{str(file_path.relative_to(build_directory)).lower()}"

    def get_redirect_keys(self, from_url, to_url):
        return (
            f"{self.key_prefix}{from_url.strip('/').lower()}/index.html",
            f"/{to_url.strip('/').lower()}",
        )

    def get_bucket_objects(self):
        result = {}
        continuation_token = None
        while True:
            kwargs = dict(Bucket=self.bucket_name)
            if self.key_prefix:
                kwargs["Prefix"] = self.key_prefix
            if continuation_token:
                kwargs["ContinuationToken"] = continuation_token
            response = self.client.list_objects_v2(**kwargs)
            for obj in response.get("Contents", ()):
                result[obj["Key"]] = obj
            if response["IsTruncated"]:
                continuation_token = response["NextContinuationToken"]
            else:
                break
        return result

    def iter_file_tasks(self, build_directory, for_counting_only=False):
        # Walk the build_directory and yield file upload tasks.
        for fp in iterdir(build_directory):
            # Exclude any files that aren't artifacts of the build.
            if (fp.name == ".DS_Store") or fp.name.endswith("~"):
                continue
            elif for_counting_only:
                yield 1
            else:
                yield UploadFileTask(fp, self.get_key(build_directory, fp))

    def iter_redirect_tasks(self, content_roots, for_counting_only=False):
        # Walk the content roots and yield redirect upload tasks.
        for content_root in content_roots:
            for fp in content_root.glob("**/_redirects.txt"):
                for line_num, line in enumerate(fp.read_text().split("\n"), start=1):
                    line = line.strip()
                    if line and not line.startswith("#"):
                        parts = line.lower().split("\t")
                        if len(parts) != 2:
                            raise Exception(
                                f"Unable to parse {fp}:{line_num} into a from/to URL pair"
                            )
                        from_url, to_url = parts
                        if for_counting_only:
                            yield 1
                        else:
                            yield UploadRedirectTask(
                                *self.get_redirect_keys(from_url, to_url)
                            )

    def count_file_tasks(self, build_directory):
        return sum(self.iter_file_tasks(build_directory, for_counting_only=True))

    def count_redirect_tasks(self, content_roots):
        return sum(self.iter_redirect_tasks(content_roots, for_counting_only=True))

    def upload(
        self,
        build_directory,
        content_roots,
        on_list_bucket_start=None,
        on_list_bucket_complete=None,
        on_task_complete=None,
        dry_run=False,
        force_refresh=False,
    ):
        if force_refresh:
            bucket_objects = None
        else:
            if on_list_bucket_start:
                on_list_bucket_start()
            with StopWatch() as timer:
                bucket_objects = self.get_bucket_objects()
            if on_list_bucket_complete:
                on_list_bucket_complete(bucket_objects, timer)

        if dry_run:
            return StopWatch()

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=MAX_WORKERS_PARALLEL_UPLOADS
        ) as executor, StopWatch() as timer:
            # Upload the redirects first, then the built files. This
            # ensures that a built file overrides its stale redirect.
            for task_iter in (
                lambda: self.iter_redirect_tasks(content_roots),
                lambda: self.iter_file_tasks(build_directory),
            ):
                futures = {}
                for task in task_iter():
                    # Note: redirect upload tasks are never skipped.
                    if bucket_objects and not task.is_redirect:
                        s3_obj = bucket_objects.get(task.key)
                        if s3_obj and s3_obj["ETag"] == task.etag:
                            task.skipped = True
                            if on_task_complete:
                                on_task_complete(task)
                            continue
                    future = executor.submit(task.upload, self)
                    futures[future] = task

                for future in concurrent.futures.as_completed(futures):
                    task = futures[future]
                    try:
                        task.error = future.exception()
                    except concurrent.futures.CancelledError as cancelled:
                        task.error = cancelled

                    if on_task_complete:
                        on_task_complete(task)

        return timer


def upload_content(build_directory, content_roots, config):
    full_timer = StopWatch().start()

    dry_run = config["dry_run"]
    bucket_name = config["bucket"]
    bucket_prefix = config["folder"]
    refresh = config["force_refresh"]
    show_progress_bar = not config["no_progressbar"]

    if bucket_name in ("dev", "stage", "prod"):
        bucket_name = f"mdn-content-{bucket_name}"

    log.info(f"Upload files from: {build_directory}")
    log.info(f"Upload redirects from: {', '.join(str(fp) for fp in content_roots)}")
    log.info("Upload into: ", nl=False)
    if bucket_prefix:
        log.info(f"{bucket_prefix}/ of ", nl=False)
    log.info(bucket_name)

    mgr = BucketManager(bucket_name, bucket_prefix)

    with StopWatch() as timer:
        total_redirects = mgr.count_redirect_tasks(content_roots)
    log.info(f"Total pending redirect uploads: {total_redirects:,} ({timer})")

    with StopWatch() as timer:
        total_possible_files = mgr.count_file_tasks(build_directory)
    log.info(f"Total pending file uploads: {total_possible_files:,} ({timer})")

    totals = Totals()
    failed_tasks = []

    def on_list_bucket_start():
        log.info("Total existing S3 objects: ", nl=False)

    def on_list_bucket_complete(bucket_objects, timer):
        log.info(f"{len(bucket_objects):,} ({timer})")

    with DisplayProgress(
        total_redirects + total_possible_files, show_progress_bar
    ) as progress:

        def on_task_complete(task):
            progress.update(task)
            if task.skipped:
                totals.skipped += 1
            elif task.error:
                failed_tasks.append(task)
            elif task.is_redirect:
                totals.uploaded_redirects += 1
            else:
                totals.uploaded_files += 1
                totals.uploaded_files_size += task.size

        upload_timer = mgr.upload(
            build_directory,
            content_roots,
            on_list_bucket_start=on_list_bucket_start,
            on_list_bucket_complete=on_list_bucket_complete,
            on_task_complete=on_task_complete,
            dry_run=dry_run,
            force_refresh=refresh,
        )

    if failed_tasks:
        log.error(f"Total failures: {len(failed_tasks):,}")
        for task in failed_tasks:
            log.error(f"\n{task} failed:\n{task.error}")

    log.info(
        f"Total uploaded files: {totals.uploaded_files:,} "
        f"({fmt_size(totals.uploaded_files_size)})"
    )
    log.info(f"Total uploaded redirects: {totals.uploaded_redirects:,}")
    log.info(f"Total skipped files: {totals.skipped:,} matched existing S3 objects")
    log.info(f"Total upload/skip time: {upload_timer}")
    log.info(f"Done in {full_timer.stop()}.")
