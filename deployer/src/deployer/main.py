from pathlib import Path

import click

from . import __version__
from .aws_lambda import update_all as update_all_lambda_functions
from .constants import (
    CONTENT_ROOT,
    CONTENT_TRANSLATED_ROOT,
    DEFAULT_BUCKET_NAME,
    DEFAULT_NO_PROGRESSBAR,
)
from .upload import upload_content
from .utils import log


@click.group()
@click.option(
    "--dry-run",
    default=False,
    help="Show what would be done, but don't actually do it.",
    show_default=True,
    is_flag=True,
)
@click.version_option(version=__version__)
@click.pass_context
def cli(ctx, **kwargs):
    ctx.ensure_object(dict)
    ctx.obj.update(kwargs)


@cli.command()
@click.pass_context
def update_lambda_functions(ctx):
    log.info(f"Deployer ({__version__})", bold=True)
    update_all_lambda_functions(dry_run=ctx.obj["dry_run"])


@cli.command()
@click.option(
    "--bucket",
    help='Name of the S3 bucket or one of "dev", "stage", or "prod"',
    default=DEFAULT_BUCKET_NAME,
    show_default=True,
)
@click.option(
    "--folder", help="Upload into this folder of the S3 bucket instead of its root",
)
@click.option(
    "--force-refresh",
    is_flag=True,
    default=False,
    show_default=True,
    help="Forces all files to be uploaded even if they already match what is in S3",
)
@click.option(
    "--content-root",
    help="The path to the root folder of the main content (defaults to CONTENT_ROOT)",
    default=CONTENT_ROOT,
    show_default=True,
)
@click.option(
    "--content-translated-root",
    help="The path to the root folder of the translated content (defaults to CONTENT_TRANSLATED_ROOT)",
    default=CONTENT_TRANSLATED_ROOT,
    show_default=True,
)
@click.option(
    "--no-progressbar",
    help="Don't show the progress bar",
    default=DEFAULT_NO_PROGRESSBAR,
    show_default=True,
    is_flag=True,
)
@click.argument("directory", type=click.Path())
@click.pass_context
def upload(ctx, directory, **kwargs):
    log.info(f"Deployer ({__version__})", bold=True)
    content_roots = []
    for name in ("content_root", "content_translated_root"):
        value = kwargs.pop(name)
        if not value:
            log.warning(f"Warning: No {name.replace('_', '-')} has been specified.")
            continue
        content_root = Path(value)
        if not content_root.is_dir():
            raise click.ClickException(
                f'The {name_hyphenated} "{content_root}" does not exist or is '
                "not a directory."
            )
        content_roots.append(content_root)

    dirpath = Path(directory)

    if not dirpath.exists():
        raise click.ClickException(f"{directory} does not exist")

    ctx.obj.update(kwargs)
    upload_content(dirpath, content_roots, ctx.obj)
