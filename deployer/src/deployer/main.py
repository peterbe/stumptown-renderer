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


def validate_directory(ctx, param, value):
    path = Path(value)
    if not path.exists():
        raise click.BadParameter(f"{value} does not exist")
    elif not path.is_dir():
        raise click.BadParameter(f"{value} is not a directory")
    return path


def validate_root(ctx, param, value):
    return validate_directory(ctx, param, value)


def validate_optional_root(ctx, param, value):
    if value:
        return validate_directory(ctx, param, value)


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
    callback=validate_root,
)
@click.option(
    "--content-translated-root",
    help=(
        "The path to the root folder of the translated content "
        "(defaults to CONTENT_TRANSLATED_ROOT)"
    ),
    default=CONTENT_TRANSLATED_ROOT,
    show_default=True,
    callback=validate_optional_root,
)
@click.option(
    "--no-progressbar",
    help="Don't show the progress bar",
    default=DEFAULT_NO_PROGRESSBAR,
    show_default=True,
    is_flag=True,
)
@click.argument("directory", type=click.Path(), callback=validate_directory)
@click.pass_context
def upload(ctx, directory: Path, **kwargs):
    log.info(f"Deployer ({__version__})", bold=True)
    content_roots = [kwargs["content_root"]]
    if kwargs["content_translated_root"]:
        content_roots.append(kwargs["content_translated_root"])

    ctx.obj.update(kwargs)
    upload_content(directory, content_roots, ctx.obj)
