import base64
import hashlib
import subprocess
from pathlib import Path

import boto3

import yaml

from .utils import log


class PackageError(Exception):
    """Errors related to creating a AWS Lambda Zip package."""

    pass


AWS_LAMBDA_FUNCTIONS_DIR = Path(__file__).joinpath("../../../../aws-lambda").resolve()


def get_sha256_hash(zip_file_bytes):
    hasher = hashlib.sha256()
    hasher.update(zip_file_bytes)
    return base64.b64encode(hasher.digest()).decode()


def get_lambda_function_dirs():
    for lambda_function_dir in AWS_LAMBDA_FUNCTIONS_DIR.glob("*"):
        if (
            lambda_function_dir.is_dir()
            and lambda_function_dir.joinpath("package.json").is_file()
            and lambda_function_dir.joinpath("index.js").is_file()
            and lambda_function_dir.joinpath("aws.yaml").is_file()
        ):
            yield lambda_function_dir


def get_yaml_info(lambda_function_dir):
    aws_yaml_file = lambda_function_dir.joinpath("aws.yaml")
    aws_config = yaml.load(aws_yaml_file.read_text(), Loader=yaml.FullLoader)
    return (aws_config["name"], aws_config["region"])


def make_package(lambda_function_dir):
    # Remove any existing zip package(s).
    for zip_filename in lambda_function_dir.glob("*.zip"):
        zip_filename.unlink()
    # Create a new zip package.
    subprocess_result = subprocess.run(
        "yarn run make-package",
        cwd=lambda_function_dir,
        shell=True,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    # If we created a zip package, return the filename.
    for zip_filename in lambda_function_dir.glob("*.zip"):
        return zip_filename
    else:
        # Otherwise, something went wrong.
        raise PackageError(
            "failed to create an AWS Lambda deployment package (Zip file)\n\n"
            f"{subprocess_result.stdout}"
        )


def update(lambda_function_dir, dry_run=False):
    zip_filename = make_package(lambda_function_dir)
    function_name, region_name = get_yaml_info(lambda_function_dir)
    client = boto3.client("lambda", region_name=region_name)
    # Get the function info for the latest version.
    function_info = client.get_function(FunctionName=function_name)
    function_hash = function_info["Configuration"]["CodeSha256"]
    zip_file_bytes = zip_filename.read_bytes()
    # If nothing has changed, don't do anything.
    if get_sha256_hash(zip_file_bytes) != function_hash:
        if dry_run:
            # If this is a dry run, just return the existing function info.
            return function_info["Configuration"]
        # Return the function info of the freshly-published version.
        return client.update_function_code(
            FunctionName=function_name, ZipFile=zip_file_bytes, Publish=True
        )
    return None


def update_all(dry_run=False):
    log.info(f"Updating all lambda functions within: {AWS_LAMBDA_FUNCTIONS_DIR}")

    for lambda_function_dir in get_lambda_function_dirs():
        log.info(f"Found {lambda_function_dir.stem}: ", nl=False)
        try:
            info = update(lambda_function_dir, dry_run=dry_run)
        except PackageError as error:
            log.error(error)
        else:
            if info:
                if dry_run:
                    log.info(
                        f"would have published a new version of {info['FunctionName']}"
                    )
                else:
                    log.success(
                        f"published version {info['Version']} of {info['FunctionName']} "
                        f"as {info['FunctionArn']}",
                    )
            else:
                log.info(f" no change (matches the latest version in AWS)")
