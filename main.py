import os
import boto3
import logging
import mimetypes
import subprocess
from zipfile import ZipFile
from botocore.exceptions import ClientError

BUCKET_NAME = "PROD_BUCKET_NAME"
OLD_BUCKET_NAME = "DEV_BUCKET_NAME"
FFMPEG_BUCKET_NAME = "FFMPEG_BUCKET_NAME"

FFMPEG_ZIP_PATH = "/tmp/ffmpeg-static.zip"
FFMPEG_EXTRACTED_ZIP_PATH = "/tmp/ffmpeg-static"
FFMPEG_PATH = "/tmp/ffmpeg-static/ffmpeg-static/ffmpeg"

S3_CLIENT = boto3.client(
    's3',
    aws_access_key_id="AWS_S3_ACCESS_KEY_ID",
    aws_secret_access_key="AWS_S3_SECRET_ACCESS_KEY",
)


def load_tools():
    S3_CLIENT.download_file(FFMPEG_BUCKET_NAME, "ffmpeg-static.zip", FFMPEG_ZIP_PATH)

    with ZipFile(FFMPEG_ZIP_PATH, 'r') as zObject:
        zObject.extractall(path=FFMPEG_EXTRACTED_ZIP_PATH)

    zObject.close()
    os.remove(FFMPEG_ZIP_PATH)

    commands = [
        "chmod",
        "755",
        FFMPEG_PATH,
    ]

    result = subprocess.call(commands)
    print(result)


def lambda_handler(event, _):
    load_tools()

    first_record = event["Records"][0]
    print(first_record["s3"]["object"])

    key = first_record["s3"]["object"]["key"]
    print(f"key: {key}")

    downloaded_file_path = f'/tmp/d{key}'
    print(f"downloaded_file_path: {downloaded_file_path}")

    S3_CLIENT.download_file(OLD_BUCKET_NAME, key, downloaded_file_path)

    mimetype = mimetypes.guess_type(downloaded_file_path)[0]
    print(f"mimetype: {mimetype}")

    processed_file_path = f'/tmp/{key.split(".")[0]}.{"mp4" if mimetype.startswith("video") else "webp"}'

    if mimetype.startswith("video"):
        transcode_video(downloaded_file_path, processed_file_path)
    else:
        transcode_image(downloaded_file_path, processed_file_path)

    # S3_CLIENT.delete_object(Bucket=OLD_BUCKET_NAME, Key=key)
    upload_result = upload_file(processed_file_path, BUCKET_NAME, key)

    print(f"{key} - {upload_result}")

    return {
        'statusCode': 200,
        'body': event["Records"],
    }


def upload_file(file_name, bucket, object_name=None):
    if object_name is None:
        object_name = os.path.basename(file_name)

    try:
        response = S3_CLIENT.upload_file(file_name, bucket, object_name)
        print(response)
    except ClientError as e:
        logging.error(e)
        return False

    return True


def transcode_video(downloaded_file_path, processed_file_path):
    commands = [
        FFMPEG_PATH,
        "-y",
        "-i",
        downloaded_file_path,
        "-s",
        "1280x720",
        "-r",
        "24",
        "-c:a",
        "copy",
        "-b:a",
        "128k",
        "-b:v",
        "1000k",
        "-c:v",
        "copy",
        "-preset",
        "fast",
        "-v",
        "error",
        "-f",
        "mp4",
        processed_file_path,
    ]

    result = subprocess.call(commands)
    print(result)


def transcode_image(downloaded_file_path, processed_file_path):
    from PIL import Image

    img = Image.open(downloaded_file_path)
    img.save(processed_file_path, quality=80, optimize=True)
