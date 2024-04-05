import datetime
import json
import os
import tempfile

import boto3
import earthaccess
# TODO get gdal installed in a lambda
from osgeo import gdal, gdalconst

s3 = boto3.client('s3')
# TODO specify region?
sns = boto3.client('sns')
# TODO check if this is necessary
# earthaccess.login(strategy='ENVIRONMENT')
gdal.UseExceptions()


def create_gibs_image(output_filename, vv_filename, vh_filename):
    # TODO figure out how to scale to 0-255 with fourth alpha band for no data
    # TODO get the science team's input on scale values
    # TODO make sure intermediate files are in the temp directory not the working directory
    vv_scaled = gdal.Translate('vv_scaled.tif', vv_filename, outputType=gdalconst.GDT_Byte, scaleParams=[[0, 0.5916]])
    vh_scaled = gdal.Translate('vh_scaled.tif', vh_filename, outputType=gdalconst.GDT_Byte, scaleParams=[[0, 0.1]])
    rgb = gdal.BuildVRT('rgb.vrt', [vv_scaled, vh_scaled, vv_scaled], separate=True)
    gdal.Warp(
        output_filename,
        rgb,
        dstSRS='+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs',
        xRes=2.74658203125e-4,
        yRes=2.74658203125e-4,
        format='GTiff',
        creationOptions=['COMPRESS=LZW', 'TILED=YES']
    )
    return output_filename


def build_gibs_message(granule_ur: str, bucket: str, key: str, size: int) -> dict:
    return {
        # TODO confirm with GIBS what they actually need in this message
        # https://github.com/podaac/cloud-notification-message-schema
        'identifier': granule_ur,
        'collection': 'OPERA_L2_RTC-S1_V1',
        'submissionTime': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'product': {
            'name': granule_ur,
            'files': {
                'name': os.path.basename(key),
                'type': 'browse',
                # TODO get file size
                'size': size,
                'uri': f's3://{bucket}/{key}',
            },
        },
        'version': '1.6.1',
    }


def download_tifs(granule_ur: str, directory: str) -> list[str]:
    results = earthaccess.search_data(
        short_name='OPERA_L2_RTC-S1_V1',
        granule_ur=granule_ur,
    )
    links = [link for link in results[0].data_links() if link.endswith('VV.tif') or link.endswith('VH.tif')]
    # TODO debug http 401 errors
    return earthaccess.download(links, directory)


def process_granule(granule_ur):
    bucket = os.environ['BUCKET']
    key = f'{granule_ur}.tif'
    topic_arn = os.environ['TOPIC_ARN']

    with tempfile.TemporaryDirectory() as temp_dir:
        local_filename = os.path.join(temp_dir, key)
        # TODO deal with non VV+VH granules
        vh_filename, vv_filename = sorted(download_tifs(granule_ur, temp_dir))
        create_gibs_image(local_filename, vv_filename, vh_filename)
        size = os.stat(local_filename).st_size
        s3.upload_file(local_filename, bucket, key)

    message = build_gibs_message(granule_ur, bucket, key, size)
    sns.publish(TopicArn=topic_arn, Message=message)


def lambda_handler(event, context):
    batch_item_failures = []
    for record in event['Records']:
        try:
            body = json.loads(record['body'])
            message = json.loads(body['Message'])
            process_granule(message['granule_ur'])
        except Exception:
            print(f'Could not process message {record["messageId"]}')
            batch_item_failures.append({'itemIdentifier': record['messageId']})
    return {'batchItemFailures': batch_item_failures}
