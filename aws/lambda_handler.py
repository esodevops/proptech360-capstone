import json
import os
import hashlib
import boto3
from urllib.parse import unquote_plus

s3 = boto3.client('s3')
AUDIT_BUCKET = os.environ.get('AUDIT_BUCKET')


def lambda_handler(event, context):
    processed = 0

    # TODO: iterate Records safely; validate s3:ObjectCreated event, raw/ prefix and .csv suffix.
    for record in event.get('Records', []):
        if record.get('eventSource') != 'aws:s3':
            continue
        if not record.get('eventName', '').startswith('ObjectCreated:'):
            continue

        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        if not key.startswith('raw/') or not key.lower().endswith('.csv'):
            continue  # Ignore audit files so they cannot trigger a loop.

        # TODO: decode the object key, inspect metadata with head_object and reject zero-size files.
        # The key was decoded above so names containing spaces work correctly.
        version = record['s3']['object'].get('versionId')
        request = {'Bucket': bucket, 'Key': key}
        if version:
            request['VersionId'] = version
        metadata = s3.head_object(**request)
        if metadata['ContentLength'] == 0:
            raise ValueError(f'Empty CSV file: {key}')

        etag = metadata['ETag'].strip('"')
        event_etag = record['s3']['object'].get('eTag', etag).strip('"')
        if not version and event_etag != etag:
            print(json.dumps({'status': 'superseded', 'key': key}))
            continue  # A newer upload has replaced this unversioned file.

        # TODO: derive idempotent audit key from bucket/key/object version or eTag.
        # Repeated notifications use the same filename instead of adding duplicates.
        identity = json.dumps([bucket, key, version or etag])
        audit_key = 'audit/' + hashlib.sha256(identity.encode()).hexdigest() + '.json'

        # TODO: write JSON audit under audit/ with scoped S3 permissions; structured logs.
        audit = {
            'source_bucket': bucket,
            'source_file': key,
            'size_bytes': metadata['ContentLength'],
            'content_type': metadata.get('ContentType'),
            'version_id': version,
            'etag': etag,
            'status': 'accepted'
        }
        s3.put_object(
            Bucket=AUDIT_BUCKET or bucket,
            Key=audit_key,
            Body=json.dumps(audit),
            ContentType='application/json',
            ServerSideEncryption='AES256'
        )
        print(json.dumps(audit))
        processed += 1

    # TODO: handle or report partial failure without swallowing exceptions.
    # S3 errors raise an exception automatically, allowing Lambda to retry.
    # Audits already written are safely overwritten during that retry.
    return {'statusCode': 200, 'files_audited': processed}
