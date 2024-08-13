import json

def lambda_handler(event, context):
    print(event)
    
    s3_event = event.get('Records', [{}])[0].get('s3', {})
    bucket_name = s3_event.get('bucket', {}).get('name', 'bigdataprojectetlpipeline')
    object_key = s3_event.get('object', {}).get('key', 'athena_query_logs')
    
    print(f"S3 Bucket: {bucket_name}")
    print(f"S3 Object Key: {object_key}")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }