import json
import boto3

sfn_client = boto3.client('stepfunctions')

def lambda_handler(event, context):
    print("Received event:", json.dumps(event))


    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    # Start execution
    response = sfn_client.start_execution(
        stateMachineArn='arn:aws:states:eu-north-1:309797288544:stateMachine:DeltaStateMachine-4585d173',
        input=json.dumps({
            "bucket": bucket,
            "key": key
        })
    )

    print("Step Function started:", response['executionArn'])
    return {
        'statusCode': 200,
        'body': json.dumps('Step Function triggered')
    }

