import boto3, json 
import botocore

config = botocore.config.Config(
    read_timeout=900
)
client = boto3.client('lambda', region_name='sa-east-1' , config=config)

def invokeFunction(input, functionARN, type = 'Event'): 
    response = client.invoke(
        FunctionName=functionARN,
        InvocationType=type,
        Payload=json.dumps(input)
    )

    if not type == 'Event':
        payload = json.load(response['Payload'])
        print(payload)
        return json.loads(payload['body'])
        