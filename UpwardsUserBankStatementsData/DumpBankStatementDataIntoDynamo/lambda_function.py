from __future__ import print_function

import json
import boto3
dynamodb = boto3.resource('dynamodb')

print('Loading function')


def respond(err, res=None):
    return {
        'statusCode': '400' if err else '200',
        'body': err.message if err else res,
        'headers': {
            'Content-Type': 'application/json',
        },
    }


def create_data(table, event):
    input_data = {
        'customer_id': str(event['body']['customer_id']),
        'created_at': int(event['body']['created_at']),
        'data': event['body']['data']
    }
    return table.put_item(Item=input_data)


def get_data(table, event):
    from boto3.dynamodb.conditions import Key, Attr
    return table.query(KeyConditionExpression=Key('customer_id').eq(event['query']['customer_id']))


def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    operations = {
        'GET': get_data,
        'POST': create_data,
    }
    operation = event['method']
    table = dynamodb.Table('UpwardsBankStatementData')
    if table and operation in operations:
        return respond(None, operations[operation](table, event))
    else:
        return respond(ValueError('Unsupported method: "{operation}" or data_type: {data_type}'.format(operation=operation, data_type=data_type)))
