# Create the DynamoDB table.
import os

import boto3


TABLE = os.environ.get('ProgressTable', 'test-Watchbot-progress')
dynamodb = boto3.resource('dynamodb')

table = dynamodb.create_table(
    TableName=TABLE,
    KeySchema=[
        {
            'AttributeName': 'id',
            'KeyType': 'HASH'
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'id',
            'AttributeType': 'S'
        },

    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 5,
        'WriteCapacityUnits': 5
    }
)
