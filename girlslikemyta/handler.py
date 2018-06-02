import os
import json
import boto3


def test_sns(event, context):
    print(event)
    return {"statusCode": 200} #ok

# {'Records': [{
#     'EventSource': 'aws:sns',
#     'EventVersion': '1.0',
#     'EventSubscriptionArn': 'arn:aws:sns:us-east-1:983584755688:itt-sns-data-core-stage:493ba9a2-52f0-4c12-b1a4-7bdaee803690'
#         ,
#     'Sns': {
#         'Type': 'Notification',
#         'MessageId': 'dc693f13-b8d8-5445-88b1-6c9954471f92',
#         'TopicArn': 'arn:aws:sns:us-east-1:983584755688:itt-sns-data-core-stage'
#             ,
#         'Subject': None,
#         'Message': '[{"source": "binance", "category": "price", "symbol": "ETH/BTC", "value": 0.077133, "timestamp": 1527835230.836},
#

def test_dynamodb(event, context):
    from boto3.dynamodb.conditions import Key, Attr
    dynamodb = boto3.resource('dynamodb')
    indicators_table = dynamodb.Table(os.environ['DYNAMODB_INDICATORS_TABLE'])
    #
    # for record in event['Records']['Sns']['Message']
    #     if record['category'] == "price":
    #         db_response = price_table.put_item(
    #             Item = {
    #                 'primary_key': "%s_%s_%d" % (data['source'], data['ticker'], data['unix_timestamp']),
    #                 'source': record['source'],
    #                 'ticker': record['symbol'],
    #                 'price': record['value'],
    #                 'unix_timestamp': int(parseFloat(record['timestamp']))
    #             }
    #         )

    db_response = indicators_table.get_item(
        Key={'primary_key': "special-string-key2"}
    )
    if "Item" in db_response:
        message = "it was already there"
    else: #create item in DynamoDB table
        db_response = indicators_table.put_item(
            Item = {
                'primary_key': "special-string-key2",
                'unix_timestamp': 1527760162,
            }
        )
        message = "we saved in the db"

    body = {"message": message,
            "db_response": str(db_response)
            }

    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }

    return response


def test_ta_lib(event, context):
    import numpy as np
    import talib

    close = np.random.random(100)
    output = talib.SMA(close)

    body = {
        "message": "Go Serverless v1.0! Your function executed successfully!",
        "input": event,
        "last_sma": output[-1],
    }

    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }

    return response

    # Use this code if you don't use the http event with the LAMBDA-PROXY
    # integration
    """
    return {
        "message": "Go Serverless v1.0! Your function executed successfully!",
        "event": event
    }
    """


def calcBollingerBands(event, context):
    import numpy as np
    import talib

    # note that all ndarrays must be the same length!
    inputs = {
        'open': np.random.random(100),
        'high': np.random.random(100),
        'low': np.random.random(100),
        'close': np.random.random(100),
        'volume': np.random.random(100)
    }

    from talib.abstract import BBANDS

    upper, middle, lower = BBANDS(inputs, 20, 2, 2)

    return {
        "statusCode": 200,
        "BBANDS": json.dumps({
            'upper': list(upper),
            'middle': list(middle),
            'lower': list(lower)
        })
    }
