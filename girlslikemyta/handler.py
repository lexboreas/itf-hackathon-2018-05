import os
import json
import boto3


def test_dynamodb(event, context):
    from boto3.dynamodb.conditions import Key, Attr
    dynamodb = boto3.resource('dynamodb')
    indicators_table = dynamodb.Table(os.environ['DYNAMODB_INDICATORS_TABLE'])

    try:
        db_response = indicators_table.get_item(
            Key={'primary_key': "special-string-key"}
        )
        message = "it was already there"
    except:
        db_response = indicators_table.put_item(
            Item = {
                'primary_key': "special-string-key",
                'unix_timestamp': 1527760162,
                'created_at': str(int(time.mktime((datetime.now()).timetuple()))),
                'modified_at': str(int(time.mktime((datetime.now()).timetuple()))),
            }
        )
        message = "pretty sure we saved in the db"

    body = {"message": message,
            "db_response": json.dumps(db_response)
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
