import json


def test_ta_lib(event, context):
    import numpy
    import talib

    close = numpy.random.random(100)
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
