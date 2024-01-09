import requests

import pandas as pd

from typing     import Union
from datetime   import datetime



#url_base    = 'http://44.221.198.180:8000/'
url_base    = "http://127.0.0.1:8000/"

#######################################################################################################################
"""

"""
#######################################################################################################################


def verify_and_get_iso_time(time, name):
    assert time is None or isinstance(time, str) or isinstance(time, datetime), f"{name} should be None or a string time or datetime object"

    if isinstance(time, datetime):
        return time.isoformat(timespec="second") + "Z"
    
    return time

def get_url(url: str, code: Union[None, str], start_time: Union[None, str], end_time: Union[None, str]):
    # Build GET API URL
    if not code is None:
        url = f"{url}/{code}/"
        if not start_time is None:
            url = f"{url}/{start_time}/"
            if not end_time is None:
                url = f"{url}/{end_time}/"
    
    return url

def init_get(url, code = None, start_time = None, end_time = None):
    # Assert that the code asset name is a string or None
    assert code is None or isinstance(code, str), "code should be None or a string represening a stock name or a currency pair"
    
    # Verify datetime format
    start_time  = verify_and_get_iso_time(start_time, "start_time")
    end_time    = verify_and_get_iso_time(end_time, "end_time")

    # Build GET API URL
    url = get_url(url, code, start_time, end_time)

    return requests.get(url)


#######################################################################################################################
"""
Price Timeseries GET & POST
"""
#######################################################################################################################


def get_price_timeseries(code = None, start_time = None, end_time = None):
    """
    GET method to pull data price timeseries from the data base
    Inputs: - code          - Code name of the Asset                        (str or None)
            - start_time    - time from which we want the data to start     (str or None)
            - end_time      - time at which we want the data to end         (str or None)
    """

    # Call the API and Get price timeseries
    response    = init_get(url_base + "price", code, start_time, end_time)
    # If the call went well, transform the json data into pandas DataFrame
    if response.status_code == 200:
        data    = pd.DataFrame(response.json())                 # Get oandas DataFrame
        data["close"]       = data["close"].astype(float)       # Check close price is float
        data["timestamp"]   = pd.to_datetime(data["timestamp"]) # Check timestamp is datetime

        # Return the data as a pandas DataFrame
        return data
    
    # Return status code if there is an error
    return response.status_code

def upload_price(data: pd.DataFrame):
    """
    POST method to push data price timeseries to the data base
    Inputs: - data  - pandas DataFrame composed of                                  (pandas.DataFrame)
                                                    - close         close price     (float)
                                                    - timestamp     UCT datetime    (str)
                                                    - code          asset code name (str)
    """

    # Assert the DataFrame structure
    assert isinstance(data, pd.DataFrame), "data should be a DataFrame"
    for c in ["close", "timestamp", "code"]:
            assert c in data.columns, f"{c} should ba column in the DataFrame"

    # Convert timestamp to UTC
    data['timestamp']   = pd.to_datetime(data['timestamp']).dt.tz_localize('UTC').dt.strftime("%Y-%m-%d %H:%M:%S")  # + "TZ"

    print(data)
    # Extract Json data from DataFrame
    data    = data.to_dict(orient='records')

    # POST the data
    return requests.post(f"{url_base}price/upload/", json=data, headers={'Content-Type': 'application/json'})

#######################################################################################################################
"""
Trading Ideas GET & POST
"""
#######################################################################################################################


def get_trading_idea(code = None, start_time = None, end_time = None):
    """
    GET method to pull data price timeseries from the data base
    Inputs: - code          - Code name of the Asset                        (str or None)
            - start_time    - time from which we want the data to start     (str or None)
            - end_time      - time at which we want the data to end         (str or None)
    """

    # Call the API and Get price timeseries
    response    = init_get(url_base + "trading-idea", code, start_time, end_time)
    # If the call went well, transform the json data into pandas DataFrame
    if response.status_code == 200:
        data    = pd.DataFrame(response.json())
        if len(data) > 0:
            data["date"]   = pd.to_datetime(data["date"])   # Check timestamp is datetime
            # Convert necessary columns to integer
            for k in ["timeframe", "likes", "comments", "timestamp"]:
                data[k] = data[k].astype(int)

        # Return the data as a pandas DataFrame
        return data
    
    # Return status code if there is an error
    return response.status_code

def upload_trading_idea(data: pd.DataFrame):
    """
    POST method to push trading ideas to the data base
    Inputs: - data  - pandas DataFrame composed of                                                                                          (pandas.DataFrame)
                                                    title           Title of the Trading Idea                                               (str)
                                                    author          Author of the Trading Idea                                              (str)
                                                    signal          Signal type i.e. long or short                                          (str)
                                                    tag             Analysis Tag e.g. Chart Pattern, Trend Analysis, ...                    (str)
                                                    account_type    Account type of the Author e.g. Premium, Plus, ...                      (str)
                                                    label           Trading Idea label #                                                    (str)
                                                    code            Asset code name                                                         (str)
                                                    region          Region\Language of the Author e.g. Worlwide\English, Spanish, etc...    (str)
                                                    market          Type of the traiding asset e.g. stock, index, crypto, forex, etc...     (str)
                                                    description     Signal full description                                                 (str)
                                                    timeframe       Trading Idea timeframe (in second)                                      (int)
                                                    timestamp       Post date in Unix timestamp rounded to second                           (int)
                                                    likes           # of likes received by the idea                                         (int)
                                                    comments        # of comments received by the idea                                      (int)
                                                    date            Post date in datetime format                                            (int)
                                                    url             URL to the (Trading View) Trading Idea                                  (str)
    """

    # Assert the DataFrame structure
    assert isinstance(data, pd.DataFrame), "data should be a DataFrame"
    for c in ["title", "author", "signal", "tag",
              "account_type", "label", "code", "region",
              "market", "description", "url", "date", "timeframe",
              "timestamp", "likes", "comments"]:
            assert c in data.columns, f"{c} should ba column in the DataFrame"

    # Convert timestamp to UTC
    try:
        data['date']   = pd.to_datetime(data['date']).dt.tz_localize('UTC').dt.strftime("%Y-%m-%d %H:%M:%S")    # + "TZ"
    except:
        pass
    print(data)
    # Extract Json data from DataFrame
    data    = data.to_dict(orient='records')

    # POST the data
    return requests.post(f"{url_base}trading-idea/upload/", json=data, headers={'Content-Type': 'application/json'})