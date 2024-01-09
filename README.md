# GET & POST Data Methods

This package is dedicated to interact with the API by posting and getting pandas.DataFrame format

## POST Trading Idea

### Method

upload_trading_idea

### Inputs

- data  - pandas DataFrame composed of                                                        (pandas.DataFrame)
    - title           Title of the Trading Idea                                               (str)
    - author          Author of the Trading Idea                                              (str)
    - signal          Signal type i.e. long or short                                          (str)
    - tag             Analysis Tag e.g. Chart Pattern, Trend Analysis, ...                    (str)
    - account_type    Account type of the Author e.g. Premium, Plus, ...                      (str)
    - label           Trading Idea label #                                                    (str)
    - code            Asset code name                                                         (str)
    - region          Region\Language of the Author e.g. Worlwide\English, Spanish, etc...    (str)
    - market          Type of the traiding asset e.g. stock, index, crypto, forex, etc...     (str)
    - description     Signal full description                                                 (str)
    - timeframe       Trading Idea timeframe (in second)                                      (int)
    - timestamp       Post date in Unix timestamp rounded to second                           (int)
    - likes           # of likes received by the idea                                         (int)
    - comments        # of comments received by the idea                                      (int)
    - date            Post date in datetime format                                            (int)
    - url             URL to the (Trading View) Trading Idea                                  (str)

where the list above is the column name and type expected to feed the upload_trading_idea function.
The ``date'' columns is expected to be a string in YYYY-MM-DD HH:MM[:ss[.uuuu]] in GMT 0 / UCT timezone.