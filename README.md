# DYDX Trading Model

This repository contains various scripts and models to submit automated orders on the dydx exchange.

The project creates multiple limit orders above and below the real-time price, based on predicitons made by a machine-learning model trained on historical data.

- main.py --> Primary real-time script for submitting orders
- model.py --> pickled XGBoost model 

Before running, install the python package:

```pip install dydx-v4-client```

And then can be run with the following in a background process:

```python3 main.py```

References:

https://dydx.trade/trade/BTC-USD

https://github.com/dydxprotocol/v4-clients/tree/main/v4-client-py-v2

https://docs.dydx.exchange/api_integration-indexer/indexer_api
