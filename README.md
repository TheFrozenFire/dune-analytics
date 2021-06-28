# Dune Analytics

This is a client which leverages the GraphQL interface supplied by Dune Analytics to run queries and load their results
into memory. This can be used to, for instance, pull data from DA into a Pandas DataFrame, for more complex analysis.

> **Disclaimer:** This package is not in any respect developed or endorsed by Dune Analytics, nor is the maintainer
> associated with Dune Analytics in any way.

[![pyversion][pyversion-image]][pyversion-url]
[![pypi][pypi-image]][pypi-url]

Tip me at [0xD660994dfD06A7d33C779E77bBd7D71b3C9C6AeA](https://etherscan.io/address/0xD660994dfD06A7d33C779E77bBd7D71b3C9C6AeA)

## Installation

    $ pip install --pre dune_analytics
    
## Usage

### Basic Usage

```python
from dune_analytics import Dune

dune = Dune(dune_username, dune_password)

results = dune.query('''
    SELECT
        *
    FROM tornado_cash."eth_call_withdraw"
    LIMIT 100
''')
```

## License

[MIT License](https://github.com/thefrozenfire/dune-analytics/blob/master/LICENSE)

