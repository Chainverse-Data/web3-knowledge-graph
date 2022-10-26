# Snapshot Scraper

This scraper reads from the the GraphQL endpoint to get all gnosis multisigs on mainnet.

# Data

```json
{
    "multisig": [<multisig Object>],
}
```

## multisig Object example

```json
{
  "multisig": "0x655a9e6b044d6b62f393f9990ec3ea877e966e18",
  "address": "0xd28e1ae2c59f6d5b9dda9a12a1d3cc08015c2735",
  "threshold": 2,
  "occurDt": 1578576407
}
```

# Bucket

This scraper reads and writes from the following bucket: `multisig`

# Metadata

The scraper reads and writes the following metadata variables:

- last_timestamp Sets the last execution time as cutoff

# Flags

No unique flags created
