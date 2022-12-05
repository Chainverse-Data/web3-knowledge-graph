# Twitter Ens Scraper

This scraper reads from the ETH Leaderboards API to retrieve twitter-ens account relationships.

# Data

```json
{
    "accounts": [<account Object>]
}
```

## account Object example

```json
{
  "ens": "fabien.eth",
  "handle": "Fabien",
  "address": "0xasdfasdfasdfasdf"
}
```

# Bucket

This scraper reads and writes from the following bucket: `twitter-ens`

# Metadata

The scraper reads and writes the following metadata variables:

- last_account_offset

# Flags

No unique flags created
