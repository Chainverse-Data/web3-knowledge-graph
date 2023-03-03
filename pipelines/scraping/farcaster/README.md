# Farcaster Scraper

This scraper reads from the the Farcaste TheGraph endpoint: `https://api.thegraph.com/subgraphs/name/0xsarvesh/farcaster-goerli` to retrieve the farcaster user informations.

# Data

```json
{
    "users": {<user Object>}
}
```

## user Object example

```json
{
    "id": "0x0011b62ccf7b61ce8940555197c1e214e15158b4",
    "address": "0x0011b62ccf7b61ce8940555197c1e214e15158b4",
    "fname": "castrike",
    "fId": "8176",
    "url": "http://www.farcaster.xyz/u/castrike.json"
}
```

# Bucket

This scraper reads and writes from the following bucket: `farcaster`

# Metadata

The scraper does not reads and writes metadata variables.

# Flags

No unique flags created
