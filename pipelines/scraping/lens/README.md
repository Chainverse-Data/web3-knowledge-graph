# Farcaster Scraper

This scraper reads from the the Lens TheGraph endpoint: `https://api.thegraph.com/subgraphs/name/anudit/lens-protocol` to retrieve the lens user informations.

# Data

```json
{
    "profiles": {<profile Object>}
}
```

## profile Object example

```json
{
  "id": "50",
  "profileId": "50",
  "creator": "0x1eec6eccaa4625da3fa6cd6339dbcc2418710e8a",
  "owner": "0x5d8b04e983a2f83174530a3574e89f42e5ee066e",
  "handle": "modene.lens",
  "createdOn": "1652880064"
}
```

# Bucket

This scraper reads and writes from the following bucket: `lens`

# Metadata

The scraper does not reads and writes metadata variables.

# Flags

No unique flags created
