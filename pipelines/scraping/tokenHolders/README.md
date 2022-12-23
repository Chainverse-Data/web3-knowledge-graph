# Token Holders Scraper

This scraper reads from the Alchemy API to retrieve the tokens that walelts in the Neo4J have interacted with, and recovers the balances for each wallet for a given token.

# Data

```json
{
    "balances": {<balance Object>},
    "assets": {<asset Object>},
    "tokens": {<token Object>},
}
```

## balance Object example
Balance objects are key value pairs, with the key being the wallet address.
```json
{
  "0x326b4bc6af0d13bb50785dd042b4e70aef2d71be": [
        {
            "contractAddress": "0x5afe3855358e112b5647b952709e6165e1c1eeee",
            "tokenBalance": "0x00000000000000000000000000000000000000000000000579a814e10a740000"
        },
        {
            "contractAddress": "0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85",
            "tokenBalance": "0x0000000000000000000000000000000000000000000000000000000000000001"
        },
        {
            "contractAddress": "0x6b175474e89094c44da98b954eedeac495271d0f",
            "tokenBalance": "0x0000000000000000000000000000000000000000000000000000000000000000"
        },
        {
            "contractAddress": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            "tokenBalance": "0x00000000000000000000000000000000000000000000000000000000000007a5"
        },
        {
            "contractAddress": "0x030ba81f1c18d280636f32af80b9aad02cf0854e",
            "tokenBalance": "0x000000000000000000000000000000000000000000000000002e4a82ce4a0a99"
        }
    ],
    ...
}
```

## asset Object example
Asset objects are key value pairs, with the key being the wallet address.

```json
{
  "0xde7105f03b6dcbc9a4eb4336786aa61f4156c4ff": [
            "0x5de0f93f3d1bf45bb632a59c2fe38bdea74e0a81",
            "0x5bf5bcc5362f88721167c1068b58c60cad075aac",
            "0x2face815247a997eaa29881c16f75fd83f4df65b",
            "0xf1b99e3e573a1a9c5e6b2ce818b617f0e664e86b",
            "0xc1c8c49b0405f6cffba5351179befb2d8a2c776c",
            "0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85",
            "0x1982b2f5814301d4e9a8b0201555376e62f82428",
            "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            "0xc36442b4a4522e871399cd717abdd847ab11fe88",
            "0x619beb58998ed2278e08620f97007e1116d5d25b",
            "0xae7ab96520de3a18e5e111b5eaab095312d7fe84",
            "0xa3b61c077da9da080d22a4ce24f9fd5f139634ca",
            "0x0d3716e3e411af431a6e87e715d4b05bbcd67000",
            "0xf1f3ca6268f330fda08418db12171c3173ee39c9",
            "0xc9a42690912f6bd134dbc4e2493158b3d72cad21"
        ],
        ...
}
```

## tokens Object query
Asset objects are key value pairs, with the key being the token contract address.

```json
{
    "0x532e5efded726c6dde5228f35d805f531c14b0eb": {
            "contractType": "erc721",
            "symbol": "NFFCC",
            "decimal": null
    },
    ...
}
```

# Bucket

This scraper reads and writes from the following bucket: `token-holders`

# Metadata

The scraper reads and writes the following metadata variables:

- wallets_last_block: The recorded last block for every wallet. It is a dictionary of key value pairs, the keys are the wallet address and the value the last block scrapped.

# Flags

No unique flags created
