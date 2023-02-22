# Unlock Scraper

This scraper reads from the TheGraph API to retrieve the managers, lock, holders, and keys.

# Bucket

This scraper reads and writes from the following bucket: `unlock`

# Metadata

The scraper reads and writes the following metadata variables:
- cutoff_block: Sets the offset for reading only new locks

# Flags

No unique flags created

# Data

```json
{
    "locks": [<lock Object>],
    "managers": [<manager Object>],
    "keys": [<key Object>],
    "holders": [<holder Object>]
}
```

## lock Object example
```json
{
    "locks": [
        
        {
            "address": "0xfaa342eeedaf766f28d74dac08dd37848c625c88",
            "id": "0xfaa342eeedaf766f28d74dac08dd37848c625c88",
            "name": "DLT HUB MEMBERSHIP",
            "tokenAddress": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            "creationBlock": "16333982",
            "price": "120000000",
            "expirationDuration": "2592000",
            "totalSupply": "0",
            "network": "mainnet"
        }
    ]
}
```

## manager Object example
```json
{
    "managers": [
        
        {
            "lock": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            "address": "0x08ac3ae8ab452823597726f73498df9e3d1680db"
        }
    ]
}
```

## key Object example
```json
{
    "keys": [
        
        {
            "id": "0xdf00944c0f24b3a972f06634b11f964108ad949e-2",
            "address": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            "expiration": "1705402235",
            "tokenURI": "https://locksmith.unlock-protocol.com/api/key/0xdf00944c0f24b3a972f06634b11f964108ad949e/2",
            "createdAt": "1673866235",
            "network": "mainnet"
        }
    ]
}
```

## holder Object example
```json
{
    "holders": [
        {
            "id": "0x38eed3cceed88f380e436eb21811250797c453c5",
            "address": "0x38eed3cceed88f380e436eb21811250797c453c5",
            "keyId": "0xdf00944c0f24b3a972f06634b11f964108ad949e-2",
            "tokenAddress": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
        }
    ]
}
```