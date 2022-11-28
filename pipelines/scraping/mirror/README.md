# Mirror Scraper

This scraper reads from the arweave API and Optimism Mirror Factory Contract to retrieve the Mirror articles, NFTs and contributors. 

# Bucket

This scraper reads and writes from the following bucket: `mirror`

# Metadata

The scraper reads and writes the following metadata variables:
- optimism_start_block: Sets the starting block for reading the Mirror Factory contract
- start_block: Sets the starting block for reading arweave transactions

# Flags

No unique flags created

# Data

```json
{
    "NFTs": [<NFT Object>],
    "articles": [<Article Object>],
    "arweave_transactions": [<Transaction Object>],
    "arweave_articles": [<Article Object>],
    "arweave_nfts": [<NFT Object>],
    "twitter_accounts": [<Twitter Object>]
}
```

## NFT Object example
```json
{
    "original_content_digest": "nulKkbFcF5EbnpyIRYXSBfcZWWi8W1AH_uMnuPEpiRY",
    "chain_id": 10,
    "funding_recipient": "0xCD0e394639B2D0b159B41F9dBe0583C33d85e874",
    "owner": "0xCD0e394639B2D0b159B41F9dBe0583C33d85e874",
    "address": "0xCD0e394639B2D0b159B41F9dBe0583C33d85e874",
    "supply": 500,
    "symbol": "SYMBOL",
}


```

## Article Object example
```json
{
    "original_content_digest": "nulKkbFcF5EbnpyIRYXSBfcZWWi8W1AH_uMnuPEpiRY",
    "current_content_digest": "nulKkbFcF5EbnpyIRYXSBfcZWWi8W1AH_uMnuPEpiRY",
    "arweaveTx": "ZhLFHwnK0iSTLfRhMDQGfVVirOz8RuAJWa3pT9N4Lig",
    "body": "The content of the article!",
    "title": "The cool title",
    "timestamp": 1669420527,
    "author": "0xCD0e394639B2D0b159B41F9dBe0583C33d85e874",
    "ens": "mycoolname.eth",
}

```

## Transaction Object example
```json
{
    "transaction_id": "ZhLFHwnK0iSTLfRhMDQGfVVirOz8RuAJWa3pT9N4Lig",
    "author": "0xCD0e394639B2D0b159B41F9dBe0583C33d85e874",
    "content-digest": "nulKkbFcF5EbnpyIRYXSBfcZWWi8W1AH_uMnuPEpiRY",
    "original_content_digest": "nulKkbFcF5EbnpyIRYXSBfcZWWi8W1AH_uMnuPEpiRY",
    "block": 456373,
    "timestamp": 1669420527,
}

```

## Twitter Object example
```json
{
    "original_content_digest": "nulKkbFcF5EbnpyIRYXSBfcZWWi8W1AH_uMnuPEpiRY",
    "twitter_handle": "coolHandle",
    "mention_count": 2
} 

```