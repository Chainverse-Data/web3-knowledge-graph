# Snapshot Scraper

This scraper reads from the Snapshot API to retrieve spaces, proposals, and votes.

# Data

```json
{
    "spaces": [<spaces Object>],
    "proposals": [<proposal Object>],
    "votes": [<vote Object>],
}
```

## space Object example

```json
{
  "id": "fabien.eth",
  "name": "Fabien",
  "about": "This is nothing more than a test space.",
  "avatar": "ipfs://QmcbtSnE5TpCjuQrKN4sxEh1Qv6B4RoKLx4aVgjtf8y3M5",
  "terms": null,
  "location": null,
  "website": "https://snapshot.org/#/fabien.eth",
  "twitter": "bonustrack87",
  "github": "bonustrack",
  "network": "1",
  "symbol": "POINT",
  "strategies": [
    {
      "name": "ticket",
      "params": {
        "value": 100,
        "symbol": "POINT"
      }
    }
  ],
  "admins": [
    "0xeF8305E140ac520225DAf050e2f71d5fBcC543e7",
    "0x4C7909d6F029b3a5798143C843F4f8e5341a3473"
  ],
  "members": ["0xeF8305E140ac520225DAf050e2f71d5fBcC543e7"],
  "filters": {
    "minScore": 1,
    "onlyMembers": false
  },
  "plugins": {
    "safeSnap": {
      "safes": [
        {
          "network": "4",
          "realityAddress": "0xF8Ca72b0d427F0b3e8984133C8A6860153b4f2D8"
        }
      ]
    }
  }
}
```

## proposal Object example

```json
{
  "id": "0xee91d83d4d99b47725fa8a541812f441096e3773b4cac2752e218728d74ac690",
  "ipfs": "bafkreidxylojkwfc7akk77vanrurzl5vse6eokme6wdoj6pznlp5pg5haq",
  "author": "0x365A9a61191F249Ba4f0d21263852Da78D6FAd7c",
  "created": 1665462283,
  "space": {
    "id": "sarah8.eth",
    "name": "Sarah"
  },
  "network": "1",
  "type": "single-choice",
  "strategies": [
    {
      "name": "ticket",
      "params": {
        "symbol": "VOTE"
      }
    },
    {
      "name": "erc20-balance-of",
      "params": {
        "symbol": "USDC",
        "address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "decimals": 6
      }
    }
  ],
  "plugins": {},
  "title": "《红楼梦》适合现代人读吗？",
  "body": "除了高中语文课文上的红楼梦，现代人有读过红楼梦的吗？\n\n还值得我们去读吗？",
  "choices": [
    "传世之作，不读妄为中国人",
    "大部头，读不下去，浪费时间",
    "一直想读，一直耽搁着，救救孩子"
  ],
  "start": 1665462241,
  "end": 1665721441,
  "snapshot": "15722454",
  "state": "active",
  "link": "https://snapshot.org/#/sarah8.eth/proposal/0xee91d83d4d99b47725fa8a541812f441096e3773b4cac2752e218728d74ac690"
}
```

## vote Object query

```json
{
  "id": "0xf819d7ade01db63db6e7456ca90a735930c5c544618bdc303f5fbb844d09dcf1",
  "ipfs": "bafkreigwjumeooncurumwl2th422szckxzfkbwxf6hhd7g73lgulg32ram",
  "voter": "0xd34Fd31Aed8672fb623FF7B86776F80081C8b760",
  "created": 1665463010,
  "choice": 1,
  "proposal": {
    "id": "0x6cb7810a9fc1bce8712cd529e4a11a5d03093312afe93540617be4dce62f192f",
    "choices": ["For", "Against", "Abstain"]
  },
  "space": {
    "id": "jbdao.eth",
    "name": "JuiceboxDAO"
  }
}
```

# Bucket

This scraper reads and writes from the following bucket: `snapshot`

# Metadata

The scraper reads and writes the following metadata variables:

- last_timestamp Sets the last execution time as cutoff

# Flags

No unique flags created
