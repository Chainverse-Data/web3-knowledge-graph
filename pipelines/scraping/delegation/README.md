# Delegation Scraper

This scraper reads from the Graph to retrieve all delegation events happening for a set of defined and hard coded protocols.

# Protocols
- ampleforth
- compound
- cryptex
- gitcoin
- idle
- indexed
- pooltogether
- radicle
- reflexer
- uniswap
- unslashed

# Data

```json
{
  "delegateChanges": [<delegateChanges Object>],
  "delegateVotingPowerChanges": [<delegateVotingPowerChanges Object>],
  "delegates": [<delegates Object>],
  "tokenHolders": [<tokenHolders Object>]
}
```

## delegateChanges Object

```json
{
  "id": "1620323799-286",
  "tokenAddress": "0x77fba179c79de5b7653f68b5039af940ada60ce0",
  "delegator": "0xfe2321d7dfa492dfc39330e8b85e7c49161e7f98",
  "delegate": "0xafbc9c281f953d683a2ebb90a70dd4a45af52ee3",
  "previousDelegate": "0x0000000000000000000000000000000000000000",
  "txnHash": "0x7aab571ce018ff5adcf57a3cde8cf93783c626d7aa9b9cf150bc28063a1c85b4",
  "blockNumber": "12382204",
  "blockTimestamp": "1620323799",
  "protocol": "ampleforth"
}

```

## delegateVotingPowerChanges Object

```json
{
  "id": "1620323799-287",
  "blockTimestamp": "1620323799",
  "blockNumber": "12382204",
  "delegate": "0xfe2321d7dfa492dfc39330e8b85e7c49161e7f98",
  "tokenAddress": "0x77fba179c79de5b7653f68b5039af940ada60ce0",
  "previousBalance": "0",
  "newBalance": "890500000000000000000000",
  "logIndex": "287",
  "txnHash": "0x7aab571ce018ff5adcf57a3cde8cf93783c626d7aa9b9cf150bc28063a1c85b4",
  "protocol": "ampleforth"
}
```

## delegates Object

```json
{
  "id": "0x0157ce36a6a3c309fc4bae89c978c6d36322c872",
  "delegatedVotesRaw": "0",
  "delegatedVotes": "0",
  "tokenHoldersRepresented": 
  [
    {
      "id": "0x3dd9bc86880a0678df824df117dbc7d577c471c4"
    },
    {
      "id": "0xfe2321d7dfa492dfc39330e8b85e7c49161e7f98"
    }
  ],
  "numberVotes": 0,
  "protocol": "ampleforth"
 }

```

## tokenHolders Object

```json
{
  "id": "0x0000000000000000000000000000000000000000",
  "tokenBalance": "1102.855440669392376983",
  "tokenBalanceRaw": "1102855440669392376983",
  "totalTokensHeld": "1102.855440669392376983",
  "totalTokensHeldRaw": "1102855440669392376983",
  "protocol": "ampleforth"
 }

```

# Bucket

This scraper reads and writes from the following bucket: `delegation`

# Metadata

None

# Flags

No unique flags created
