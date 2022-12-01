# DAOHaus Scraper

This scraper reads from the theGraph to retrieve the all of DAOHaus information from the superGraph.

# Bucket

This scraper reads and writes from the following bucket: `daohaus`

# Metadata

The scraper reads and writes the following metadata variables:
- last_grant_offset: Sets the offset for reading only new grants
- last_bounty_offset: Sets the offset for reading only new bounties
- last_block_number: : Sets the offset for reading only new blocks

# Flags

No unique flags created

# Data

```json
{
    "daoMetas": [<daoMetas Object>],
    "moloches": [<moloches Object>],
    "tokenBalances": [<tokenBalances Object>],
    "votes": [<votes Object>],
    "members": [<members Object>],
    "proposals": [<proposals Object>]
}
```

## daoMetas Object example
```json
{
            "id": "0x0013bb51047920cbe0e17e36077267126d76225b",
            "title": "The super DAO",
            "version": "2.1",
            "newContract": "1",
            "http": null
        },

```

## moloches Object example
```json
{
    "id": "0x0bf1971d43a7f0a835c295249d53021c1692f59a",
    "version": "2.1",
    "summoner": "0x70ee1166e52f227ac161570e02a75d68f6e3c92a",
    "newContract": "1",
    "summoningTime": "1614799365",
    "createdAt": "1614799365",
    "periodDuration": "86400",
    "votingPeriodLength": "7",
    "gracePeriodLength": "7",
    "proposalDeposit": "1000000000000000000000",
    "approvedTokens": [
        {
            "id": "0x0bf1971d43a7f0a835c295249d53021c1692f59a-token-0xb0c5f3100a4d9d9532a4cfd68c55f1ae8da987eb",
            "tokenAddress": "0xb0c5f3100a4d9d9532a4cfd68c55f1ae8da987eb",
            "whitelisted": true,
            "symbol": "HAUS",
            "decimals": "18"
        },
        {
            "id": "0x0bf1971d43a7f0a835c295249d53021c1692f59a-token-0xe91d153e0b41518a2ce8dd3d7944fa863463a97d",
            "tokenAddress": "0xe91d153e0b41518a2ce8dd3d7944fa863463a97d",
            "whitelisted": true,
            "symbol": "WXDAI",
            "decimals": "18"
        }
    ],
    "guildBankAddress": "0xb0c5f3100a4d9d9532a4cfd68c55f1ae8da987eb",
    "guildBankBalanceV1": "0",
    "tokens": [
        {
            "id": "0x0bf1971d43a7f0a835c295249d53021c1692f59a-token-0xb0c5f3100a4d9d9532a4cfd68c55f1ae8da987eb",
            "tokenAddress": "0xb0c5f3100a4d9d9532a4cfd68c55f1ae8da987eb",
            "whitelisted": true,
            "symbol": "HAUS",
            "decimals": "18"
        },
        {
            "id": "0x0bf1971d43a7f0a835c295249d53021c1692f59a-token-0xe91d153e0b41518a2ce8dd3d7944fa863463a97d",
            "tokenAddress": "0xe91d153e0b41518a2ce8dd3d7944fa863463a97d",
            "whitelisted": true,
            "symbol": "WXDAI",
            "decimals": "18"
        }
    ],
    "totalLoot": "0",
    "totalShares": "1530"
}

```

## tokenBalances Object example
```json
{
    "id": "0x0013bb51047920cbe0e17e36077267126d76225b-token-0x6b175474e89094c44da98b954eedeac495271d0f-member-0x000000000000000000000000000000000000beef",
    "moloch": {
        "id": "0x0013bb51047920cbe0e17e36077267126d76225b"
    },
    "token": {
        "tokenAddress": "0x6b175474e89094c44da98b954eedeac495271d0f"
    },
    "tokenBalance": "0"
}
```

## votes Object example
```json
{
    "id": "0x4570b4faf71e23942b8b9f934b47ccedf7540162-member-0xd662fa474c0a1346a26374bb4581d1f6d3fb2d94-vote-527",
    "createdAt": "1669669091",
    "proposal": {
        "proposalId": "527"
    },
    "uintVote": 1,
    "molochAddress": "0x4570b4faf71e23942b8b9f934b47ccedf7540162",
    "memberAddress": "0xd662fa474c0a1346a26374bb4581d1f6d3fb2d94",
    "proposalIndex": "24",
    "memberPower": "1662"
}

```

## members Object example
```json
{
    "id": "0xefadfcb99106948c202216cc3fb11d03cd8801e8-member-0x0c46c3a7e0dbbc72038e072458e19ae83c1b440b",
    "createdAt": "1669876535",
    "memberAddress": "0x0c46c3a7e0dbbc72038e072458e19ae83c1b440b",
    "tokenTribute": "0",
    "molochAddress": "0xefadfcb99106948c202216cc3fb11d03cd8801e8",
    "shares": "10284",
    "loot": "0",
    "exists": true,
    "didRagequit": false,
    "kicked": false,
    "jailed": null
}

```

## proposals Object example
```json
{
    "createdAt": "1669891415",
    "createdBy": "0xa69ab783a864af92b4c425f45f54f85900ee51fb",
    "proposalIndex": "0",
    "proposalId": "0",
    "molochAddress": "0xefadfcb99106948c202216cc3fb11d03cd8801e8",
    "memberAddress": "0x0000000000000000000000000000000000000000",
    "applicant": "0xa69ab783a864af92b4c425f45f54f85900ee51fb",
    "proposer": "0xa69ab783a864af92b4c425f45f54f85900ee51fb",
    "sponsor": "0x0c46c3a7e0dbbc72038e072458e19ae83c1b440b",
    "processor": "0x0000000000000000000000000000000000000000",
    "sharesRequested": "5142",
    "lootRequested": "0",
    "tributeOffered": "500",
    "tributeToken": "0x783c68814126b66b9242c4c6538ae47db5e33169",
    "tributeTokenSymbol": "RODO",
    "tributeTokenDecimals": "2",
    "paymentRequested": "0",
    "paymentToken": "0x783c68814126b66b9242c4c6538ae47db5e33169",
    "paymentTokenSymbol": "WXDAI",
    "paymentTokenDecimals": "18",
    "startingPeriod": "1",
    "yesVotes": "0",
    "noVotes": "0",
    "sponsored": true,
    "sponsoredAt": "1669891775",
    "processed": false,
    "processedAt": "1669891775",
    "didPass": false,
    "cancelled": false,
    "cancelledAt": "1669891775",
    "aborted": false,
    "whitelist": false,
    "guildkick": false,
    "newMember": true,
    "trade": false,
    "details": "{\"title\":\"Team RODO\",\"description\":\"10% for RODO team\",\"proposalType\":\"Member Proposal\"}",
    "maxTotalSharesAndLootAtYesVote": "0",
    "yesShares": "0",
    "noShares": "0",
    "votingPeriodStarts": "1669962935",
    "votingPeriodEnds": "1670567735",
    "gracePeriodEnds": "1671172535",
    "uberHausMinionExecuted": false,
    "executed": false,
    "minionAddress": null,
    "isMinion": false,
    "minionExecuteActionTx": null
}

```
