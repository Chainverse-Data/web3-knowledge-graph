# Prop house scraper

This scraper utilizes the internal the graph hosted service by prop house to run queries to get all the prop house DAOs auctions and proposals.


# Data

```json
{
    "communities": [<communities Object>],
    "auctions": [<auctions Object>],
    "proposals": [<proposals Object>],
    "votes": [<votes Object>],
}
```

## communities Object

```json
{
    "id": 3,
    "contractAddress": "0xe169c2ed585e62b1d32615bf2591093a629549b6",
    "name": "NounPunks",
    "profileImageUrl": "https://i.imgur.com/nfPXi3e.jpg",
    "description": "A Nouns DAO funded house for the NounPunks community. Builders can apply with any idea to get funded.",
    "auctions": [
    {
        "title": "Round 1",
        "id": 5
    },
    {
        "title": "Round 2",
        "id": 11
    }
    ],
    "createdDate": "2022-05-19T13:55:41.003Z"
}
```

## auctions Object

```json
{
    "id": 5,
    "title": "Round 1",
    "startTime": "2022-05-31T22:00:00.000Z",
    "proposalEndTime": "2022-06-07T22:00:00.000Z",
    "fundingAmount": 2,
    "currencyType": "ETH",
    "description": "An open round where builders are encouraged to propose any idea they believe will benefit the NounPunks ecosystem. Anyone with an ETH address is welcome to propose. Once the proposing period is complete, NounPunks holders will vote on their favorite proposals. The proposals with the most votes will get funded. ",
    "numWinners": 1,
    "community": {
    "id": 3
    },
    "createdDate": "2022-05-19T14:09:58.634Z"
}
```

## proposals Object

```json
{
    "address": "0xA74b4fEeFc120BE0864E702E73FBc24eCab197e3",
    "id": 383,
    "title": "NounPunks.Nouns.Blog",
    "tldr": "Sponsor Nouns.Blog + Create a Blog for NounPunks",
    "createdDate": "2022-06-07T08:16:03.448Z",
    "winner": false,
    "communityId": 3,
    "auctionId": 5
}
```

## votes Object

```json
{
    "address": "0xb4E269d95Fcb15De22a6A378620c4d208c98Ad62",
    "id": 1304,
    "direction": 1,
    "weight": 1,
    "createdDate": "2022-06-07T23:18:18.164Z",
    "proposal": 383
}
```


