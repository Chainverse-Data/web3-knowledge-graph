# Unlock Ingestor

This ingestor will take care of data obtained by the Unlock scraper.

# Ontology


# Nodes 

## Lock 
*Lock is the Unlock Protocol name for an NFT-based subscription.*


### Labels

`(lock:Nft:ERC721:Lock)`


### Properties 

- `address` - contract address for the lock
- `name` - name given to the lock by the creator of the lock 
- `price` - price of `Keys`, or subscriptions, for the lock 
- `createDt`
- `lastUpdateDt`
- `ingestedBy`
- `uuid`

## Key 
An individual subscription, associated with a lock.

### Labels
`(key:Nft:ERC721:Instance)`

### Properties 
- `contractAddress` - contract address for the lock 
- `tokenId` 
- `createAt` - date NFT was minted, not date node was created 
- `expiration` when the subscription expires
- `tokenUri` - link to token metadata 
- `network` - i/e which chain, Polygon, Arbitrum
- `ingestedBy`
- `createdDt`
- `uuid`


- 
## Manager and Holder
Manger created the lock. Holder holds key issued by Lock.

### Labels 
`(wallet:Wallet)`

### Properties 
- `address`
- `createdDt`
- `lastUpdatedDt`
- `ingestedBy`
- `uuid`

## Relationships

**1. Lock manager**

*Pattern*
`(wallet:Wallet)-[r:CREATED]->(lock:Lock)

*Properties*
- `createdDt`
- `lastUpdateDt`


**2. Lock to key**

*Pattern*
`(lock)-[HAS_INSTANCE]->(key)`

*Properties*
- `createdDt`
- `lastUpdateDt`

**3. Holder to Lock**

*Pattern*
`wallet)-[:HOLDS]->(lock)`

*Properties*
- `createdDt`
- `lastUpdateDt` 
- `numericBalance`

**4. Hold to key**

*Pattern*
`(wallet)-[:HOLDS_INSTANCE]->(key)`

*Properties*
- `createdDt`
- `lastUpdateDt`

# Bucket

This ingestor reads and writes from the following bucket: `unlock`

# Metadata

No unique metadata created

# Flags

No unique flags created
