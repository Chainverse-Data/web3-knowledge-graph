# Unlock Ingestor

This ingestor will take care of data obtained by the Unlock scraper.

# Ontology


Nodes:
Lock:
    lock:Nft:ERC721:Lock
        Properties:
            address
            name
            price
            createdDt
            lastUpdateDt    
            ingestedBy
            uuid
Key:
    key:Nft:ERC721:Instance
        Properties:
            contractAddress
            tokenId
            createdAt
            expiration
            tokenUri
            network
            createdDt
            lastUpdateDt
            ingestedBy
            uuid
Manager and Holder:
    wallet:Wallet
        Properties:
            address
            createdDt
            lastUpdateDt
            ingestedBy
            uuid


Edges:
Manager to Lock:
    (wallet)-[CREATED]->(lock)
        Properties:
            createdDt
            lastUpdateDt
Lock to Key:
    (lock)-[HAS_KEY]->(key)
        Properties:
            createdDt
            lastUpdateDt
Holder to Lock:
    (wallet)-[HOLDS]->(lock)
        Properties:
            createdDt
            lastUpdateDt
Holder to Key:
    (wallet)-[HOLDS_INSTANCE]->(key)
        Properties:
            createdDt
            lastUpdateDt


# Bucket

This ingestor reads and writes from the following bucket: `unlock`

# Metadata

No unique metadata created

# Flags

No unique flags created