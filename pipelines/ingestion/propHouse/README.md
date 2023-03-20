# NFTfi Ingestor

This ingestor will take care of data obtained by the NFTfi scraper.

# Ontology

Nodes:

- PropHouse:Entity
- PropHouse:Auction
- PropHouse:Proposal
- PropHouse:Proposal:Winner: If that proposal won a vote
- Wallet:Account

Edges:

- (Auction)-[r:HAS_AUCTION]->(Entity)
- (Auction)-[r:HAS_PROPOSAL]->(Proposal)
- (Wallet)-[r:AUTHOR]->(Proposal)
- (community)-[r:HAS_PROPOSAL]->(Proposal)
- (Wallet)-[v:VOTED]->(Proposal)

# Bucket

This ingestor reads and writes from the following bucket: `nftfi`

# Metadata

No unique metadata created

# Flags

No unique flags created
