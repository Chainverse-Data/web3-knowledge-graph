# Snapshot Ingestor

This ingestor will take care of data obtained by the Snapshot scraper.

# Ontology

Nodes:

- EntitySnapshotSpace:Snapshot:Space:Entity
- Snapshot:Proposal:ProposalSnapshot
- Wallet
- Twitter:Account

Edges:

- (Space)-[HAS_PROPOSAL]->(Proposal)
- (Wallet)-[AUTHOR]->(Proposal)
- (Wallet)-[CONTRIBUTOR]->(Space)
- (Space)-[HAS_ALIAS]->(Alias)
- (Space)-[HAS_ACCOUNT]->(Twitter)
- (Wallet)-[VOTED]->(Proposal)
- (Space)-[HAS_STRATEGY]->(Token)

# Bucket

This ingestor reads and writes from the following bucket: `snapshot`

# Metadata

No unique metadata created

# Flags

No unique flags created
