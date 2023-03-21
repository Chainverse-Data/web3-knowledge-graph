# Mirror NFT Token holders

This pipeline scrapes the Optimism Mirror NFT holders.

# Ontology

Nodes:
- (Mirror:Token:ERC721)
- (Wallet:Account)

Edges:
- (Wallet)-[HOLDS_TOKEN]-(Mirror)