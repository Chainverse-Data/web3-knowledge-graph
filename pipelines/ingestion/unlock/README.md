# Unlock Ingestor

This ingestor will take care of data obtained by the Unlock scraper.

# Ontology


Nodes:
lockNode:Unlock:Lock
keyNode:Unlock:Key
managerNode:Unlock:Manager
holderNode:Unlock:Holder



Edges:
(managerNode)-[CREATED]->(lockNode)
(lockNode)-[HAS_KEY]->(keyNode)
(holderNode)-[HOLDS]->(lockNode)
(holderNode)-[HOLDS_INSTANCE]->(keyNode)


<!-- Nodes:
- EventGitCoinGrant:GitCoin:Grant:Event
- EventGitCoinBounty:GitCoin:Bounty:Event
- UserGitCoin:GitCoin:UserGitHub:GitHub:Account
- Wallet
- Twitter:Account

Edges:
- (UserGitCoin)-[MEMBER]->(EventGitCoinGrant)
- (Wallet)-[IS_ADMIN]->(EventGitCoinGrant)
- (EventGitCoinGrant)-[HAS_ACCOUNT]->(Twitter)
- (Wallet)-[DONATION]->(EventGitCoinGrant)
- (UserGitCoin)-[IS_OWNER]->(EventGitCoinBounty)
- (UserGitCoin)-[HAS_FULLFILLED]->(EventGitCoinBounty)
- (UserGitCoin)-[HAS_INTEREST]->(EventGitCoinBounty)
- (UserGitCoin)-[HAS_WALLET]->(Wallet) -->

# Bucket

This ingestor reads and writes from the following bucket: `unlock`

# Metadata

No unique metadata created

# Flags

No unique flags created