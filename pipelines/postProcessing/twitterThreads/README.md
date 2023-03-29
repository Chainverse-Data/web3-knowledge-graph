# Twitter Threads exctract Post-Processing

This post-processor searches for search query using the Twitter search API. It then collects the conversation id (the threads IDs) and follows them through.

It runs two regexp against the text to find ENS names and wallet addresses. 

Finally, it creates threads nodes, and will retrieve those conversations until no tweet has been posted in the conversation for over 7 days. 

## Ontology

Nodes:
- (Twitter:Thread) A thread node
  - conversationId: The conversation ID of that thread
- (Twitter:Account)
- (Wallet:Account)
- (Alias:Ens)

Edges:
- (Wallet:Account)-[edge:HAS_ACCOUNT]->(Twitter:Account)
  - citation = "From twitter thread scraper"
  - tweets = Tweets that contain the information
- (Twitter:Account)-[edge:HAS_ALIAS]->(Alias:Ens)
  - edge.citation = "From twitter thread scraper"
  - tweets = Tweets that contain the information
- (Twitter:Account)-[:REPLIED]->(Twitter:Thread)
- (Twitter:Account)-[:AUTHOR]->(Twitter:Thread)

## Bucket

This post-processor reads and writes from the following bucket: `twitter-threads`

## Flags

No unique flags created
