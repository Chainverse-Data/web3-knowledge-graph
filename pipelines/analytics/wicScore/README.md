# WIC Score

This analysis computes the WIC reputation score for wallets in the graph 

# Analyses

Computation of the score takes place as follows:
 - Retrieves all wallets with a WIC edge.
 - Compute their WIC degree (how many context do they have)
 - Takes the log of the degree distribution
 - Compute the Zscore for the distribution
 - rescale the Zscore between 0 and 100

# Ontology

## Nodes
  - Wallet
    - reputationScore: float