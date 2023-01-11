

create
(blah:Wic:Farmer:Governance)
set
blah.label = ‘Incentive Farmer’
set
blah.uuid = apoc.create.uuid()
set
blah.category = ‘Governance’

;

match
(wic:Wic:Farmer:Governance)
match
(wallet:Wallet)-[]-(:Proposal)-[]-(e:Entity)-[:HAS_STRATEGY]-(token:Token)
where
token.symbol in [‘DAI’, ‘USDC’, ‘ETH’, ‘WETH’, ‘WBTC’, ‘USDT’]
with
wic, wallet, e.uuid as uuid
merge
(wallet)-[r:WIC]->(wic)
set
r.context = uuid

with
wallet

match
(w:Wallet)-[r:IS_MEMBER]->(e:Elements:Tokens)-[]-(c:Community:Main {{communityId: ‘{communityId}’}})
optional match
(w)-[:WIC_SUBGRAPH]->(f:Wic:Farmer:Governance)

;

match
(wic:Wic:Farmer:Governance)
match
(wallet:Wallet)-[]-(:Proposal)-[]-(e:Entity)-[:HAS_STRATEGY]-(token:Token)
where
token.symbol in [‘DAI’, ‘USDC’, ‘ETH’, ‘WETH’, ‘WBTC’, ‘USDT’]
with
wic, wallet, e.uuid as uuid
merge
(wallet)-[r:WIC]->(wic)
set
r.context = uuid

