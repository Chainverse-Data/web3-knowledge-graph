


MATCH (w:Wallet)-[r:VOTED]->(p:Proposal)
MATCH (d:Delegation)-[:DELEGATED|DELEGATED_TO]-(w)
with w,d,p, timestamp(d.blockTimestamp) as votedDt, d.blockTimestamp as blockTimestamp
where votedDt > blockTimestamp
with w, d
order by blockTimestamp desc
limit 1
return d