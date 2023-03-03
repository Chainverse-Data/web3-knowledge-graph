# Summary

Let's simplify our API queries by adding `HAS_ACCOUNT` edges between all linked accounts.

Then, we can use one query to capture **all** accounts for any node in the graph.

We would also add the `Account` label to wallets.

We can also add the account type as `accountType` on the `Account` node so we can pull account type without having to look at the labels. 

# Clean up

**Script to clean-up my testing**


## Sample query

```
with 
    tolower("0x12eb9bfab36a16891ddd9604089734ef7c869e07") as address
match 
    (wallet:Wallet {address:address})
with 
    wallet, address
call 
    apoc.path.expandConfig(wallet,
        {
            relationshipFilter: "HAS_ACCOUNT"
        }) yield path
with 
    last(nodes(path)) as account
WITH account.handle as handle, account.userId as userId, account.address as address, account.api_accountType as platform
RETURN platform, collect({handle: handle, userId: userId, address: address, platform: platform}) as accounts
```

## Steps

### Add `Account` as a label for `Wallet`  

```
call apoc.periodic.commit("match (w:Wallet)
where not w:Account 
with w limit 10000
set w:Account 
return count(*)")
```

### Add `api_accountType` to all covered accounts 

#### Wallet 
```
call apoc.periodic.commit("match (w:Wallet) where w.api_accountType is null 
with w limit 10000
set w.api_accountType = 'Wallet' 
return count(*)")
```
#### Twitter 
```
call apoc.periodic.commit("match (twitter:Twitter) where twitter.api_accountType is null 
with twitter limit 10000
set twitter.api_accountType = 'Twitter' 
return count(*)")
```
#### Farcaster 

```
call apoc.periodic.commit("match (farcaster:Farcaster) where farcaster.api_accountType is null 
with farcaster limit 10000
set farcaster.api_accountType = 'Farcaster' 
return count(*)")
```
#### Github 

```
call apoc.periodic.commit("match (github:Github) where github.api_accountType is null 
with github limit 10000
set github.api_accountType = 'Github' 
return count(*)")
```

### Add `HAS_ACCOUNT` edges, where relevant, between covered accounts 

#### Wallet - Twitter
```
call apoc.periodic.commit("
match 
    (wallet:Wallet)-[:HAS_ALIAS]-(alias:Alias)-[:HAS_ALIAS]-(twitter:Twitter)
where not 
    (twitter)-[:HAS_ACCOUNT]->(wallet)
with 
    wallet, twitter limit 10000 
merge 
    (twitter)-[r:HAS_ACCOUNT]->(wallet)
set 
    r.citation = 'Twitter - self-attested in tweet or bio.'
return 
    count(*)")
```


#### Wallet - Github [include in delete]

```
match 
    (wallet:Wallet)-[:HAS_WALLET]-(g:Github)
where not 
    (wallet)-[:HAS_ACCOUNT]->(g)
with 
    wallet, g limit 10000
merge 
    (wallet)-[r:HAS_ACCOUNT]->(g)
return 
    count(distinct(r))```

