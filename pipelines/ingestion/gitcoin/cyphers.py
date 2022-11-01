from ...helpers import Cypher
from ...helpers import Constraints
from ...helpers import Indexes
from ...helpers import Queries
from ...helpers import count_query_logging
import logging
import sys

class GitcoinCyphers(Cypher):
    def __init__(self):
        super().__init__()
        self.queries = Queries()

    def create_constraints(self):
        constraints = Constraints()
        constraints.twitter()
        constraints.wallets()
        constraints.gitcoin_grants()
        constraints.gitcoin_users()
        constraints.gitcoin_bounties()

    def create_indexes(self):
        indexes = Indexes()
        indexes.twitter()
        indexes.wallets()
        indexes.gitcoin_grants()
        indexes.gitcoin_users()
        indexes.gitcoin_bounties()

    @count_query_logging
    def create_or_merge_grants(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS grants
                    MERGE(grant:GitcoinGrant:Grant:Event {{id: grants.id}})
                    ON CREATE set grant.uuid = apoc.create.uuid(),
                        grant.id = grants.id, 
                        grant.title = grants.title, 
                        grant.text = grants.text, 
                        grant.types = grants.types,
                        grant.tags = grants.tags,
                        grant.url = grants.url,
                        grant.asOf = grants.asOf,
                        grant.amount = toFloat(grants.amount),
                        grant.amountDenomination = grants.amountDenomination,
                        grant.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        grant.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        grant.ingestedBy = "{self.CREATED_ID}"
                    ON MATCH set grant.title = grants.title,
                        grant.text = grants.text,
                        grant.types = grants.types,
                        grant.tags = grants.tags,
                        grant.amount = grants.amount,
                        grant.asOf = grants.asOf,
                        grant.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        grant.ingestedBy = "{self.UPDATED_ID}"
                    return count(grant)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def set_grant_round(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS grants
                    MATCH (grant:GitcoinGrant:Grant {{id: grants.id}})
                    SET grant:grants.grant_round
                    RETURN count(grant)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def create_or_merge_grants_tags(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS tags
                    MERGE (tag:Tag {{label: toLower(tags.label)}})
                    ON CREATE set tag.uuid = apoc.create.uuid(),
                        tag.label = tags.label, 
                        tag.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        tag.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        tag.ingestedBy = "{self.CREATED_ID}"
                    ON MATCH set tag.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        tag.ingestedBy = "{self.UPDATED_ID}"
                    return count(tag)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_grants_tag(self, urls):
        count = 0
        for url in urls:
            query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' AS grant_tags
                        MATCH (grant:GitcoinGrant {{id: grant_tags.grantId}}), (tag:Tag {{label: toLower(grant_tags.label)}})
                        WITH grant, tag
                        MERGE (grant)-[edge:HAS_TAG]->(tag)
                        ON CREATE set edge.uuid = apoc.create.uuid(),
                            edge.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                            edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                            edge.ingestedBy = "{self.CREATED_ID}"
                        ON MATCH set edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                            edge.ingestedBy = "{self.UPDATED_ID}"
                        return count(edge)
                """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def create_or_merge_team_members(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS members
                    MERGE(user:GitcoinUser:GitHubUser:Account {{id: members.userId}})
                    ON CREATE set user.uuid = apoc.create.uuid(),
                        user.id = members.userId, 
                        user.handle = members.handle, 
                        user.asOf = members.asOf,
                        user.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        user.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        user.ingestedBy = "{self.CREATED_ID}"
                    ON MATCH set user.handle = members.handle,
                        user.asOf = members.asOf,
                        user.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        user.ingestedBy = "{self.UPDATED_ID}"
                    return count(user)
            """

            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_or_merge_team_members(self, urls):
        count = 0
        for url in urls:

            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS members
                    MATCH (grant:GitcoinGrant {{id: members.grantId}}), (member:GitcoinUser {{id: members.userId}})
                    WITH grant, member, members
                    MERGE (member)-[edge:MEMBER_OF]->(grant)
                    ON CREATE set edge.uuid = apoc.create.uuid(),
                        edge.citation = members.citation,
                        edge.asOf = members.asOf,
                        edge.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        edge.ingestedBy = "{self.CREATED_ID}"
                    ON MATCH set edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        edge.asOf = members.asOf,
                        edge.ingestedBy = "{self.UPDATED_ID}"
                    return count(edge)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def create_or_merge_admins(self, urls):
        count = self.queries.create_wallets(urls)
        return count

    @count_query_logging
    def link_or_merge_admin_wallet(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS admin_wallets
                    MATCH (grant:GitcoinGrant {{id: admin_wallets.grantId}}), (wallet:Wallet {{address: admin_wallets.address}})
                    WITH grant, wallet, admin_wallets
                    MERGE (wallet)-[edge:IS_ADMIN]->(grant)
                    ON CREATE set edge.uuid = apoc.create.uuid(),
                        edge.citation = admin_wallets.citation,
                        edge.asOf = admin_wallets.asOf,
                        edge.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        edge.ingestedBy = "{self.CREATED_ID}"
                    ON MATCH set edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        edge.asOf = admin_wallets.asOf,
                        edge.ingestedBy = "{self.UPDATED_ID}"
                    return count(edge)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def create_or_merge_twitter_accounts(self, urls):
        count = self.queries.create_or_merge_twitter(urls)
        return count

    @count_query_logging
    def link_or_merge_twitter_accounts(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS twitter_accounts
                    MATCH (twitter:Twitter {{handle: twitter_accounts.handle}}), (grant:GitcoinGrant:Grant:Event {{id: twitter_accounts.grantId}})
                    WITH twitter, grant, twitter_accounts
                    MERGE (grant)-[edge:HAS_ACCOUNT]->(twitter)
                    ON CREATE set edge.uuid = apoc.create.uuid(),
                        edge.citation = twitter_accounts.citation,
                        edge.asOf = twitter_accounts.asOf,
                        edge.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        edge.ingestedBy = "{self.CREATED_ID}"
                    ON MATCH set edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        edge.asOf = twitter_accounts.asOf,
                        edge.ingestedBy = "{self.UPDATED_ID}"
                    return count(edge)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def create_or_merge_donators(self, urls):
        count = self.queries.create_wallets(urls)
        return count

    @count_query_logging
    def link_or_merge_donations(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS donations
                    MATCH (grant:GitcoinGrant)-[is_admin:IS_ADMIN]-(admin_wallet:Wallet {{address: donations.destination}})
                    OPTIONAL MATCH (donor:Wallet {{address: donations.donor}})
                    WITH grant, donor, donations
                    MERGE (donor)-[donation:DONATION {{txHash: donations.txHash}}]->(grant)
                    ON CREATE set donation.uuid = apoc.create.uuid(),
                        donation.token = donations.token,
                        donation.amount = donations.amount,
                        donation.txHash = donations.txHash,
                        donation.chainId = donations.txHash,
                        donation.chain = donations.chain,
                        donation.blockNumber = donations.blockNumber,
                        donation.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        donation.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')), 
                        donation.ingestedBy = "{self.CREATED_ID}"
                    ON MATCH set donation.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')), 
                        donation.ingestedBy = "{self.UPDATED_ID}"
                    return count(donation)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def create_or_merge_bounties(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS bounties
                    MERGE(bounty:GitcoinBounty:Event {{id: bounties.id}})
                    ON CREATE set bounty.uuid = apoc.create.uuid(),
                        bounty.id = bounties.id, 
                        bounty.title = bounties.title, 
                        bounty.text = bounties.text, 
                        bounty.url = bounties.url,
                        bounty.github_url = bounties.github_url,
                        bounty.status = bounties.status,
                        bounty.value_in_token = bounties.value_in_token,
                        bounty.token_name = bounties.token_name,
                        bounty.token_address = bounties.token_address,
                        bounty.bounty_type = bounties.bounty_type,
                        bounty.project_length = bounties.project_length,
                        bounty.experience_level = bounties.experience_level,
                        bounty.keywords = bounties.keywords,
                        bounty.token_value_in_usdt = bounties.token_value_in_usdt,
                        bounty.network = bounties.network,
                        bounty.org_name = bounties.org_name,
                        bounty.asOf = bounties.asOf,
                        bounty.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        bounty.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        bounty.ingestedBy = "{self.CREATED_ID}"
                    ON MATCH set bounty.title = bounties.title,
                        bounty.text = bounties.text, 
                        bounty.status = bounties.status,
                        bounty.value_in_token = bounties.value_in_token,
                        bounty.token_name = bounties.token_name,
                        bounty.token_address = bounties.token_address,
                        bounty.bounty_type = bounties.bounty_type,
                        bounty.project_length = bounties.project_length,
                        bounty.experience_level = bounties.experience_level,
                        bounty.keywords = bounties.keywords,
                        bounty.token_value_in_usdt = bounties.token_value_in_usdt,
                        bounty.network = bounties.network,
                        bounty.org_name = bounties.org_name,
                        bounty.asOf = bounties.asOf,
                        bounty.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        bounty.ingestedBy = "{self.UPDATED_ID}"
                    return count(bounty)
            """
            
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def create_or_merge_bounties_orgs(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS orgs
                    MERGE (org:Entity {{name: orgs.org_name}})
                    ON CREATE set org.uuid = apoc.create.uuid(),
                        org.name = orgs.org_name, 
                        org.asOf = orgs.asOf,
                        org.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        org.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        org.ingestedBy = "{self.CREATED_ID}"
                    ON MATCH set org.name = orgs.org_name,
                        org.asOf = orgs.asOf,
                        org.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        org.ingestedBy = "{self.UPDATED_ID}"
                    return count(org)
                    """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_or_merge_bounties_orgs(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS orgs
                    MATCH (bounty:GitcoinBounty {{id: orgs.bountyId}}), (entity:Entity {{name: orgs.org_name}})
                    WITH bounty, entity, orgs
                    MERGE (entity)-[link:HAS_BOUNTY]->(bounty)
                    ON CREATE set link.uuid = apoc.create.uuid(),
                        link.asOf = orgs.asOf,
                        link.citation = orgs.citation,
                        link.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        link.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        link.ingestedBy = "{self.CREATED_ID}"
                    ON MATCH set link.asOf = orgs.asOf,
                        link.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        link.ingestedBy = "{self.UPDATED_ID}"
                    return count(link)
                    """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def create_or_merge_bounties_owners(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS owners
                    MERGE(user:GitcoinUser:GitHubUser:Account {{id: owners.id}})
                    ON CREATE set user.uuid = apoc.create.uuid(),
                        user.id = owners.id, 
                        user.name = owners.name, 
                        user.handle = owners.handle, 
                        user.asOf = owners.asOf,
                        user.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        user.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        user.ingestedBy = "{self.CREATED_ID}"
                    ON MATCH set user.handle = owners.handle,
                        user.name = owners.name, 
                        user.asOf = owners.asOf,
                        user.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        user.ingestedBy = "{self.UPDATED_ID}"
                    return count(user)
            """

            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_or_merge_bounties_owners(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS owners
                    MATCH (user:GitcoinUser {{id: owners.id}}), (bounty:GitcoinBounty {{id: owners.bounty_id}})
                    WITH user, bounty, owners
                    MERGE (user)-[link:IS_OWNER]->(bounty)
                    ON CREATE set link.uuid = apoc.create.uuid(),
                        link.asOf = owners.asOf,
                        link.citation = owners.citation,
                        link.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        link.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        link.ingestedBy = "{self.CREATED_ID}"
                    ON MATCH set link.asOf = owners.asOf,
                        link.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        link.ingestedBy = "{self.UPDATED_ID}"
                    return count(link)
            """

            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def create_or_merge_bounty_owner_wallets(self, urls):
        count = self.queries.create_wallets(urls)
        return count
    
    @count_query_logging
    def link_or_merge_bounty_owner_wallets(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS owners
                    MATCH (user:GitcoinUser {{id: owners.id}}), (wallet:Wallet {{address: owners.address}})
                    WITH user, wallet, owners
                    MERGE (user)-[link:HAS_WALLET]->(wallet)
                    ON CREATE set link.uuid = apoc.create.uuid(),
                        link.citation = owners.citation,
                        link.asOf = owners.asOf,
                        link.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        link.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        link.ingestedBy = "{self.CREATED_ID}"
                    ON MATCH set link.asOf = owners.asOf,
                        link.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        link.ingestedBy = "{self.UPDATED_ID}"
                    return count(link)
            """

            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def create_or_merge_bounties_fullfilers(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS fullfilers
                    MERGE(user:GitcoinUser:GitHubUser:Account {{id: fullfilers.id}})
                    ON CREATE set user.uuid = apoc.create.uuid(),
                        user.id = fullfilers.id, 
                        user.email = fullfilers.email, 
                        user.name = fullfilers.name, 
                        user.handle = fullfilers.handle, 
                        user.keywords = fullfilers.keywords, 
                        user.asOf = fullfilers.asOf,
                        user.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        user.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        user.ingestedBy = "{self.CREATED_ID}"
                    ON MATCH set user.handle = fullfilers.handle,
                        user.email = fullfilers.email, 
                        user.name = fullfilers.name, 
                        user.keywords = fullfilers.keywords, 
                        user.asOf = fullfilers.asOf,
                        user.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')), 
                        user.ingestedBy = "{self.UPDATED_ID}"
                    return count(user)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_or_merge_bounties_fullfilers(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS fullfilers
                    MATCH (user:GitcoinUser {{id: fullfilers.id}}), (bounty:GitcoinBounty {{id: fullfilers.bounty_id}})
                    WITH user, bounty, fullfilers
                    MERGE (user)-[link:HAS_FULLFILLED]->(bounty)
                    ON CREATE set link.uuid = apoc.create.uuid(),
                        link.accepted = fullfilers.accepted,
                        link.citation = fullfilers.citation,
                        link.asOf = fullfilers.asOf,
                        link.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        link.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        link.ingestedBy = "{self.CREATED_ID}"
                    ON MATCH set link.asOf = fullfilers.asOf,
                        link.accepted = fullfilers.accepted,
                        link.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        link.ingestedBy = "{self.UPDATED_ID}"
                    return count(link)
            """

            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def create_or_merge_bounties_fullfilers_wallets(self, urls):
        count = self.queries.create_wallets(urls)
        return count

    @count_query_logging
    def link_or_merge_bounties_fullfilers_wallets(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS fullfilers
                    MATCH (user:GitcoinUser {{id: fullfilers.id}}), (wallet:Wallet {{address: fullfilers.address}})
                    WITH user, wallet, fullfilers
                    MERGE (user)-[link:HAS_WALLET]->(wallet)
                    ON CREATE set link.uuid = apoc.create.uuid(),
                        link.citation = fullfilers.citation,
                        link.asOf = fullfilers.asOf,
                        link.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        link.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        link.ingestedBy = "{self.CREATED_ID}"
                    ON MATCH set link.asOf = fullfilers.asOf,
                        link.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        link.ingestedBy = "{self.UPDATED_ID}"
                    return count(link)
            """

            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def create_or_merge_bounties_interested(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS interested
                    MERGE(user:GitcoinUser:GitHubUser:Account {{id: interested.id}})
                    ON CREATE set user.uuid = apoc.create.uuid(),
                        user.id = interested.id, 
                        user.name = interested.name, 
                        user.handle = interested.handle, 
                        user.keywords = interested.keywords, 
                        user.asOf = interested.asOf,
                        user.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        user.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        user.ingestedBy = "{self.CREATED_ID}"
                    ON MATCH set user.handle = interested.handle,
                        user.name = interested.name, 
                        user.keywords = interested.keywords, 
                        user.asOf = interested.asOf,
                        user.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        user.ingestedBy = "{self.UPDATED_ID}"
                    return count(user)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_or_merge_bounties_interested(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS interested
                    MATCH (user:GitcoinUser {{id: interested.id}}), (bounty:GitcoinBounty {{id: interested.bounty_id}})
                    WITH user, bounty, interested
                    MERGE (user)-[link:HAS_INTEREST]->(bounty)
                    ON CREATE set link.uuid = apoc.create.uuid(),
                        link.asOf = interested.asOf,
                        link.citation = interested.citation,
                        link.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        link.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        link.ingestedBy = "{self.CREATED_ID}"
                    ON MATCH set link.asOf = interested.asOf,
                        link.accepted = interested.accepted,
                        link.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        link.ingestedBy = "{self.UPDATED_ID}"
                    return count(link)
            """

            count += self.query(query)[0].value()
        return count
