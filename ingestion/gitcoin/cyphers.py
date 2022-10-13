from ...helpers import Cypher
from ...helpers import Constraints
from ...helpers import Indexes
import logging
import sys

class GitcoinCyphers(Cypher):
    def __init__(self):
        super().__init__()

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

    def create_or_merge_grants(self, urls):
        logging.info(f"Ingesting with: {sys._getframe().f_code.co_name}")
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS grants
                    MERGE(grant:Gitcoin:Grant:Event {{id: grants.id}})
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
                        grant.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    ON MATCH set grant.title = grants.title,
                        grant.text = grants.text,
                        grant.types = grants.types,
                        grant.tags = grants.tags,
                        grant.amount = grants.amount,
                        grant.asOf = grants.asOf,
                        grant.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    return count(grant)
            """
            count += self.query(query)[0].value()
        logging.info(f"Created or merged: {count}")
        return count

    def create_or_merge_team_members(self, urls):
        logging.info(f"Ingesting with: {sys._getframe().f_code.co_name}")
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS members
                    MERGE(user:Gitcoin:GitHub:Account {{id: members.userId}})
                    ON CREATE set user.uuid = apoc.create.uuid(),
                        user.id = members.userId, 
                        user.handle = members.handle, 
                        user.asOf = members.asOf,
                        user.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        user.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    ON MATCH set user.handle = members.handle,
                        user.asOf = members.asOf,
                        user.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    return count(user)
            """

            count += self.query(query)[0].value()
        logging.info(f"Created or merged: {count}")
        return count

    def link_or_merge_team_members(self, urls):
        logging.info(f"Ingesting with: {sys._getframe().f_code.co_name}")
        count = 0
        for url in urls:

            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS members
                    MATCH (grant:Gitcoin:Grant:Event {{id: members.grantId}}), (member:Gitcoin:Account {{id: members.userId}})
                    WITH grant, member, members
                    MERGE (member)-[edge:MEMBER_OF]->(grant)
                    ON CREATE set edge.uuid = apoc.create.uuid(),
                        edge.citation = members.citation,
                        edge.asOf = members.asOf,
                        edge.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    ON MATCH set edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        edge.asOf = members.asOf
                    return count(edge)
            """
            count += self.query(query)[0].value()
        logging.info(f"Created or merged: {count}")
        return count

    def create_or_merge_admins(self, urls):
        logging.info(f"Ingesting with: {sys._getframe().f_code.co_name}")
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS admin_wallets
                    MERGE(wallet:Wallet {{address: admin_wallets.address}})
                    ON CREATE set wallet.uuid = apoc.create.uuid(),
                        wallet.address = admin_wallets.address,
                        wallet.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        wallet.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    ON MATCH set wallet.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        wallet.address = admin_wallets.address
                    return count(wallet)
            """
            count += self.query(query)[0].value()
        logging.info(f"Created or merged: {count}")
        return count

    def link_or_merge_admin_wallet(self, urls):
        logging.info(f"Ingesting with: {sys._getframe().f_code.co_name}")
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS admin_wallets
                    MATCH (grant:Gitcoin:Grant:Event {{id: admin_wallets.grantId}}), (wallet:Wallet {{address: admin_wallets.address}})
                    WITH grant, wallet, admin_wallets
                    MERGE (wallet)-[edge:IS_ADMIN]->(grant)
                    ON CREATE set edge.uuid = apoc.create.uuid(),
                        edge.citation = admin_wallets.citation,
                        edge.asOf = admin_wallets.asOf,
                        edge.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    ON MATCH set edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        edge.asOf = admin_wallets.asOf
                    return count(edge)
            """
            count += self.query(query)[0].value()
        logging.info(f"Created or merged: {count}")
        return count

    def create_or_merge_twitter_accounts(self, urls):
        logging.info(f"Ingesting with: {sys._getframe().f_code.co_name}")
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS twitter_accounts
                    MERGE(twitter:Twitter:Account {{handle: twitter_accounts.handle}})
                    ON CREATE set twitter.uuid = apoc.create.uuid(),
                        twitter.handle = twitter_accounts.handle,
                        twitter.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        twitter.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    ON MATCH set twitter.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    return count(twitter)
            """
            count += self.query(query)[0].value()
        logging.info(f"Created or merged: {count}")
        return count

    def link_or_merge_twitter_accounts(self, urls):
        logging.info(f"Ingesting with: {sys._getframe().f_code.co_name}")
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS twitter_accounts
                    MATCH (twitter:Twitter {{handle: twitter_accounts.handle}}), (grant:Gitcoin:Grant:Event {{id: twitter_accounts.grantId}})
                    WITH twitter, grant, twitter_accounts
                    MERGE (grant)-[edge:HAS_ACCOUNT]->(twitter)
                    ON CREATE set edge.uuid = apoc.create.uuid(),
                        edge.citation = twitter_accounts.citation,
                        edge.asOf = twitter_accounts.asOf,
                        edge.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    ON MATCH set edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        edge.asOf = twitter_accounts.asOf
                    return count(edge)
            """
            count += self.query(query)[0].value()
        logging.info(f"Created or merged: {count}")
        return count

    def create_or_merge_donators(self, urls):
        logging.info(f"Ingesting with: {sys._getframe().f_code.co_name}")
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS donations
                    MERGE(wallet:Wallet {{address: donations.donor}})
                    ON CREATE set wallet.uuid = apoc.create.uuid(),
                        wallet.address = donations.donor,
                        wallet.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        wallet.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    ON MATCH set wallet.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        wallet.address = donations.donor
                    return count(wallet)
            """
            count += self.query(query)[0].value()
        logging.info(f"Created or merged: {count}")
        return count

    def link_or_merge_donations(self, urls):
        logging.info(f"Ingesting with: {sys._getframe().f_code.co_name}")
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS donations
                    MATCH (grant:Gitcoin:Grant:Event)-[is_admin:IS_ADMIN]-(admin_wallet:Wallet {{address: donations.destination}})
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
                        donation.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    ON MATCH set donation.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    return count(donation)
            """
            count += self.query(query)[0].value()
        logging.info(f"Created or merged: {count}")
        return count

    def create_or_merge_bounties(self, urls):
        logging.info(f"Ingesting with: {sys._getframe().f_code.co_name}")
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS bounties
                    MERGE(bounty:Gitcoin:Bounty:Event {{id: bounties.id}})
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
                        bounty.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
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
                        bounty.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    return count(bounty)
            """
            
            count += self.query(query)[0].value()
        logging.info(f"Created or merged: {count}")
        return count

    def create_or_merge_bounties_orgs(self, urls):
        logging.info(f"Ingesting with: {sys._getframe().f_code.co_name}")
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS orgs
                    MERGE (org:Entity {{name: orgs.org_name}})
                    ON CREATE set org.uuid = apoc.create.uuid(),
                        org.name = orgs.org_name, 
                        org.asOf = orgs.asOf,
                        org.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        org.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    ON MATCH set org.name = orgs.org_name,
                        org.asOf = orgs.asOf,
                        org.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    return count(org)
                    """
            count += self.query(query)[0].value()
        logging.info(f"Created or merged: {count}")
        return count

    def link_or_merge_bounties_orgs(self, urls):
        logging.info(f"Ingesting with: {sys._getframe().f_code.co_name}")
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS orgs
                    MATCH (bounty:Gitcoin:Bounty:Event {{id: orgs.bountyId}}), (entity:Entity {{name: orgs.org_name}})
                    WITH bounty, entity, orgs
                    MERGE (entity)-[link:HAS_BOUNTY]->(bounty)
                    ON CREATE set link.uuid = apoc.create.uuid(),
                        link.asOf = orgs.asOf,
                        link.citation = orgs.citation,
                        link.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        link.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    ON MATCH set link.asOf = orgs.asOf,
                        link.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    return count(link)
                    """
            count += self.query(query)[0].value()
        logging.info(f"Created or merged: {count}")
        return count

    def create_or_merge_bounties_owners(self, urls):
        logging.info(f"Ingesting with: {sys._getframe().f_code.co_name}")
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS owners
                    MERGE(user:Gitcoin:GitHub:Account {{id: owners.id}})
                    ON CREATE set user.uuid = apoc.create.uuid(),
                        user.id = owners.id, 
                        user.name = owners.name, 
                        user.handle = owners.handle, 
                        user.asOf = owners.asOf,
                        user.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        user.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    ON MATCH set user.handle = owners.handle,
                        user.name = owners.name, 
                        user.asOf = owners.asOf,
                        user.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    return count(user)
            """

            count += self.query(query)[0].value()
        logging.info(f"Created or merged: {count}")
        return count

    def link_or_merge_bounties_owners(self, urls):
        logging.info(f"Ingesting with: {sys._getframe().f_code.co_name}")
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS owners
                    MATCH (user:Gitcoin:User {{id: owners.id}}), (bounty:Gitcoin:Bounty:Event {{id: owners.bounty_id}})
                    WITH user, bounty, owners
                    MERGE (user)-[link:IS_OWNER]->(bounty)
                    ON CREATE set link.uuid = apoc.create.uuid(),
                        link.asOf = owners.asOf,
                        link.citation = owners.citation,
                        link.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        link.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    ON MATCH set link.asOf = owners.asOf,
                        link.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    return count(link)
            """

            count += self.query(query)[0].value()
        logging.info(f"Created or merged: {count}")
        return count

    def create_or_merge_bounty_owner_wallets(self, urls):
        logging.info(f"Ingesting with: {sys._getframe().f_code.co_name}")
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS owners
                    MERGE(wallet:Wallet {{address: owners.address}})
                    ON CREATE set wallet.uuid = apoc.create.uuid(),
                        wallet.address = owners.address, 
                        wallet.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        wallet.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    ON MATCH set wallet.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        wallet.address = owners.address
                    return count(wallet)
            """

            count += self.query(query)[0].value()
        logging.info(f"Created or merged: {count}")
        return count
    
    def link_or_merge_bounty_owner_wallets(self, urls):
        logging.info(f"Ingesting with: {sys._getframe().f_code.co_name}")
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS owners
                    MATCH (user:Gitcoin:GitHub:Account {{id: owners.id}}), (wallet:Wallet {{address: owners.address}})
                    WITH user, wallet, owners
                    MERGE (user)-[link:HAS_WALLET]->(wallet)
                    ON CREATE set link.uuid = apoc.create.uuid(),
                        link.citation = owners.citation,
                        link.asOf = owners.asOf,
                        link.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        link.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    ON MATCH set link.asOf = owners.asOf,
                        link.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    return count(link)
            """

            count += self.query(query)[0].value()
        logging.info(f"Created or merged: {count}")
        return count

    def create_or_merge_bounties_fullfilers(self, urls):
        logging.info(f"Ingesting with: {sys._getframe().f_code.co_name}")
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS fullfilers
                    MERGE(user:Gitcoin:GitHub:Account {{id: fullfilers.id}})
                    ON CREATE set user.uuid = apoc.create.uuid(),
                        user.id = fullfilers.id, 
                        user.email = fullfilers.email, 
                        user.name = fullfilers.name, 
                        user.handle = fullfilers.handle, 
                        user.keywords = fullfilers.keywords, 
                        user.asOf = fullfilers.asOf,
                        user.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        user.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    ON MATCH set user.handle = fullfilers.handle,
                        user.email = fullfilers.email, 
                        user.name = fullfilers.name, 
                        user.keywords = fullfilers.keywords, 
                        user.asOf = fullfilers.asOf,
                        user.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    return count(user)
            """
            count += self.query(query)[0].value()
        logging.info(f"Created or merged: {count}")
        return count

    def link_or_merge_bounties_fullfilers(self, urls):
        logging.info(f"Ingesting with: {sys._getframe().f_code.co_name}")
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS fullfilers
                    MATCH (user:Gitcoin:GitHub:Account {{id: fullfilers.id}}), (bounty:Gitcoin:Bounty:Event {{id: fullfilers.bounty_id}})
                    WITH user, bounty, fullfilers
                    MERGE (user)-[link:HAS_FULLFILLED]->(bounty)
                    ON CREATE set link.uuid = apoc.create.uuid(),
                        link.accepted = fullfilers.accepted,
                        link.citation = fullfilers.citation,
                        link.asOf = fullfilers.asOf,
                        link.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        link.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    ON MATCH set link.asOf = fullfilers.asOf,
                        link.accepted = fullfilers.accepted,
                        link.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    return count(link)
            """

            count += self.query(query)[0].value()
        logging.info(f"Created or merged: {count}")
        return count

    def create_or_merge_bounties_fullfilers_wallets(self, urls):
        logging.info(f"Ingesting with: {sys._getframe().f_code.co_name}")
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS owners
                    MERGE(wallet:Wallet {{address: owners.address}})
                    ON CREATE set wallet.uuid = apoc.create.uuid(),
                        wallet.address = owners.address, 
                        wallet.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        wallet.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    ON MATCH set wallet.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        wallet.address = owners.address
                    return count(wallet)
            """

            count += self.query(query)[0].value()
        logging.info(f"Created or merged: {count}")
        return count

    def link_or_merge_bounties_fullfilers_wallets(self, urls):
        logging.info(f"Ingesting with: {sys._getframe().f_code.co_name}")
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS fullfilers
                    MATCH (user:Gitcoin:GitHub:Account {{id: fullfilers.id}}), (wallet:Wallet {{address: fullfilers.address}})
                    WITH user, wallet, fullfilers
                    MERGE (user)-[link:HAS_WALLET]->(wallet)
                    ON CREATE set link.uuid = apoc.create.uuid(),
                        link.citation = fullfilers.citation,
                        link.asOf = fullfilers.asOf,
                        link.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        link.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    ON MATCH set link.asOf = fullfilers.asOf,
                        link.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    return count(link)
            """

            count += self.query(query)[0].value()
        logging.info(f"Created or merged: {count}")
        return count

    def create_or_merge_bounties_interested(self, urls):
        logging.info(f"Ingesting with: {sys._getframe().f_code.co_name}")
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS interested
                    MERGE(user:Gitcoin:GitHub:Account {{id: interested.id}})
                    ON CREATE set user.uuid = apoc.create.uuid(),
                        user.id = interested.id, 
                        user.name = interested.name, 
                        user.handle = interested.handle, 
                        user.keywords = interested.keywords, 
                        user.asOf = interested.asOf,
                        user.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        user.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    ON MATCH set user.handle = interested.handle,
                        user.name = interested.name, 
                        user.keywords = interested.keywords, 
                        user.asOf = interested.asOf,
                        user.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    return count(user)
            """
            count += self.query(query)[0].value()
        logging.info(f"Created or merged: {count}")
        return count

    def link_or_merge_bounties_interested(self, urls):
        logging.info(f"Ingesting with: {sys._getframe().f_code.co_name}")
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS interested
                    MATCH (user:Gitcoin:GitHub:Account {{id: interested.id}}), (bounty:Gitcoin:Bounty:Event {{id: interested.bounty_id}})
                    WITH user, bounty, interested
                    MERGE (user)-[link:HAS_INTEREST]->(bounty)
                    ON CREATE set link.uuid = apoc.create.uuid(),
                        link.asOf = interested.asOf,
                        link.citation = interested.citation,
                        link.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        link.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    ON MATCH set link.asOf = interested.asOf,
                        link.accepted = interested.accepted,
                        link.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    return count(link)
            """

            count += self.query(query)[0].value()
        logging.info(f"Created or merged: {count}")
        return count
