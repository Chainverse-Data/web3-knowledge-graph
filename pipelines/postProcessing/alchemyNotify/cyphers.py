from pipelines.helpers.decorators import get_query_logging
from ...helpers import count_query_logging
from ...helpers import Cypher


class WebhooksCyphers(Cypher):
    def __init__(self, database=None):
        super().__init__(database)

    @get_query_logging
    def get_webhooks(self):
        query = f"""
            MATCH (webhook:Alchemy:AddressesWebhook)
            RETURN webhook as webhook, apoc.node.degree(webhook, "IS_MEMBER_OF") as degree
        """
        records = self.query(query)
        return records

    @count_query_logging
    def create_address_webhook(self, network, webhook_id, callbackUrl):
        query = """
            CREATE (webhook:Alchemy:AddressesWebhook)
            SET webhook.id = $id,
                webhook.network = $network
                webhook.callbackUrl = $callbackUrl
            RETURN count(webhook)
        """
        count = self.query(query, parameters={"id": webhook_id, "network": network, "callbackUrl": callbackUrl})[0].value()
        return count

    @get_query_logging
    def get_wallets_to_watch(self):
        query = """
            MATCH (wallet:Wallet)
            WHERE wallet.notifySelected = true AND NOT wallet-[:IS_WATCHED_BY]-(:AddressesWebhook)
            RETURN wallet.address as address
        """
        records = self.query(query)
        wallets = [record["address"] for record in records]
        return wallets


    @get_query_logging
    def get_wallets_to_remove(self):
        query = """
            MATCH (wallet:Wallet)-[edge:IS_WATCHED_BY]-(webhook:AddressesWebhook)
            WHERE wallet.notifySelected <> true
            RETURN wallet.address as address, webhook.id as webhook_id
        """
        records = self.query(query)
        data = [(record["address"], record["webhook_id"]) for record in records]
        return data

    @count_query_logging
    def remove_address_from_webhook(self, webhook_id, addresses):
        query = f"""
            MATCH (webhook:AddressesWebhook {{id: $webhook_id}})<-[edge:IS_WATCHED_BY]-(wallet:Wallet)
            WHERE wallet.address in $addresses
            DELETE edge
            RETURN count(edge)
        """ 
        count = self.query(query, parameters={"webhook_id": webhook_id, "addresses": addresses})[0].value()
        return count

    @count_query_logging
    def connect_wallet_to_webhook(self, webhook_id, addresses):
        query = f"""
            MATCH (webhook:Alchemy:AddressesWebhook {{id: $webhook_id}})
            MATCH (wallet:Wallet)
            WHERE wallet.address IN $addresses
            MERGE (wallet)-[edge:IS_WATCHED_BY]->(webhook)
            ON CREATE SET edge.createdDt = datetime()
            ON MATCH SET edge.lastUpdatedDt = datetime()
            return count(edge)
        """
        count = self.query(query, parameters={"webhook_id": webhook_id, "addresses": addresses})[0].value()
        return count