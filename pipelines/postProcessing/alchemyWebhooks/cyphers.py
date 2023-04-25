from pipelines.helpers.decorators import get_query_logging
from pipelines.helpers.indexes import Indexes
from ...helpers import count_query_logging
from ...helpers import Cypher


class WebhooksCyphers(Cypher):
    def __init__(self, database=None):
        super().__init__(database)

    def create_indexes(self):
        indexes = Indexes()
        indexes.walletsBools()

    @get_query_logging
    def get_webhooks(self, webhook_label) -> dict[str]:
        query = f"""
            MATCH (webhook:Alchemy:{webhook_label})
            RETURN webhook.id as webhook_id, webhook.network as network, apoc.node.degree(webhook, "IS_MEMBER_OF") as degree
        """
        records = self.query(query)
        webhooks = {}
        for record in records:
            degree = record["degree"]
            network = record["network"]
            if network not in webhooks:
                webhooks[network] = {}
            webhooks[network][record["webhook_id"]] = degree
        return webhooks


    @count_query_logging
    def create_webhook(self, network, webhook_id, callbackUrl, webhook_label):
        query = f"""
            CREATE (webhook:Alchemy:{webhook_label})
            SET webhook.id = $id,
                webhook.network = $network,
                webhook.callbackUrl = $callbackUrl
            RETURN count(webhook)
        """
        count = self.query(query, parameters={"id": webhook_id, "network": network, "callbackUrl": callbackUrl})[0].value()
        return count

# Address queries

    @get_query_logging
    def get_items_to_watch(self, label, webhook_label):
        """Parameters:
            - label: Wallet | Token
            - webhook_label: AddressesWebhook | TokensWebhook
        """
        query = f"""
            MATCH (item:{label})
            WHERE item.notifySelected = true AND NOT (item)-[:IS_WATCHED_BY]-(:Alchemy:{webhook_label})
            RETURN item.address as address
        """
        records = self.query(query)
        items = [record["address"] for record in records]
        return items

    @get_query_logging
    def get_items_to_remove(self, label, webhook_label):
        """Parameters:
            - label: Wallet | Token
            - webhook_label: AddressesWebhook | TokensWebhook
        """
        query = f"""
            MATCH (item:{label})-[edge:IS_WATCHED_BY]-(webhook:Alchemy:{webhook_label})
            WHERE item.notifySelected <> true
            RETURN item.address as address, webhook.id as webhook_id
        """
        records = self.query(query)
        data = [(record["address"], record["webhook_id"]) for record in records]
        return data
    
    @count_query_logging
    def remove_item_from_webhook(self, webhook_id, addresses, label, webhook_label):
        """Parameters:
            - webhook_id: The id of the webhook
            - addresses: Array of addresses (must be lowercase)
            - label: Wallet | Token
            - webhook_label: AddressesWebhook | TokensWebhook
        """
        query = f"""
            MATCH (webhook:Alchemy:{webhook_label} {{id: $webhook_id}})<-[edge:IS_WATCHED_BY]-(item:{label})
            WHERE item.address in $addresses
            DELETE edge
            RETURN count(edge)
        """ 
        count = self.query(query, parameters={"webhook_id": webhook_id, "addresses": addresses})[0].value()
        return count

    @count_query_logging
    def connect_items_to_webhook(self, webhook_id, addresses, label, webhook_label):
        """Parameters:
            - webhook_id: The id of the webhook
            - addresses: Array of addresses (must be lowercase)
            - label: Wallet | Token
            - webhook_label: AddressesWebhook | TokensWebhook
        """
        query = f"""
            MATCH (webhook:Alchemy:{webhook_label} {{id: $webhook_id}})
            MATCH (item:{label})
            WHERE item.address IN $addresses
            MERGE (item)-[edge:IS_WATCHED_BY]->(webhook)
            ON CREATE SET edge.createdDt = datetime()
            ON MATCH SET edge.lastUpdateDt = datetime()
            return count(edge)
        """
        count = self.query(query, parameters={"webhook_id": webhook_id, "addresses": addresses})[0].value()
        return count