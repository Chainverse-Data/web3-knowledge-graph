import logging
import re

from pipelines.helpers import Alchemy
from .cyphers import WebhooksCyphers
from ..helpers import Processor

class WebhooksProcessor(Processor):
    def __init__(self):
        self.cyphers = WebhooksCyphers()
        self.alchemy = Alchemy()
        self.callbackURL = "https://whatever.com" # TODO: CHANGE THIS
        self.networks = ["ETH_MAINNET", "MATIC_MAINNET", "ARB_MAINNET", "OPT_MAINNET"]
        super().__init__(bucket_name="alchemy-webooks")

    def get_current_webhooks(self):
        records = self.cyphers.get_webhooks()
        webhooks = {}
        for record in records:
            webhook = record["webhook"]
            degree = record["degree"]
            webhooks[webhook["id"]] = {"webhook": webhook, "degree": degree}

    def get_wallets_to_watch(self):
        wallets = self.cyphers.get_wallets_to_watch()
        return wallets

    def get_wallets_to_remove(self):
        data = self.cyphers.get_wallets_to_remove()
        return data

    def process_removals(self):
        data = self.get_wallets_to_remove()
        for_removal = {}
        for wallet, webhook_id in data:
            if webhook_id not in for_removal:
                for_removal[webhook_id] = []
            for_removal[webhook_id].append(wallet)
        for webhook_id in for_removal:
            self.alchemy.update_webhook_address(webhook_id=webhook_id, addresses_to_remove=for_removal[webhook_id])
            self.cyphers.remove_address_from_webhook(webhook_id, for_removal[webhook_id])
        
    def process_additions(self):
        wallets = self.get_wallets_to_watch()
        current_index = 0
        webhooks = self.get_current_webhooks()
        for webhook_id in webhooks:
            remaining = 50000 - webhooks[webhook_id]["degree"]
            tmp_wallets = wallets[current_index: current_index+remaining]
            self.alchemy.update_webhook_address(webhook_id, addresses_to_add=tmp_wallets)
            current_index += remaining
        while current_index < len(wallets):
            tmp_wallets = wallets[current_index: current_index+50000]
            for network in self.networks:
                webhook_id = self.alchemy.create_webhook(network=network, webhook_type="ADDRESS_ACTIVITY", webhook_url=self.callbackURL, addresses=tmp_wallets)
                self.cyphers.create_address_webhook(network=network, webhook_id=webhook_id, callbackURL=self.callbackURL)
                self.cyphers.connect_wallet_to_webhook(webhook_id, tmp_wallets)

    def run(self):
        self.process_removals()
        self.process_additions()

if __name__ == "__main__":
    P = WebhooksProcessor()
    P.run()