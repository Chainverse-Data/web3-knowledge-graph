import logging
import re

from pipelines.helpers import Alchemy
from .cyphers import WebhooksCyphers
from ..helpers import Processor
from tqdm import tqdm

class WebhooksProcessor(Processor):
    def __init__(self):
        self.cyphers = WebhooksCyphers()
        self.alchemy = Alchemy()
        self.addressCallbackURL = "https://chainverse-api.com/v1/api/alchemy/addressInput"
        self.tokenCallbackURL = "https://chainverse-api.com/v1/api/alchemy/tokenInput"
        self.networks = ["ETH_MAINNET", "MATIC_MAINNET", "ARB_MAINNET", "OPT_MAINNET"]
        self.max_wallet_address_webhook = 50000
        self.max_tokens_webhook = 50000
        super().__init__(bucket_name="alchemy-webhooks")

    def split(self, array, chunk_size):
        for i in range(0, len(array), chunk_size):
            yield array[i:i + chunk_size]

    def process_addresses_removals(self):
        data = self.cyphers.get_items_to_remove("Wallet", "AddressesWebhook")
        for_removal = {}
        for wallet, webhook_id in data:
            if webhook_id not in for_removal:
                for_removal[webhook_id] = []
            for_removal[webhook_id].append(wallet)
        for webhook_id in for_removal:
            self.alchemy.update_webhook_address(webhook_id=webhook_id, addresses_to_remove=for_removal[webhook_id])
            self.cyphers.remove_item_from_webhook(webhook_id, for_removal[webhook_id], "Wallet", "AddressesWebhook")
    
    def process_addresses_additions(self):
        wallets = self.cyphers.get_items_to_watch("Wallet", "AddressesWebhook")
        current_index = 0
        webhooks = self.cyphers.get_webhooks("AddressesWebhook")
        additions = {}
        for network in self.networks:
            additions[network] = {}
        for network in webhooks:
            for webhook_id in webhooks[network]:
                additions[network][webhook_id] = []
        for wallet in tqdm(wallets, desc="Sorting addresses to existing webhooks ..."):
            for network in webhooks:
                for webhook_id in webhooks[network]:
                    if webhooks[network][webhook_id] < self.max_wallet_address_webhook:
                        additions[network][webhook_id].append(wallet)
                        webhooks[network][webhook_id] += 1
                        break
            current_index += 1
            
        print("Addision", additions)

        for network in tqdm(additions, desc="Updating webhooks..."):
            for webhook_id in additions[network]:
                self.alchemy.update_webhook_address(webhook_id, addresses_to_add=additions[network][webhook_id])
                self.cyphers.connect_items_to_webhook(webhook_id, additions[network][webhook_id], "Wallet", "AddressesWebhook")

        done = {}
        for network in self.networks:
            done[network] = set()
            for webhook_id in additions[network]:
                for wallet in additions[network][webhook_id]:
                    done[network].add(wallet)

        print("DONE", done)

        for network in tqdm(self.networks, desc="Creating new webhooks ..."):
            tmp_wallets = []
            for wallet in wallets:
                if wallet not in done[network]:
                    tmp_wallets.append(wallet)
            for wallet_chunk in self.split(tmp_wallets, 50000):
                webhook = self.alchemy.create_webhook(network=network, webhook_type="ADDRESS_ACTIVITY", webhook_url=self.addressCallbackURL, addresses=wallet_chunk)
                self.cyphers.create_webhook(webhook["network"], webhook["id"], webhook["webhook_url"], "AddressesWebhook")
                self.cyphers.connect_items_to_webhook(webhook["id"], wallet_chunk, "Wallet", "AddressesWebhook")
                current_index += self.max_wallet_address_webhook

    def process_tokens_removals(self):
        data = self.cyphers.get_items_to_remove("Token", "TokensWebhook")
        for_removal = {}
        for token, webhook_id in data:
            if webhook_id not in for_removal:
                for_removal[webhook_id] = []
            for_removal[webhook_id].append(token)
        for webhook_id in for_removal:
            self.alchemy.update_webhook_tokens(webhook_id=webhook_id, tokens_to_remove=for_removal[webhook_id])
            self.cyphers.remove_item_from_webhook(webhook_id, for_removal[webhook_id], "Token", "TokensWebhook")
        
    def process_tokens_additions(self):
        wallets = self.cyphers.get_items_to_watch("Token", "TokensWebhook")
        current_index = 0
        webhooks = self.cyphers.get_webhooks("TokensWebhook")
        for webhook_id in webhooks:
            remaining = self.max_tokens_webhook - webhooks[webhook_id]
            tmp_tokens = wallets[current_index: current_index+remaining]
            self.alchemy.update_webhook_tokens(webhook_id, addresses_to_add=tmp_tokens)
            current_index += remaining
        while current_index < len(wallets):
            tmp_tokens = wallets[current_index: current_index+self.max_tokens_webhook]
            for network in self.networks:
                webhook_id = self.alchemy.create_webhook(network=network, webhook_type="TOKEN_ACTIVITY", webhook_url=self.callbackURL, addresses=tmp_tokens)
                self.cyphers.create_webhook(network=network, webhook_id=webhook_id, callbackURL=self.callbackURL)
                self.cyphers.connect_items_to_webhook(webhook_id, tmp_tokens, "Token", "TokensWebhook")

    def run(self):
        self.process_addresses_removals()
        self.process_addresses_additions()

if __name__ == "__main__":
    P = WebhooksProcessor()
    P.run()