# Summary

This script takes care of adding new wallets to watch and remove wallets that should not be watched anymore.

# Ontology

Wallets that need to be watched should have the property: `notifySelected` set to true.
    - (wallet:Wallet)
        wallet.notifySelected = true

Webhooks are stored in the DB and wallets watched by each webhook ID are saved in the following ontology:
    - (webhook:Alchemy:AddressesWebhook)
        - webhook.id: The alchemy webhook id
        - webhook.callbackUrl: The callback URL set for this webhook
        - webhook.network: The network this webhook watches

Wallets watched by a webhook are linked via:
    - (wallet:Wallet)-[:IS_WATCHED_BY]->(webhook:Alchemy:AddressesWebhook)
        
