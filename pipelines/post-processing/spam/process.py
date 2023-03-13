from ...helpers import Alchemy
from ..helpers import Processor
from .cyphers import SpamCyphers



class SpamProcessor(Processor):
    """This class reads from the Neo4J instance for Twitter nodes to call the Twitter API and retreive extra infos"""
    def __init__(self):
        self.cyphers = SpamCyphers()
        self.alchemy = Alchemy()
        super().__init__("spam")

    def get_mainnet_spam_contracts(self):
        for chain in ["ethereum", "polygon"]:
            response_data = self.alchemy.getSpamContracts(chain=chain)
            if response_data:
                spamAddresses = [address.lower() for address in response_data]
                self.cyphers.label_spam_contracts(spamAddresses)            

    def run(self):
        self.get_mainnet_spam_contracts()

if __name__ == '__main__':
    processor = SpamProcessor()
    processor.run()
