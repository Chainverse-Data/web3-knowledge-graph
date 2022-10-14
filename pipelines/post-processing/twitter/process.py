from ..helpers import Processor
from .cyphers import TwitterCyphers

class TwitterPostProcess(Processor):
    """This class reads from the Neo4J instance for Twitter nodes to call the Twitter API and retreive extra infos"""
    def __init__(self):
        super().__init__()
        self.cyphers = TwitterCyphers()

    def clean_twitter_nodes(self):
        self.cyphers.clean_twitter_nodes(self)

    def run(self):
        self.clean_twitter_nodes()

if __name__ == '__main__':
    processor = TwitterPostProcess()
    processor.run()