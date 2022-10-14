from ..helpers import Processor
from .cyphers import TwitterCyphers

class TwitterPostProcess(Processor):
    """This class reads from the Neo4J instance for Twitter nodes to call the Twitter API and retreive extra infos"""
    def __init__(self):
        super().__init__()
        self.cyphers = TwitterCyphers()

    # Do some stuff