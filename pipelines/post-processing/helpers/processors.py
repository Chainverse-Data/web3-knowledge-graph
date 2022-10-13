from ...helpers import Cypher

class Processor():
    def __init__(self):
        pass

    def run(self):
        "Main function to be called. Every postprocessor must implement its own run function!"
        raise NotImplementedError(
            "ERROR: the run function has not been implemented!")
