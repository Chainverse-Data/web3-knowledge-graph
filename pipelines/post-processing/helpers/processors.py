from ...helpers import Base

class Processor(Base):
    def __init__(self, bucket_name, chain="ethereum"):
        Base.__init__(self, bucket_name=bucket_name, metadata_filename="processor_metadata.json", load_data=False, chain=chain)
        
        try:
            self.cyphers
        except:
            raise ValueError("Cyphers have not been instanciated to self.cyphers")

    def run(self):
        "Main function to be called. Every postprocessor must implement its own run function!"
        raise NotImplementedError("ERROR: the run function has not been implemented!")