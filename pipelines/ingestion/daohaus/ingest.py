from ..helpers import Ingestor

class DAOHausIngestor(Ingestor):
    def __init__(self, bucket_name="daohaus"):
        super().__init__(bucket_name)