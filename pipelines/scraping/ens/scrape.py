from ..helpers import Scraper


class ENSScraper(Scraper):
    def __init__(self, bucket_name="ens-records", allow_override=None):
        super().__init__(bucket_name, allow_override)