from ..helpers import Scraper
import logging


class LensScraper(Scraper):
    def __init__(self, bucket_name="lens"):
        super().__init__(bucket_name)

        self.graph_url = "https://api.thegraph.com/subgraphs/name/anudit/lens-protocol"
        self.headers = {"accept": "application/json", "content-type": "application/json"}

        self.interval = 1000
        self.data["profiles"] = []

    def get_profiles(self):
        logging.info(f"Getting profiles from {self.graph_url}...")
        skip = 0
        cutoff_date = self.metadata.get("cutoff_date", 0)

        while True:
            if skip > 5000:
                skip = 0
                cutoff_date = self.data["profiles"][-1]["createdOn"]
            variables = {"first": 1000, "skip": skip, "cutoff": cutoff_date}

            profiles_query = """
                query($first: Int!, $skip: Int!, $cutoff: BigInt!) {
                    profiles (first: $first, skip: $skip, orderBy: createdOn, orderDirection: asc, where: {createdOn_gt: $cutoff}) {
                        id
                        profileId
                        creator
                        owner
                        handle
                        createdOn
                        }
                    }
                    """
            result = self.call_the_graph_api(self.graph_url, profiles_query, variables, ["profiles"])
            if result["profiles"] == []:
                logging.info(f"Got {len(self.data['profiles'])} profiles, ending scrape")
                break

            self.data["profiles"].extend(result["profiles"])
            self.metadata["cuttoff_date"] = self.data["profiles"][-1]["createdOn"]
            skip += 1000

    def run(self):
        self.get_profiles()
        self.save_data()
        self.save_metadata()


if __name__ == "__main__":
    scraper = LensScraper()
    scraper.run()
    logging.info("Run complete!")
