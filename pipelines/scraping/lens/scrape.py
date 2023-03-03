import time
from ..helpers import Scraper
import gql
import logging
from gql.transport.aiohttp import AIOHTTPTransport, log as gql_log

gql_log.setLevel(logging.WARNING)


class LensScraper(Scraper):
    def __init__(self, bucket_name="lens", allow_override=True):
        super().__init__(bucket_name, allow_override=allow_override)

        self.graph_url = "https://api.thegraph.com/subgraphs/name/anudit/lens-protocol"
        self.headers = {"accept": "application/json", "content-type": "application/json"}

        self.interval = 1000
        self.data["profiles"] = []

    def call_the_graph_api(self, graph_url, query, variables, counter=0):
        time.sleep(counter)
        if counter > 20:
            return None

        transport = AIOHTTPTransport(url=graph_url)
        client = gql.Client(transport=transport, fetch_schema_from_transport=True)
        try:
            result = client.execute(query, variables)
            if result.get("profiles", None) == None:
                logging.error(f"The Graph API did not return profiles, counter: {counter}")
                return self.call_the_graph_api(graph_url, query, variables, counter=counter + 1)
        except Exception as e:
            logging.error(f"An exception occured getting The Graph API {e} counter: {counter} client: {client}")
            return self.call_the_graph_api(graph_url, query, variables, counter=counter + 1)
        return result

    def get_profiles(self):
        logging.info(f"Getting profiles from {self.graph_url}...")
        skip = 0
        cutoff_date = self.metadata.get("cutoff_date", 0)

        while True:
            if skip > 5000:
                skip = 0
                cutoff_date = self.data["profiles"][-1]["createdOn"]
            variables = {"first": 1000, "skip": skip, "cutoff": cutoff_date}

            profiles_query = gql.gql(
                """
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
            )
            result = self.call_the_graph_api(self.graph_url, profiles_query, variables)
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
