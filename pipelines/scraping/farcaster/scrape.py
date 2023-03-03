import time
from ..helpers import Scraper
import gql
import logging
from gql.transport.aiohttp import AIOHTTPTransport, log as gql_log

gql_log.setLevel(logging.WARNING)


class FarcasterScraper(Scraper):
    def __init__(self, bucket_name="farcaster", allow_override=True):
        super().__init__(bucket_name, allow_override=allow_override)

        self.graph_url = "https://api.thegraph.com/subgraphs/name/0xsarvesh/farcaster-goerli"
        self.headers = {"accept": "application/json", "content-type": "application/json"}

        self.interval = 1000
        self.data["users"] = []

    def call_the_graph_api(self, graph_url, query, variables, counter=0):
        time.sleep(counter)
        if counter > 20:
            return None

        transport = AIOHTTPTransport(url=graph_url)
        client = gql.Client(transport=transport, fetch_schema_from_transport=True)
        try:
            result = client.execute(query, variables)
            if result.get("users", None) == None:
                logging.error(f"The Graph API did not return users, counter: {counter}")
                return self.call_the_graph_api(graph_url, query, variables, counter=counter + 1)
        except Exception as e:
            logging.error(f"An exception occured getting The Graph API {e} counter: {counter} client: {client}")
            return self.call_the_graph_api(graph_url, query, variables, counter=counter + 1)
        return result

    def get_profiles(self):
        logging.info(f"Getting users from {self.graph_url}...")
        skip = 0
        cutoff_id = ""

        while True:
            if skip > 5000:
                skip = 0
                cutoff_id = self.data["users"][-1]["id"]
            variables = {"first": self.interval, "skip": skip, "cutoff": cutoff_id}

            locks_query = gql.gql(
                """
                query($first: Int!, $skip: Int!, $cutoff: ID!) {
                    users(first: $first, skip:$skip, orderBy:id, orderDirection:asc, where:{id_gt: $cutoff}) {
                            id
                            address
                            fname
                            fId
                            url
                        }
                    }
                    """
            )
            result = self.call_the_graph_api(self.graph_url, locks_query, variables)
            if result["users"] == []:
                logging.info(f"Got {len(self.data['users'])} users ending scrape")
                break

            self.data["users"].extend(result["users"])
            skip += self.interval

    def run(self):
        self.get_profiles()
        self.save_metadata()
        self.save_data()


if __name__ == "__main__":
    scraper = FarcasterScraper()
    scraper.run()
    logging.info("Run complete!")
