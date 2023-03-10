import logging
import time
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import gql
from gql.transport.aiohttp import AIOHTTPTransport, log as gql_log

gql_log.setLevel(logging.WARNING)

class Requests:
    "This section handles all requests and API calls."
    def __init__(self) -> None:
        pass

    def get_request(self, 
                    url, 
                    params=None, 
                    headers=None, 
                    allow_redirects=True, 
                    decode=True, 
                    json=False, 
                    ignore_retries=False, 
                    retry_on_403=False, 
                    retry_on_404=True, 
                    counter=0, 
                    max_counter=10):
        """This makes a GET request to a url and return the data.
        It can take params and headers as parameters following python's request library.
        The method returns the raw request content, you must then parse the content with the correct parser."""
        time.sleep(counter * max_counter)
        if counter > 10:
            return None
        try:
            r = requests.get(url, params=params, headers=headers,
                            allow_redirects=allow_redirects, verify=False)
            if not retry_on_404 and r.status_code == 404:
                return None
            elif retry_on_403 and (r.status_code == 403 or "403 Forbidden" in r.content.decode("UTF-8")):
                logging.error(f"403 forbidden detected: {r.content.decode('UTF-8')}")
                return self.get_request(url, params=params, headers=headers, allow_redirects=allow_redirects, counter=counter + 1)
            elif r.status_code != 200 and not ignore_retries:
                logging.error(f"Status code not 200: {r.status_code} Retrying in {counter*10}s (counter = {counter})...")
                return self.get_request(url, params=params, headers=headers, allow_redirects=allow_redirects, counter=counter + 1)
            if not json and decode:
                return r.content.decode("UTF-8")
            if json:
                return r.json()
            return r
        except Exception as e:
            logging.error(f"An unrecoverable exception occurred: {e}")
            return self.get_request(url, params=params, headers=headers, allow_redirects=allow_redirects, counter=counter + 1)

    def post_request(self, url, data=None, json=None, headers=None, return_json=False, counter=0):
        time.sleep(counter * 10)
        if counter > 10:
            return None
        try:
            r = requests.post(url, data=data, json=json, headers=headers, verify=False)
            if r.status_code <= 200 and r.status_code > 300:
                logging.error(f"Status code not 200: {r.status_code} Retrying {counter*10}s (counter = {counter})...")
                return self.post_request(url, data=data, json=json, headers=headers, counter=counter + 1)
            if return_json:
                return r.json()
            else:
                return r.content.decode("UTF-8")
        except Exception as e:
            logging.error(f"An unrecoverable exception occurred: {e}")
            return self.post_request(url, data=data, json=json, headers=headers, counter=counter + 1)

    def call_the_graph_api(self, graph_url, query, variables, result_names, counter=0):
        time.sleep(counter)
        if counter > 10:
            time.sleep(counter)
            return None
        gql_query =  gql.gql(query)
        transport = AIOHTTPTransport(url=graph_url)
        client = gql.Client(transport=transport, fetch_schema_from_transport=True)
        try:
            result = client.execute(gql_query, variables)
            for result_name in result_names:
                if result.get(result_name, None) == None:
                    logging.error(f"The Graph API did not return {result_name}, counter: {counter}")
                    return self.call_the_graph_api(graph_url, query, variables, result_names, counter=counter + 1)
        except Exception as e:
            logging.error(f"An exception occured getting The Graph API {e} counter: {counter} client: {client}")
            return self.call_the_graph_api(graph_url, query, variables, result_names, counter=counter + 1)
        return result