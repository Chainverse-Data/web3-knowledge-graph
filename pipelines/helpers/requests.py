import logging
import time
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class Requests:
    "This section handles all requests and API calls."
    def __init__(self) -> None:
        pass

    def get_request(self, url, params=None, headers=None, allow_redirects=True, decode=True, json=False, ignore_retries=False, counter=0):
        """This makes a GET request to a url and return the data.
        It can take params and headers as parameters following python's request library.
        The method returns the raw request content, you must then parse the content with the correct parser."""
        time.sleep(counter * 10)
        if counter > 10:
            return None
        try:
            r = requests.get(url, params=params, headers=headers,
                            allow_redirects=allow_redirects, verify=False)
            if r.status_code != 200 and not ignore_retries:
                logging.error(f"Status code not 200: {r.status_code} Retrying in {counter*10}s (counter = {counter})...")
                return self.get_request(url, params=params, headers=headers, allow_redirects=allow_redirects, counter=counter + 1)
            if "403 Forbidden" in r.content.decode("UTF-8") and not ignore_retries:
                logging.error(f"Status code not 200: {r.status_code} Retrying in {counter*10}s (counter = {counter})...")
                return self.get_request(url, params=params, headers=headers, allow_redirects=allow_redirects, counter=counter + 1)
            if decode:
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
