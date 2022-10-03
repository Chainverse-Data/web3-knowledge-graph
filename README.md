# DiamondDAO scraping and ingesting pipelines

This repo has scripts for scraping various data sources and ingesting this data into a neo4j graph database.

The code is organized into three main python modules: **ingestion**, **scraping**, **analytics**. There are scrict design guidelines to allow for easy maintenance and expandability.

## Design strategy
The code stack follows a python module approach such that one can call the following: `python3 -m module_name.service_name.action`. For example, to scrape GitCoin `python3 -m scraping.gitcoin.scrape`.

## Install
It is recommended to created a virtual python environment using **Python 3.10**. Then install all requirements with `pip3 install -r requierements.txt`.

## Requirements
You will need to define the following Environment variable for all the modules to be running:
```
AWS_BUCKET_PREFIX=[A unique bucket name prefix]
AWS_DEFAULT_REGION=[The targeted AWS Region]
AWS_ACCESS_KEY_ID=[Your AWS ID]
AWS_SECRET_ACCESS_KEY=[Your AWS Key]
LOGLEVEL=[DEBUG|INFO|WARNING|ERROR] python logging library log level
```

# Docker image
The module is packaged into a Docker image for cloud deployement, or local use. To build the image just launch `docker build . -t diamond-pipelines`.

You can then launch the modules through the docker image directly by replacing module, service and action with the desired pipeline module.
 `docker run --env-file .env diamond-pipeline python3 -m [module].[service].[action]`. 

**Make sure you have a `.env` file with all the environment variables set.**

For convenience, a docker-compose file is also available.

# The scraping module
The scrapping module can be either imported or ran as a package using the `python -m` command. Every internal package implements a `scrape.py` file which will run the scrapping for the particular service. 

## Design strategy
- Each scraper must inherit from the `Scraper` class defined in the `helpers/scraper.py`. 
- Each service must be in its own folder
- Each service must have a README.me file that defines the script usage arguments
- Each service must have an explanation of what the scraper does in the README.md
- Each service must have a `__init__.py` file exporting the scraper and all other defined classes for use in the module.
- Each service must have a `scrape.py` file as the main executable, which accepts CLI parameters with the argparse library
- Each service must save its data to a unique bucket with the following format: `data_[date].json`
- Each service must read and save its necessary metadata, such as the last block number scraped, to the same bucket under `scraper_metadata.json`

## Environment variables
You can set the following environement variables that will apply to all scraper modules.
```
ETHERSCAN_API_KEY=[Your Etherscan API Key]
ALCHEMY_API_KEY=[Your Alchemy API Key]
ALLOW_OVERRIDE=1 [Set to 1 if you want to allow overiding saved data on S3, else remove or set to 0]
```

## The Scraper class
The scraper class provides the minimum set of functions required for scraping:
```python

# This class is the base class for all scrapers.
# Every scraper must inherit this class and define its own run function
# The base class takes care of loading the metadata and checking that data was not already ingested for today
# The data and metadata are not saved automatically at the end of the execution to allow for user defined save points.
# Use the save_metadata and save_data functions to automatically save to S3
# During the run function, save the data into the instance.data field. 
# The data field is a dictionary, define each root key as you would a mongoDB index.

class Scraper:
    def __init__(self, bucket):
        self.metadata = {} #This variable is the metadata variables. You can use it to store the last state, last block number or any other piece of metadata you need to store.
        self.data = {} #This variable is the export variable. Save the scraped data to this variable. It will be store as scraper_metadata.json on S3.
        self.s3 = S3Utils() # S3 helper class to store dict as json, read json, save pandas DF as CSV etc. See helpers/s3.py for more information. 
        ...

    def run(self):
        #Main function to be called. Every scrapper must implement its own run function !
        ...

    def get_request(url, params, header):
        # This function is useful to make GET requests to API endpoints, use it preferentially, it is resilient to network errors such as timeouts.
        ...

    def post_request(url, data=None, json=None, headers=None):
        # This functions sends a POST request to the url, with data (url attributes), json (a python dictionary) and headers as optional fields. If the status code is a success >=200 and <300, returns the content.
        ...

    def read_metadata(self):
        #Access the S3 bucket to read the metadata and returns a dictionary that corresponds to the saved JSON object. This is called automatically.
        ...

    def save_data():
        # This function allows to generate the calls to save the scraped data to amazon S3. This exports the self.data variable as a JSON file to S3.
        ...
    
    def save_metadata(self):
        #Saves the current metadata to S3. This needs to be called manually when you want to save a checkpoint. Usually, after the save_data is succesfull.
        ...
```

```python
class ExampleScraper(Scraper):
    def __init__(self):
        super().__init__("example")
        self.last_block_number = 10245999 # block of contract creation
        if "last_block_number" in self.metadata:
            self.last_block_number = self.metadata["last_block_number"]

        self.url = "https://the.service.com/api/something?block_nb={}"

        def get_all_logs(self):
            logging.info("Doing the thing!")
            self.data["events"] = []
            while True:
                content = self.get_request(self.url.format(self.last_block_number))
                data = json.loads(content)
                if len(data) == 0:
                    break
                self.data["events"] += content["events"]
                self.last_block_number += content["next"]
                self.metadata["last_block_number"] = self.last_block_number
            logging.info("The thing is done!")

        def run(self):
            self.get_all_logs()
            self.save_data()
            self.save_metadata()

if __name__ == "__main__":
    scraper = ExampleScraper()
    scraper.run()
```

## Implemented services
- GitCoin: `scraping/gitcoin`
### To be cleaned
- Miror.xyz: `scraping/miror`
- Snapshot: `scraping/snapshot`

## Services to be added:
- DAOHaus
- Twitter
- Wallets

# The ingestion module
The ingestion module can be either imported or ran as a package using the `python -m` command. Every internal package implements a `ingest.py` and a `cyphers.py`. the `ingest.py` file will run the ingestion for the particular service by reading the `data_...json` files saved to S3. The `cyphers.py` file contains all the Neo4J queries as functions. Ingestors must read from the same S3 bucket created by the scrapers. 

In a normal setting, for efficiency, the data is processed into a pandas dataframe, that is then saved to S3 as a CSV. The class takes care of splitting the file if necessary to stay within the 10Mb limit of Neo4J. The CSV files are then used in the `cyphers.py` through queries containing the `LOAD CSV FROM {url} AS data` line. For convenience, the ingestor save CSV function always returns an array of urls, even if the dataset does not need to be split. 

## Design strategy
- Each service must have a `ingest.py` file as the main executable.
- Each service must have a `cyphers.py` file that contains the Neo4J queries.
- Each ingestor `ingest.py` must inherit from the `Ingestor` class defined in the `helpers/ingestor.py`. 
- Each ingestor `cyphers.py` must inherit from the `Cypher` class defined in the `helpers/cypher.py`. 
- Each service must be in its own folder
- Each service should use ENV as arguments.
- Each service should not require CLI arguments.
- Each service must have a README.me file that defines the script usage arguments if any.
- Each service must have an explanation of what the ingestor does in the README.md
- Each service must have a `__init__.py` file.
- Each service must read its data from the unique S3 bucket created by the corresponding scraper.
- Each service must read and save its necessary metadata, such as the last data file read, to the same bucket under `ingestor_metadata.json`

## Environment variables
You can define the following environment variables for any ingestor.

```
NEO_URI=[The URL of the NEO4J instance]
NEO_USERNAME=[The user name of the DB]
NEO_PASSWORD=[The password to the DB]
NEO_DB=[Optional Value: If the DB is not set by the URI, or you want to target another DB]
INGEST_FROM_DATE=[YYYY-MM-DD] #Filter the data files from the scraper from (inclusive) this date
INGEST_TO_DATE=[YYYY-MM-DD] #Filter the data files from the scraper to (inclusive) this date
```

## The Ingestor class
The ingestor class provides the minimum set of functions required for ingesting:
```python

class Ingestor:
    def __init__(self, bucket_name, start_date=None, end_date=None):
        self.runtime # Convenience variable to access the current runtime
        self.asOf # Formated string to add the asOf tag to nodes and edges
        
        self.s3 = S3Utils() # Utility to read and write to S3

        self.cyphers # Variable that needs to be defined in the init of the chlidren class BEFORE the super().__init__() is called.

        self.metadata # Dictionary of metadatas

        self.start_date # If filtering the ingestion for a particular date. Set automatically from ENV vars, or can be passed as an init argument. 
        self.end_date # If filtering the ingestion for a particular date. Set automatically from ENV vars, or can be passed as an init argument.

        self.scraper_data # The data from the scraper will be automatically loaded to this variable.

        self.ingest_data # Convenience variable to store processed data. Optional.

    def run(self):
        "Main function to be called. Every ingestor must implement its own run function!"
        ...

    def set_start_end_date(self):
        "Sets the start and end date from either params, env or metadata"
        ...

    def read_metadata(self):
        "Access the S3 bucket to read the metadata and returns a dictionary that corresponds to the saved JSON object"
        ...

    def save_metadata(self):
        "Saves the current metadata to S3"
        ...

    def load_data(self):
        "Loads the data in the S3 bucket from the start date to the end date (if defined)"
        ...
```

```python
class ExampleIngestor(Ingestor):
    
    def __init__(self, )
        self.cyphers = ExampleCyphers()
        super().__init__("example")

    def ingest_data(self):
        data = self.process_data()
        urls = self.s3.save_json_as_csv(
            data["events"], self.bucket_name, f"ingestor_events_{self.asOf}")
        count = self.cyphers.example_query(urls)
        assert(count == len(data["events"]), "Not all data was ingested!")

    def process_data(self):
        data = {
            "events": []
        }
        for element in self.data["events"]:
            tmp = {
                "id": element["id"],
                "title": element["title"].replace('"', '')
            }
            data["events"].append(tmp)
        return data

    def run(self):
        self.ingest_data()
        self.save_metadata()

if __name__ == "__main__":
    ingestor = ExampleIngestor()
    ingestor.run()
```

## The Cyphers class

The cyphers 

```python
class Cypher:
    def __init__(self, database=None):
        self.neo4j_driver # The neo4j driver to connect to the database.
        self.database # If set through the ENV or through parameter, the database to target from the Neo4J instance.

    def create_constraints(self):
        """This function must be redefined by the child class.
        In it you must input all the constraints that the ingestor must create.
        If your ingestor does not need any constraints, then you must still define a function that does nothing."""
        ...

    def create_indexes(self):
        """This function must be redefined by the child class.
        In it you must input all the constraints that the ingestor must create.
        If your ingestor does not need any constraints, then you must still define a function that does nothing"""
        ...

    def query(self, query, parameters=None):
        "Function to launch a query to Neo4J. Accepts parameters to be added to the query following the official parameters from neo4J setting."

    def close(self):
        "Function to close the connection to the database."

    def __del__(self):
        # Called upon class Deinstanciation or deletion to ensure the connection closes
```

```python
class ExampleCyphers(Cypher):
    def __init__(self):
        super().__init__()

    def create_constraints(self):
        "Exampe constraints function"
        wallet_query = """CREATE CONSTRAINT UniqueAddress IF NOT EXISTS FOR (wallet:Wallet) REQUIRE wallet.address IS UNIQUE"""
        self.query(wallet_query)

    def create_indexes(self):
        "Example index function"
        wallet_query = """CREATE INDEX UniqueAddress IF NOT EXISTS FOR (n:Wallet) ON (n.address)"""
        self.query(wallet_query)

    def example_query(self, urls):
        "Exampe query"
        logging.info("Doing Something")
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS data
                    MERGE(node:NodeLabel:NodeLabel2 {{id: data.id}})
                    ON CREATE set node.uuid = apoc.create.uuid(),
                        node.id = data.id,
                        node.title = data.title,
                        node.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        node.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    ON MATCH set node.title = data.title,
                        node.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    return count(node)
            """
            count += self.query(query)[0].value()
        logging.info(f"Created or merged: {count}")
        return count
```

## Implemented services

- GitCoin: `ingestion/gitcoin`

### To be cleaned
- Miror.xyz: `ingestion/miror`
- Snapshot: `ingestion/snapshot`
- DAOHaus: `ingestion/daohaus` 
- Twitter: `ingestion/twitter`
- Wallets: `ingestion/wallets`

# Contribute
To contribute a new service, clone the main branch, then create a new branch named after the service or analysis you are targetting. Create the scraper, ingestor, analysis or post processing scripts, then create a Pull request for code-review.

# TODO
- [ ] Remove the env.sample files
- [ ] Convert the Mirror scraper to the scraper module architecture
- [ ] Convert the Snapshot scraper to the scraper module architecture
- [ ] Convert the Snapshot scraper to the scraper module architecture
