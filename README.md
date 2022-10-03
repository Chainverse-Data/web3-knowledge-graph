# DiamondDAO scraping and ingesting pipelines

This repo has scripts for scraping various data sources and ingesting this data into a neo4j graph database.

The code is organized into three main python modules: **ingestion**, **scraping**, **analytics**. There are scrict design guidelines to allow for easy maintenance and expandability.

## Design strategy
The code stack follows a python module approach such that one can call the following: `python3 -m module_name.service_name.action`. For example, to scrape GitCoin `python3 -m scraping.gitcoin.scrape`.

## Install
It is recommended to created a virtual python environment using **Python 3.10**. Then install all requirements with `pip3 install -r requierements.txt`.

## Requirements
You will need to define the two following Environment variable for the module to be running:
NEO_URI=[The URL of the NEO4J instance]
NEO_USERNAME=[The user name of the DB]
NEO_PASSWORD=[The password to the DB]
NEO_DB=[Optional Value: If the DB is not set by the URI, or you want to target another DB]
ETHERSCAN_API_KEY=[Your Etherscan API Key]
ALCHEMY_API_KEY=[Your Alchemy API Key]
AWS_BUCKET_PREFIX=[A unique bucket name prefix]
AWS_DEFAULT_REGION=[The targeted AWS Region]
AWS_ACCESS_KEY_ID=[Your AWS ID]
AWS_SECRET_ACCESS_KEY=[Your AWS Key]
ALLOW_OVERRIDE=1 [Set to 1 if you want to allow overiding saved data on S3, else remove or set to 0]
LOGLEVEL=[DEBUG|INFO|WARNING|ERROR] python logging library log level

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

## The Scraper class
**Work In Progress**
The scraper class provides the minimum set of functions required for scraping:
```python

class Scraper:
    def __init__(self, bucket):
        self.data = {} #This variable is the export variable. Save the scraped data to this variable. 
        self.bucket = bucket
        ...

    def get_request(url, params, header):
        # This function is useful to make GET requests to API endpoints, use it preferentially, it is resilient to network errors such as timeouts.
        ...

    def save_data():
        # This function allows to generate the calls to save the scraped data to amazon S3. This exports the self.data variable as a JSON file to S3.
        ...
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
**Work In Progress**
The ingestion module can be either imported or ran as a package using the `python -m` command. Every internal package implements a `ingest.py` file which will run the ingestion for the particular service. Ingestors must read from the S3 bucket created by the scrapers.

## Design strategy
- Each ingestor must inherit from the `Ingestor` class defined in the `helpers/ingestor.py`. 
- Each service must be in its own folder
- Each service should not require CLI arguments.
- Each service must have a README.me file that defines the script usage arguments if any.
- Each service must have an explanation of what the ingestor does in the README.md
- Each service must have a `__init__.py` file exporting the ingestor and all other defined classes for use in the module.
- Each service must have a `ingest.py` file as the main executable, which can accepts CLI parameters with the argparse library.
- Each service must read its data from the unique S3 bucket created by the corresponding scraper.
- Each service must read and save its necessary metadata, such as the last data file read, to the same bucket under `ingestor_metadata.json`

## The Ingestor class
**Work In Progress**
The ingestor class provides the minimum set of functions required for ingesting:
```python

class Ingestor:
    def __init__(self):
        ...
```

## Implemented services
### To be cleaned
- Miror.xyz: `ingestion/miror`
- Snapshot: `ingestion/snapshot`
- DAOHaus: `ingestion/daohaus` 
- Twitter: `ingestion/twitter`
- Wallets: `ingestion/wallets`

## Services to be added:
- GitCoin

# TODO
- [] Remove the env.sample files
- [] Convert the Mirror scraper to the scraper module architecture
- [] Convert the Snapshot scraper to the scraper module architecture
- [] Convert the Snapshot scraper to the scraper module architecture
