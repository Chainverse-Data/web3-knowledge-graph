# Chainverse Data Ingestion Framework

This repo has scripts for scraping various data sources and ingesting this data into the Chainverse Knowledge Graph.

The code is organized into four main python modules: **ingestion**, **scraping**, **analytics**, **post-processing**. There are scrict design guidelines to allow for easy maintenance and expandability.

**If you need a .env example ask Jordan or Leo!**

## Design strategy
The code stack follows a python module approach such that one can call the following: `python3 -m pipelines.[module_name].[service_name].[action]`. For example, to scrape GitCoin `python3 -m pipelines.scraping.gitcoin.scrape`.

## Install
It is recommended to created a virtual python or conda environment using **Python 3.10**. Then install all requirements with `pip3 install -r requierements.txt`.

## Requirements
You will need to define the following Environment variable for all the modules to be running:
```
AWS_BUCKET_PREFIX=[A unique bucket name prefix]
AWS_DEFAULT_REGION=[The targeted AWS Region]
AWS_ACCESS_KEY_ID=[Your AWS ID]
AWS_SECRET_ACCESS_KEY=[Your AWS Key]
LOGLEVEL=[DEBUG|INFO|WARNING|ERROR] python logging library log level
DEBUG=1 #Set the DEBUG flag to 1, this has a various amount of influences but mostly restricts scraping to small subset of the data. 
```

# Docker image
The module is packaged into a Docker image for cloud deployement, or local use. To build the image just launch `docker build . -t diamond-pipelines`.

You can then launch the modules through the docker image directly by replacing module, service and action with the desired pipeline module.
 `docker run --env-file .env diamond-pipeline python3 -m pipelines.[module].[service].[action]`. 

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
OPTIMISTIC_ETHERSCAN_API_KEY=[Your Optimism API Key]
ALCHEMY_API_KEY=[Your Alchemy API Key]
ALLOW_OVERRIDE=1 [Set to 1 if you want to allow overiding saved data on S3, else remove or set to 0]
REINITIALIZE=1 [Set to 1 if you want to allow overiding saved data on S3, else remove or set to 0]
GRAPH_API_KEY=[Your API key from TheGraph]
```

# WICs
The WICs module (`pipelines/analytics/wics`) uses scraped data to apply labels to wallets. We separate the WIC module from the `Scraper` and `Ingestion` modules in order to define labels with multiple conditions: i/e `The SmartContractDev requires one of/all of these three conditions to be met`. By taking into account multiple conditions, we can apply wallet labels with more granularity and increased accuracy. 

# The Ingestion module
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



## Contribute
To contribute a new service, clone the main branch, then create a new branch named after the service or analysis you are targetting. Create the scraper, ingestor, analysis or post processing scripts, then create a Pull request for code-review.
