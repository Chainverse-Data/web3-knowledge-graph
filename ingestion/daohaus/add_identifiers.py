import logging
import os
import boto3
from dotenv import load_dotenv
import sys
from pathlib import Path
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[2]))
from ingestion.helpers.s3 import *
from ingestion.helpers.graph import ChainverseGraph

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

uri = os.getenv("NEO_URI")
username = os.getenv("NEO_USERNAME")
password = os.getenv("NEO_PASSWORD")
conn = ChainverseGraph(uri, username, password)

client = boto3.client("s3")
resource = boto3.resource("s3")
s3 = boto3.client("s3")
BUCKET = "chainverse"

SPLIT_SIZE = int(os.environ.get("SPLIT_SIZE", 20000))

def add_names(url, conn):

    name_query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS daos
                    MERGE (d:DaoHaus:Dao:Entity {{daohausId: daos.contract}})
                    SET d.name = daos.name
                    return count(d)
                """

    x = conn.query(name_query)
    print("names added", x)

if __name__ == "__main__":

    df = pd.read_csv("ingestion/daohaus/daohaus_checkpoint_three.csv")
    df = df.drop_duplicates(subset=["contract"])

    # add names for daos
    name_dao_chunks = split_dataframe(df, SPLIT_SIZE)

    for idx, space_batch in enumerate(name_dao_chunks):
        url = write_df_to_s3(space_batch, BUCKET, f"neo/daohaus/nodes/names/name-{idx * SPLIT_SIZE}.csv", resource, s3)
        add_names(url, conn)
        set_object_private(BUCKET, f"neo/daohaus/nodes/names/name-{idx * SPLIT_SIZE}.csv", resource)

    conn.close()