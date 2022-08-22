import requests
import argparse
import boto3
from dotenv import load_dotenv
from datetime import datetime
import os
import json
from helpers.arweave import getArweaveTxs, requestData
import multiprocessing
import contextlib
import joblib
from tqdm import tqdm
from joblib import Parallel, delayed
from pathlib import Path


@contextlib.contextmanager
def tqdm_joblib(tqdm_object):
    """Context manager to patch joblib to report into tqdm progress bar given as argument"""

    class TqdmBatchCompletionCallback(joblib.parallel.BatchCompletionCallBack):
        def __call__(self, *args, **kwargs):
            tqdm_object.update(n=self.batch_size)
            return super().__call__(*args, **kwargs)

    old_batch_callback = joblib.parallel.BatchCompletionCallBack
    joblib.parallel.BatchCompletionCallBack = TqdmBatchCompletionCallback
    try:
        yield tqdm_object
    finally:
        joblib.parallel.BatchCompletionCallBack = old_batch_callback
        tqdm_object.close()


def exportData(all_txs):
    final = []
    failed = []

    now = datetime.now()
    for tx in all_txs:
        try:
            s3Object = s3.Object(BUCKET, f"mirror/data/{tx}.json")
            body = json.loads(json.loads(s3Object.get()["Body"].read().decode("utf-8")))
            body["id"] = tx
            final.append(body)
        except:
            failed.append(tx.split("/")[-1].split(".")[0])

    def generate_data(row):
        try:
            if row["authorship"]:
                newRow = {
                    "contributor": row["authorship"].get("contributor", ""),
                    "publication": row["content"].get("publication", ""),
                    "title": row["content"].get("title", ""),
                    "body": row["content"].get("body", ""),
                    "timestamp": row["content"].get("timestamp", 0),
                    "transaction": row["id"],
                }
                return newRow
        except:
            print(row)
            return ""

    final = [generate_data(row) for row in final]
    final = [row for row in final if row != ""]

    s3Object = s3.Object(BUCKET, f"mirror/final/{now.strftime('%m-%d-%Y')}/data.json")
    s3Object.put(Body=json.dumps(final))

    s3Object = s3.Object(BUCKET, f"mirror/final/{now.strftime('%m-%d-%Y')}/failed.json")
    s3Object.put(Body=json.dumps(failed))

    print("Exported {} transactions".format(len(final)))
    print("Failed to export {} transactions".format(len(failed)))

    return final


def reqData(i):
    r = requests.get(f"https://arweave.net/{i}", allow_redirects=True)
    try:
        body = r.text
        return body

    except:
        print("Error getting data")
        return ""


def fetchData(item):
    x = reqData(item[1])
    return (x, item[1])


load_dotenv()
s3 = boto3.resource("s3")
client = boto3.client("s3")
BUCKET = "chainverse"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--start", help="Start block", type=int, default=0)
    parser.add_argument("-e", "--end", help="End block", type=int, default=0)
    parser.add_argument("-a", "--all", help="Export all data", action="store_true")

    args = parser.parse_args()

    if args.end == 0:
        r = requests.get("https://arweave.net/info")
        x = json.loads(r.text)
        end_block = x["blocks"]
    else:
        end_block = args.end

    if args.start != 0:
        start_block = args.start
    else:
        os.chdir(os.path.abspath(os.path.dirname(__file__)))
        if Path("start_block.txt").is_file():
            with open("start_block.txt", "r") as f:
                start_block = int(f.read())
        else:
            raise Exception("No start block file found")

    print(f"Getting data from {start_block} to {end_block}")

    tickets = getArweaveTxs(start_block, end_block, 1000)

    all_ids = set()
    cleanedTickets = []
    for ticket in tickets:
        transaction_id = ticket["node"]["id"]
        if transaction_id in all_ids:
            continue
        all_ids.add(transaction_id)
        contributer = ticket["node"]["tags"][2]["value"]

        cleanedTickets.append([contributer, transaction_id])

    uniqCleanedTickets = cleanedTickets
    print(f"{len(uniqCleanedTickets)} unique tickets found")

    with tqdm_joblib(tqdm(desc="Fetching Data", total=len(uniqCleanedTickets))) as progress_bar:
        fetched_data = Parallel(n_jobs=-1)(delayed(fetchData)(item) for item in uniqCleanedTickets)
    print(f"{len(fetched_data)} data fetched")

    # fetched_data = fetchData(uniqCleanedTickets)
    for entry in fetched_data:
        s3Object = s3.Object(BUCKET, "mirror/data/{}.json".format(entry[1]))
        s3Object.put(Body=json.dumps(entry[0]))

    all_txs = [entry[1] for entry in fetched_data]

    if args.all:
        all_txs = []
        paginator = client.get_paginator("list_objects_v2")
        result = paginator.paginate(Bucket=BUCKET, Prefix="mirror/data/")
        for page in result:
            for key in page["Contents"]:
                keyString = key["Key"]
                tx = keyString.split("/")[-1].split(".")[0]
                all_txs.append(tx)

    exportData(all_txs)

    # update end block for next run
    with open("start_block.txt", "w") as f:
        f.write(str(end_block))

