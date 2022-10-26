# mirror-scraper

Scrape Mirror Articles

The `scrape.py` file is the primary file used to collect and scrape the mirror articles, then saving them to s3 Bucket.

The script takes 3 arguments, `-s` is the optional start block (otherwise the file will read the start block from the file `start_block.txt`), `-e` is the optional end block (otherwise the file will get the current highest block and fetch till there), and `-c` which will export all of the current data files (articles) into the single exported data json, otherwise it will only export those collected by this specific run of the script.
