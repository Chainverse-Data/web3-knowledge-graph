import logging
import os
from dotenv import load_dotenv

load_dotenv()

if "LOGLEVEL" in os.environ:
    logging.basicConfig(level=os.environ["LOGLEVEL"])
