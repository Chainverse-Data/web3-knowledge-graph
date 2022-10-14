from datetime import datetime
from neo4j import GraphDatabase
import os
import logging

class Cypher:
    def __init__(self, database=None):
        self.database = database
        if "NEO_DB" in os.environ:
            self.database = os.environ["NEO_DB"]

        self.create_constraints()
        self.create_indexes()

    def get_driver(self):
        neo4j_driver = GraphDatabase.driver(
            os.environ["NEO_URI"],
            auth=(os.environ["NEO_USERNAME"],
                  os.environ["NEO_PASSWORD"]))
        return neo4j_driver

    def create_constraints(self):
        logging.warning("This function should be implemented in the children class.")

    def create_indexes(self):
        logging.warning("This function should be implemented in the children class.")

    def query(self, query, parameters=None):
        neo4j_driver = self.get_driver()
        assert neo4j_driver is not None, "Driver not initialized!"
        session = None
        response = None
        try:
            session = neo4j_driver.session(
                database=self.database) if self.database is not None else neo4j_driver.session()
            response = list(session.run(query, parameters))
        except Exception as e:
            logging.error("Query failed:", e)
        finally:
            if session is not None:
                session.close()
        neo4j_driver.close()
        return response

    def sanitize_text(self, string):
        if string:
            return string.rstrip().replace('\r', '').replace('\\', '').replace('"', '').replace("'", "").replace("`", "").replace("\n", "")
        else:
            return ""
