import logging

def count_query_logging(function):
    "A function wrapped with this decorator must return a count of affected objects."
    def wrapper():
        logging.info(f"Ingesting with: {function.func_name}")
        count = function()
        logging.info(f"Created or merged: {count}")
        return count
    return wrapper
