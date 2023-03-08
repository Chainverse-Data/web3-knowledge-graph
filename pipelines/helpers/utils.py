import re


class Utils:
    def __init__(self) -> None:
        pass

    def str2bool(self, v):
        if v.lower() in ("yes", "true", "t", "y", "1"):
            return True
        elif v.lower() in ("no", "false", "f", "n", "0"):
            return False
        else:
            raise ValueError("String value not covered.")
