import re


class Utils:
    def __init__(self) -> None:
        pass

    def is_valid_address(self, address):
        check = re.compile("^0x[a-fA-F0-9]{40}$")
        if check.match(address):
            return True
        return False

    def is_zero_address(self, address):
        if self.is_valid_address(address):
            if int(address, 16) == 0:
                return True
            return False
        return False

    def str2bool(self, v):
        if v.lower() in ("yes", "true", "t", "y", "1"):
            return True
        elif v.lower() in ("no", "false", "f", "n", "0"):
            return False
        else:
            raise ValueError("String value not covered.")
