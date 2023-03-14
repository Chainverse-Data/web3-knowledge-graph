
from . import S3Utils
from . import Utils
from . import Web3Utils
from . import Requests
from . import Multiprocessing

class Base(Requests, S3Utils, Multiprocessing, Utils, Web3Utils):
    def __init__(self, chain) -> None:
        Requests.__init__(self)
        S3Utils.__init__(self)
        Multiprocessing.__init__(self)
        Utils.__init__(self)
        Web3Utils.__init__(self, chain=chain)