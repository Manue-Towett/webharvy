import os
import logging
from typing import Optional

class Logger:
    """Logs info, warning and error messages"""
    if os.path.isfile("./logs/logs.log"):
        with open("./logs/logs.log", "w") as f:
            pass
        
    def __init__(self, name: Optional[str]=None) -> None:
        if name is None:
            name = __class__.__name__
        
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)

        fmt = logging.Formatter(
            "%(name)s:%(levelname)s - %(message)s")
        
        self.__add_file_handler(fmt)

        self.__add_stream_handler(fmt)
    
    def info(self, message: str) -> None:
        self.logger.info(message)
    
    def warn(self, message: str) -> None:
        self.logger.warning(message)
    
    def error(self, message: str) -> None:
        self.logger.error(message)
    
    def __add_file_handler(self, fmt: logging.Formatter) -> None:
        f_handler = logging.FileHandler("./logs/logs.log")
        f_handler.setFormatter(fmt)
        f_handler.setLevel(logging.INFO)
        self.logger.addHandler(f_handler)

    def __add_stream_handler(self, fmt: logging.Formatter) -> None:
        s_handler = logging.StreamHandler()
        s_handler.setFormatter(fmt)
        s_handler.setLevel(logging.INFO)
        self.logger.addHandler(s_handler)