import dataclasses
from datetime import datetime

@dataclasses.dataclass
class ConfigFile:
    filename: str
    last_run: datetime