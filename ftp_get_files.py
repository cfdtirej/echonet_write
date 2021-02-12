import time
import datetime
from dataclasses import dataclass
from ftplib import FTP
from pathlib import Path
from typing import List, Dict

import yaml


conf_yaml = Path(__file__).parent / 'conf.yaml'


with open(conf_yaml, 'r') as f:
    conf = yaml.safe_load(f)

FTP().set_pasv(False)
class HpcsFTP(FTP):
    def __init__(self):
        pass

