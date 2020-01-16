import time
import logging
import configparser
from config_reader import VsailConfigReader
import gzip
import asyncio
from vsail_data_parser import VsailDataParser
reader = VsailConfigReader()
#print(reader.redis_host)
import binascii



s = '23 23 02 fe 38 36 36 33 32 33 30 33 31 39 37 36 34 38 32 0d 0a 01 00 4d 14 01 0e 10 04 11 05 00 06 ee ab 32 02 64 a6 5c 80 3b 66 39 80 6f 9a 1d 1a 80 40 00 00 00 11 18 16 00 00 00 00 00 0e 18 16 00 00 00 00 00 0e 18 16 00 00 00 00 00 0f 18 16 00 00 00 00 00 0f 18 16 00 00 00 00 00 0e 18 16 00 00 26 fd ed'
parser = VsailDataParser(s)
print(parser.translate_to_json())