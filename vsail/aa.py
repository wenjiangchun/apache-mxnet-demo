import time
import logging
import configparser
import pika
from config_reader import VsailConfigReader
import gzip
import asyncio
from vsail_data_parser import VsailDataParser
reader = VsailConfigReader()
#print(reader.redis_host)



message1 = '##BJSRDZNMKV1010028\00m9\00cP[fYnw@\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00w'
def str_to_hex(dt):
        return ' '.join([hex(ord(c)).replace('0x', '').zfill(2) for c in dt])

parser = VsailDataParser(message1)
print(parser.get_bus_vin())
