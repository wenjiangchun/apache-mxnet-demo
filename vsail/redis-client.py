import redis
import sched
import time
import psycopg2 as pg
import requests as req
import logging
import pika
import base64
import json
from vsail_data_parser import VsailDataParser
from config_reader import VsailConfigReader
import binascii

#该类作用：
#       1 初始化从数据库中加载车辆基本信息和位置信息到缓存
#       2 从rabbitMQ中监听队列，消费报文消息，同时将获取的报文解析后发送至平台

logging.basicConfig(filename="redis-client.log", filemode="w", format="%(asctime)s %(name)s:%(levelname)s:%(message)s", datefmt="%Y-%M-%d %H:%M:%S", level=logging.INFO)
class RedisClient(object):
    def __init__(self):
        self.reader = VsailConfigReader()
        #获取redis连接
        self.rs_conn = self.init_redis()
        #获取MQ连接
        self.rq_conn = self.init_rabbitMq()
        #获取DB连接
        db_conn, db_curs = self.init_db()
        logging.info('get bus info from postgresql') 
        try:
            query_bus_sql = 'select b.id, b.create_time, b.vin, b.bus_num, b.driving_num, b.engine_num, b.motor_name, b.motor_num, b.regist_num, g.full_name, r.full_name as root_full_name, g.id, r.id from v_bus b left join sys_group g on b.group_id=g.id left join sys_group r on b.root_group_id=r.id where b.deleted=false'
            db_curs.execute(query_bus_sql)
            dataes = db_curs.fetchall()
            for d in dataes:
                bus = {}
                vin = d[2]
                bus['vin'] = d[2]
                bus['id'] = d[0]
                bus['create_time'] = str(d[1])
                bus['busNum'] = d[3]
                bus['drivingNum'] = d[4]
                bus['engineNum'] = d[5]
                bus['motorNum'] = d[6]
                bus['motorNum'] = d[7]
                bus['registNum'] = d[8]
                bus['groupName'] = d[9]
                bus['rootGroupName'] = d[10]
                bus['groupId'] = d[11]
                bus['rootGroupId'] = d[12]
                bus['eventCode'] = '4'
                bus_key = 'bus_' + vin
                for key, value in bus.items():
                    self.rs_conn.hset(name= bus_key, key= key, value= value)
            logging.info('从DB中加载车辆基本信息已成功')
        except Exception as ex:
            logging.exception('初始化缓存出错') 
            raise ex
        finally:
            db_curs.close()
            db_conn.close()
        logging.info('redis初始化成功') 
    
    def init_redis(self):
        try:
            pool = redis.ConnectionPool(host=self.reader.redis_host,max_connections=1000)
            rs_client = redis.Redis(connection_pool=pool)
            rs_client.flushdb()
            logging.info('redis已连接')
            return rs_client
        except Exception as ex:
            logging.exception('redis初始化错误')
            raise ex
            
    def init_db(self):
        try:
            pgconn = pg.connect(database = self.reader.db_database, user = self.reader.db_user, password = self.reader.db_password, host = self.reader.db_host, port = self.reader.db_port)
            curs = pgconn.cursor()
            logging.info('postgresql已连接')
            return pgconn, curs
        except Exception as ex:
            logging.exception('postgresql初始化错误') 
            raise ex
    
    def init_rabbitMq(self):
        #初始化rabbitmq连接
        try:
            parameters = pika.ConnectionParameters(self.reader.rq_host, credentials=pika.credentials.PlainCredentials(self.reader.rq_user,self.reader.rq_passd), heartbeat=0)
            rq_connection = pika.BlockingConnection(parameters)
            logging.info('rabbitmq已连接')
            return rq_connection
        except Exception as ex:
            logging.exception('rabbitmq初始化错误') 
            raise ex
    
    def on_message(self, channel, method_frame, header_frame, body):
        #res.set(str(method_frame.delivery_tag), str(method_frame.delivery_tag))
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        try:
            bus_data = str(body, encoding = "utf-8")
            parser = VsailDataParser(bus_data)
            d = parser.translate_to_json()
            print(d)
            vin = d['vin']
            #self.rs_conn.hmset('bus_' + vin, d)
            bus_key = 'bus_' + vin
            for key, value in d.items():
                self.rs_conn.hset(name= bus_key, key= key, value= str(value))
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            try:
                rest_url = self.reader.websocket_url + bus_key
                req.post(rest_url)
            except Exception as ex:
                logging.exception('rest服务发送失败') 
        except Exception as ex:
            logging.exception('消息处理失败,忽略') 
            #raise ex
        
    def startConsume(self):
        channel = self.rq_conn.channel()
        channel.basic_consume(self.reader.rq_queue_redis, self.on_message)
        try:
            logging.info('开始监听mq消息队列...')  
            channel.start_consuming()
        except Exception as ex:
            channel.stop_consuming()
            logging.exception('mq任务监听失败') 
        finally:
            self.rq_conn.close()
            self.rs_conn.close()
            
if __name__ == '__main__':
    redis_client = RedisClient()
    redis_client.startConsume()

