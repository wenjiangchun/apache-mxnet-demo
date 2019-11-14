#!/usr/bin/env python
# coding: utf-8

# In[8]:


import redis
import sched
import time
import psycopg2 as pg
import requests as req
import logging
import configparser
import pika
import base64
import json
from vsail_data_parser import VsailDataParser


# In[9]:


#该类作用：
#       1 初始化从数据库中加载车辆基本信息和位置信息到缓存
#       2 从rabbitMQ中监听队列，消费报文消息，如果监听到车辆上线，下线，火警以及故障及时更新到数据库 TODO
#       3 定时将缓存中数据更新到数据库 TODO


# In[ ]:


class RedisClient(object):
    def __init__(self):
        logging.basicConfig(filename="redis-client.log", filemode="w", format="%(asctime)s %(name)s:%(levelname)s:%(message)s", datefmt="%Y-%M-%d %H:%M:%S", level=logging.INFO)
        self.parser = configparser.ConfigParser()
        self.parser.read('application.ini')
        logging.info('加载application.ini配置')
        #获取redis配置信息
        self.bus_pos_key = self.parser.get('redis', 'bus_pos_key')
        self.syn_time = float(self.parser.get('redis', 'syn_time'))
        #获取redis连接
        self.rs_conn = self.init_redis()
        #获取MQ连接
        self.rq_conn = self.init_rabbitMq()
        #获取DB连接
        db_conn, db_curs = self.init_db()
        logging.info('get bus info from postgresql') 
        try:
            query_bus_sql = 'select b.id, b.create_time, b.vin, b.bus_num, b.driving_num, b.engine_num, b.motor_name, b.motor_num, b.regist_num, g.full_name, r.full_name as root_full_name from v_bus b left join sys_group g on b.group_id=g.id left join sys_group r on b.root_group_id=r.id where b.deleted=false'
            #query_bus_pos = 'select id, ST_X(geom), ST_Y(geom), ST_M(geom), name, update_time from v_bus_pos'
            db_curs.execute(query_bus_sql)
            dataes = db_curs.fetchall()
            for d in dataes:
                bus = {}
                vin = d[2]
                bus['vin'] = d[2]
                bus['id'] = d[0]
                bus['create_time'] = str(d[1])
                bus['bus_num'] = d[3]
                bus['driving_num'] = d[4]
                bus['engine_num'] = d[5]
                bus['motor_name'] = d[6]
                bus['motor_num'] = d[7]
                bus['regist_num'] = d[8]
                bus['group_name'] = d[9]
                bus['root_group_name'] = d[10]
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
        logging.info('redis-client init success') 
    
    def init_redis(self):
        try:
            pool = redis.ConnectionPool(host=self.parser.get('redis', 'host'),max_connections=1000)
            rs_client = redis.Redis(connection_pool=pool)
            rs_client.flushdb()
            logging.info('redis已连接')
            return rs_client
        except Exception as ex:
            logging.exception('redis初始化错误')
            raise ex
            
    def init_db(self):
        try:
            pgconn = pg.connect(database = self.parser.get('postgresql', 'database'), user = self.parser.get('postgresql', 'user'), password = self.parser.get('postgresql', 'password'), host = self.parser.get('postgresql', 'host'))
            curs = pgconn.cursor()
            logging.info('postgresql已连接')
            return pgconn, curs
        except Exception as ex:
            logging.exception('postgresql初始化错误') 
            raise ex
    
    def init_rabbitMq(self):
        #初始化rabbitmq连接
        try:
            parameters = pika.ConnectionParameters(self.parser.get('rabbitmq', 'host'), credentials=pika.credentials.PlainCredentials('admin','admin'), heartbeat=0)
            rq_connection = pika.BlockingConnection(parameters)
            logging.info('rabbitmq已连接')
            return rq_connection
        except Exception as ex:
            logging.exception('rabbitmq初始化错误') 
            raise ex
    
    def on_message(self, channel, method_frame, header_frame, body):
        #res.set(str(method_frame.delivery_tag), str(method_frame.delivery_tag))
        try:
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            #channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            #TODO 处理消息 此处需要获取车辆机构信息(从缓存中直接获取，如果缓存中没有则直接存放新的临时数据)
            #上线 下线既时更新数据库
            #另外开启线程通知系统websocket
            bus_data = str(body, encoding = "utf-8")
            parser = VsailDataParser(bus_data)
            d = parser.translate_to_json()
            vin = d['vin']
            #self.rs_conn.hmset('bus_pos_' + vin, d)
            #for key, value in d.items():
                #self.rs_conn.hset(name= 'bus_pos_' + vin, key= key, value= str(value))
                #key
            try:
                rest_url = 'http://localhost:8080/websocket/sendMessage?message='
                #req.post(rest_url + base64.b64encode(body))
            except Exception as ex:
                logging.exception('rest发送失败') 
        except Exception as ex:
            print(ex)
            logging.exception('消息处理失败,忽略') 
            #raise ex
        
    def startConsume(self):
        channel = self.rq_conn.channel()
        channel.basic_consume('bus-db', self.on_message)
        try:
            logging.info('开始监听mq消息队列...')  
            channel.start_consuming()
        except Exception as ex:
            print(ex)
            channel.stop_consuming()
            logging.exception('mq任务监听失败') 
        finally:
            self.rq_conn.close()
            self.rs_conn.close()
            
if __name__ == '__main__':
    redis_client = RedisClient()
    redis_client.startConsume()


# In[ ]:





# In[ ]:




