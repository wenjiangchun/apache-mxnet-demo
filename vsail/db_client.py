import json
import psycopg2 as pg
import logging
import pika
from psycopg2.extras import Json
from psycopg2.extensions import register_adapter
from datetime import datetime, date
from config_reader import VsailConfigReader
from vsail_data_parser import VsailDataParser

logging.basicConfig(filename="db_client.log", filemode="w", format="%(asctime)s %(name)s:%(levelname)s:%(message)s", datefmt="%Y-%M-%d %H:%M:%S", level=logging.INFO)

class DBClient(object):
    def __init__(self):
        self.reader = VsailConfigReader()
        logging.info('加载application.ini配置')
        #获取MQ连接
        self.rq_conn = self.init_rabbitMq()
        #获取DB连接
        self.db_conn = self.init_db()
        logging.info('db-client init success') 
    def init_db(self):
        try:
            pgconn = pg.connect(database = self.reader.db_database, user = self.reader.db_user, password = self.reader.db_password, host = self.reader.db_host, port=self.reader.db_port)
            logging.info('postgresql已连接')
            register_adapter(dict, Json)
            return pgconn
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
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        try:
            data = str(body, encoding = "utf-8")
            #print(data)
            bus_data = ''
            parser = VsailDataParser(data)
            try:
                #bus_data = parser.translate_to_json()
                bus_data = parser.dts
            except Exception:
                pass
            if parser.is_valid():
                with self.db_conn:
                    with self.db_conn.cursor() as curs:
                        sql = 'insert into v_bus_data_log(bus_data,upload_time, data) values(%(bus_data)s,%(upload_time)s::timestamp, %(data)s)'
                        curs.execute(sql,{'bus_data':bus_data, 'upload_time': datetime.now(),'data':data})
        except Exception as ex:
            #logging.exception('消息处理失败,忽略') 
            print(ex)
            #raise ex
        
    def startConsume(self):
        channel = self.rq_conn.channel()
        #监听队列
        channel.basic_consume(self.reader.rq_queue_redis, self.on_message)
        try:
            logging.info('开始监听mq消息队列...')  
            channel.start_consuming()
        except Exception as ex:
            channel.stop_consuming()
            logging.exception('mq任务监听失败') 
        finally:
            self.rq_conn.close()
            self.db_conn.close()
            
if __name__ == '__main__':
    db_client = DBClient()
    db_client.startConsume()