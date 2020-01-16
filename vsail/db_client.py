import json
import psycopg2 as pg
import logging
import pika
from psycopg2.extras import Json
from psycopg2.extensions import register_adapter
from datetime import datetime, date
from config_reader import VsailConfigReader
from vsail_data_parser import VsailDataParser

logging.basicConfig(filename="db_client.log", filemode="w", format="%(asctime)s %(name)s:%(levelname)s:%(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.INFO)

class DBClient(object):
    def __init__(self):
        self.reader = VsailConfigReader()
        #获取MQ连接
        self.rq_conn = self.init_rabbitMq()
        #获取DB连接
        self.db_conn = self.init_db()
        logging.info('db_client初始化成功!') 
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
            parameters = pika.ConnectionParameters(self.reader.rq_host, port=self.reader.rq_port, credentials=pika.credentials.PlainCredentials(self.reader.rq_user,self.reader.rq_passd), heartbeat=5)
            rq_connection = pika.BlockingConnection(parameters)
            logging.info('rabbitmq已连接')
            return rq_connection
        except Exception as ex:
            logging.exception('rabbitmq初始化错误') 
            raise ex
    
    def on_message(self, channel, method_frame, header_frame, body):
        logging.info('开始处理数据...')
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        try:
            data = str(body, encoding = "utf-8")
            parser = VsailDataParser(data)
            if parser.is_valid():
                with self.db_conn:
                    with self.db_conn.cursor() as curs:
                        d = parser.translate_to_json()
                        #判断是上线还是实时信息
                        if parser.get_data_type() == 3:
                            sql = 'insert into v_bus_data_log(vin, geom, bus_data, sensores, upload_time, data) values(%(vin)s, st_setsrid(st_makepoint(%(lat)s, %(lon)s), 4326), %(bus_data)s::jsonb,%(sensores)s::jsonb[],%(upload_time)s::timestamp, %(data)s)'
                            curs.execute(sql,{'vin': parser.get_bus_vin(),'bus_data':d, 'sensores': d['sensores'], 'upload_time': d['stime'],'data':data,'lon':d['y'], 'lat':d['x']})
                            #判断是否有火警
                            if d['isFire'] == 1:
                                fire_sql = 'insert into v_bus_fire_log(vin, bus_num, driving_num,group_id, group_name, root_group_id, root_group_name, bus_data, geom, log_time, log_year, log_month, log_day, model_name) '
                                fire_sql += '(select b.vin, b.bus_num, b.driving_num,b.line_group_id,g.full_name,b.root_group_id, r.full_name, %(bus_data)s::json, st_setsrid(st_makepoint(%(lat)s, %(lon)s), 4326),%(upload_time)s::timestamp,%(log_year)s,%(log_month)s,%(log_day)s, b.model_name '
                                fire_sql += 'from v_bus b left join sys_group g on b.line_group_id=g.id left join sys_group r on b.root_group_id=r.id where b.vin=%(vin)s and b.deleted=false)'
                                log_time = datetime.strptime(d['stime'],"%Y-%m-%d %H:%M:%S")
                                log_year = log_time.year
                                log_month = log_time.month
                                log_day = log_time.day
                                dp = {'upload_time':d['stime'],'lon':d['y'], 'lat':d['x'], 'bus_data':parser.translate_to_json(), 'vin':d['vin'], 'log_year':log_year,'log_month':log_month,'log_day':log_day}
                                curs.execute(fire_sql, dp)
                            #判断是否有故障 TODO
                            if d['isError'] == 1:
                                fire_sql = 'insert into v_bus_break_down_log(vin, bus_num, driving_num,group_id, group_name, root_group_id, root_group_name, bus_data, geom, log_time, log_year, log_month, log_day, model_name) '
                                fire_sql += '(select b.vin, b.bus_num, b.driving_num,b.line_group_id,g.full_name,b.root_group_id, r.full_name, %(bus_data)s::json, st_setsrid(st_makepoint(%(lat)s, %(lon)s), 4326),%(upload_time)s::timestamp,%(log_year)s,%(log_month)s,%(log_day)s, b.model_name '
                                fire_sql += 'from v_bus b left join sys_group g on b.line_group_id=g.id left join sys_group r on b.root_group_id=r.id where b.vin=%(vin)s and b.deleted=false)'
                                log_time = datetime.strptime(d['stime'],"%Y-%m-%d %H:%M:%S")
                                log_year = log_time.year
                                log_month = log_time.month
                                log_day = log_time.day
                                dp = {'upload_time':d['stime'],'lon':d['y'], 'lat':d['x'], 'bus_data':parser.translate_to_json(), 'vin':d['vin'], 'log_year':log_year,'log_month':log_month,'log_day':log_day}
                                curs.execute(fire_sql, dp)
                        elif parser.get_data_type() == 1:
                            sql = 'insert into v_bus_data_log(vin, bus_data,upload_time, data) values(%(vin)s, %(bus_data)s::jsonb,%(upload_time)s::timestamp, %(data)s)'
                            curs.execute(sql,{'vin': parser.get_bus_vin(),'bus_data':parser.translate_to_json(), 'upload_time': d['stime'],'data':data})
                            on_sql = 'insert into v_bus_on_off_log(vin, bus_num, driving_num,group_id, group_name, root_group_id, root_group_name, log_time, log_year, log_month, log_day, flag) '
                            on_sql += '(select b.vin, b.bus_num, b.driving_num,b.group_id,g.name,b.root_group_id, r.name, %(upload_time)s::timestamp,%(log_year)s,%(log_month)s,%(log_day)s, 0  '
                            on_sql += 'from v_bus b left join sys_group g on b.group_id=g.id left join sys_group r on b.root_group_id=r.id where b.vin=%(vin)s and b.deleted=false)'
                            log_time = datetime.strptime(d['stime'],"%Y-%m-%d %H:%M:%S")
                            log_year = log_time.year
                            log_month = log_time.month
                            log_day = log_time.day
                            dp = {'upload_time':d['stime'], 'vin':d['vin'], 'log_year':log_year,'log_month':log_month,'log_day':log_day}
                            curs.execute(on_sql, dp)
        except Exception as ex:
            logging.exception('消息处理失败,忽略') 
            logging.exception(ex) 
            #raise ex
        
    def startConsume(self):
        channel = self.rq_conn.channel()
        #监听队列
        channel.basic_consume(self.reader.rq_queue_db, self.on_message)
        try:
            logging.info('开始监听mq消息队列...')  
            channel.start_consuming()
        except Exception as ex:
            logging.exception('mq任务监听失败') 
            logging.exception(ex)
            channel.stop_consuming()
        finally:
            logging.exception('关闭连接...') 
            self.rq_conn.close()
            self.db_conn.close()
            
if __name__ == '__main__':
    db_client = DBClient()
    db_client.startConsume()