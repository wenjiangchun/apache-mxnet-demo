import json
import psycopg2 as pg
import logging
import pika
from psycopg2.extras import Json
from psycopg2.extensions import register_adapter
from datetime import datetime, date
from config_reader import VsailConfigReader

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
        self.batch_data = []
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
        try:
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            bus_data = str(body, encoding = "utf-8")
            #self.batch_data.append(eval(bus_data))
            
            #获取周几
            weekday = date.today().weekday()
            
            if len(self.batch_data) == 10:
                with self.db_conn:
                    with self.db_conn.cursor() as curs:
                        batch_params = []
                        temp_sql = 'insert into v_bus_pos_log_temp%(weekday)s(vin, state, sensores, geom, upload_time) values(%(vin)s, %(state)s, %(sensores)s::jsonb[], st_setsrid(st_makepoint(%(lat)s, %(lon)s), 4326),%(upload_time)s::timestamp)'
                        for d in self.batch_data:
                            #判断数据类型 1为车辆上线 2为车辆下线 3为车辆实时信息
                            data_type = d['type']
                            param = {}
                            if data_type == 1:
                                #车辆上线
                                log_sql = 'insert into v_bus_pos_log(start_time, vin) values(%(start_time)s::timestamp,%(vin)s)';
                                dp = {'start_time':d['stime'],'vin':d['vin']}
                                #curs.execute(log_sql, dp)
                                on_sql = 'insert into v_bus_on_off_log(vin, bus_num, driving_num,engine_num,regist_num,group_id, group_name, root_group_id, root_group_name, log_time, log_year, log_month, log_day, flag, data) '
                                on_sql += '(select b.vin, b.bus_num, b.driving_num,b.engine_num,b.regist_num,b.group_id,g.name,b.root_group_id, %(upload_time)s::timestamp,%(log_year)s,%(log_month)s,%(log_day)s, 0, %(data)s '
                                on_sql += 'from v_bus b left join sys_group g on b.group_id=g.id left join sys_group r on b.root_group_id=r.id where b.vin=%(vin)s and b.deleted=false)'
                                on_sql = datetime.strptime(d['stime'],"%Y-%m-%d %H:%M:%S")
                                log_year = log_time.year
                                log_month = log_time.month
                                log_day = log_time.day
                                dp = {'upload_time':d['stime'], 'data':bus_data, 'vin':d['vin'], 'log_year':log_year,'log_month':log_month,'log_day':log_day}
                                #curs.execute(on_sql, dp)
                                param = {'upload_time':d['stime'], 'vin':d['vin'], 'state': data_type, 'sensores': None, 'lat':0, 'lon':0, 'weekday':weekday }
                            elif data_type == 2:
                                #车辆下线
                                log_sql = 'update v_bus_pos_log set end_time= %(end_time)s::timestamp where vin=%(vin)s and end_time is null';
                                dp = {'end_time':d['stime'],'vin':d['vin']}
                                #curs.execute(log_sql, dp)
                                #添加下线记录
                                off_sql = 'insert into v_bus_on_off_log(vin, bus_num, driving_num,engine_num,regist_num,group_id, group_name, root_group_id, root_group_name, log_time, log_year, log_month, log_day, flag, data) '
                                off_sql += '(select b.vin, b.bus_num, b.driving_num,b.engine_num,b.regist_num,b.group_id,g.name,b.root_group_id, %(upload_time)s::timestamp,%(log_year)s,%(log_month)s,%(log_day)s, 1, %(data)s '
                                off_sql += 'from v_bus b left join sys_group g on b.group_id=g.id left join sys_group r on b.root_group_id=r.id where b.vin=%(vin)s and b.deleted=false)'
                                off_sql = datetime.strptime(d['stime'],"%Y-%m-%d %H:%M:%S")
                                log_year = log_time.year
                                log_month = log_time.month
                                log_day = log_time.day
                                dp = {'upload_time':d['stime'], 'data':bus_data, 'vin':d['vin'], 'log_year':log_year,'log_month':log_month,'log_day':log_day}
                                #curs.execute(off_sql, dp)
                                param = {'upload_time':d['stime'], 'vin':d['vin'], 'state': data_type, 'sensores': None, 'lat':0, 'lon':0 , 'weekday':weekday}
                            else:
                                #查询对应vin轨迹记录，如果存在没有下线的记录则更新 否则则插入，原则上该地方只有更新
                                sql = 'with upsert as (update v_bus_pos_log set state=state||%(state)s::smallint, upload_time=upload_time||%(upload_time)s::timestamp, sensores=sensores||%(sensores)s::jsonb, geom=geom||st_setsrid(st_makepoint(%(lat)s, %(lon)s), 4326)::geometry where vin=%(vin)s and end_time is null returning *) insert into v_bus_pos_log(vin, state,sensores, geom, upload_time,start_time) select %(vin)s, Array[%(state)s],Array[%(sensores)s::jsonb],Array[st_setsrid(st_makepoint(%(lat)s, %(lon)s), 4326)],Array[%(upload_time)s::timestamp], now() where not exists (select 1 from upsert where vin=%(vin)s)'
                                #正常运行
                                state = 3
                                if d['isFire']:
                                    #只有火警
                                    state = 4
                                if d['isError']:
                                    #只有故障
                                    state = 5
                                if d['isFire'] and d['isError']:
                                    #同时有火警和故障
                                    state = 6
                                #插入轨迹记录表
                                #curs.execute(sql,{'state':state, 'upload_time':d['stime'],'lon':d['y'], 'lat':d['x'], 'sensores':{'data':d['sensores']}, 'vin':d['vin']})
                                if d['isFire']:
                                    #插入火警记录表
                                    fire_sql = 'insert into v_bus_fire_log(vin, bus_num, driving_num,engine_num,regist_num,group_id, group_name, root_group_id, root_group_name, sensores, geom, log_time, log_year, log_month, log_day) '
                                    fire_sql += '(select b.vin, b.bus_num, b.driving_num,b.engine_num,b.regist_num,b.group_id,g.name,b.root_group_id, r.name, %(sensores)s::jsonb[], st_setsrid(st_makepoint(%(lat)s, %(lon)s), 4326),%(upload_time)s::timestamp,%(log_year)s,%(log_month)s,%(log_day)s '
                                    fire_sql += 'from v_bus b left join sys_group g on b.group_id=g.id left join sys_group r on b.root_group_id=r.id where b.vin=%(vin)s and b.deleted=false)'
                                    log_time = datetime.strptime(d['stime'],"%Y-%m-%d %H:%M:%S")
                                    log_year = log_time.year
                                    log_month = log_time.month
                                    log_day = log_time.day
                                    dp = {'upload_time':d['stime'],'lon':d['y'], 'lat':d['x'], 'sensores':d['sensores'], 'vin':d['vin'], 'log_year':log_year,'log_month':log_month,'log_day':log_day}
                                    curs.execute(fire_sql, dp)
                                if d['isError']:
                                    #插入故障记录表
                                    1
                                param = {'upload_time':d['stime'], 'vin':d['vin'], 'state': state, 'sensores': d['sensores'], 'lat':d['x'], 'lon':d['y'], 'weekday':weekday}
                            batch_params.append(param)
                        #pg.extras.execute_batch(curs,temp_sql, batch_params)
                        self.batch_data = []
            
        except Exception as ex:
            #logging.exception('消息处理失败,忽略') 
            print(ex)
            #raise ex
        
    def startConsume(self):
        channel = self.rq_conn.channel()
        #监听队列
        channel.basic_consume(self.reader.rq_queue_db, self.on_message)
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