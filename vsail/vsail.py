from twisted.internet import protocol, reactor, endpoints
from twisted.internet.protocol import Factory, connectionDone
from twisted.protocols import basic
from twisted.protocols import wire
import time
import logging
import configparser
import pika
from vsail_data_parser import VsailDataParser
from config_reader import VsailConfigReader
import binascii
import psycopg2 as pg
import requests as req
import redis
from datetime import datetime, date
from psycopg2.extras import Json
from psycopg2.extensions import register_adapter

logging.basicConfig(filename="vsail.log", filemode="w", format="%(asctime)s %(name)s:%(levelname)s:%(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.INFO)

def hex_to_str(s):
        '''
         将16进制转换为字符串
        '''
        return ''.join([chr(i) for i in [int(b, 16) for b in s.split(r'/x')[1:]]])

#Vsail车辆数据接收服务
class VsailServer(object):
    def __init__(self):
        self.reader = VsailConfigReader()
        self.init_rq = False
        #self.rq_conn = self.init_rabbitmq(self.reader.rq_host)
        #获取DB连接
        self.db_conn, db_curs = self.init_db()
        #获取redis连接
        self.rs_conn = self.init_redis()
        logging.info('从DB中加载车辆信息...') 
        try:
            query_bus_sql = 'select b.id, b.create_time, b.vin, COALESCE(b.bus_num,\'\'), COALESCE(b.driving_num,\'\'), b.model_name, COALESCE(b.product_num,\'\'), b.line_group_id, b.site_group_id, b.branch_group_id, b.root_group_id, l.full_name, s.full_name as site_full_name, COALESCE(s.linker,\'\'), COALESCE(s.linker_mobile,\'\'), COALESCE(s.address,\'\'), br.full_name as branch_full_name, r.full_name as root_full_name from v_bus b left join sys_group l on b.line_group_id=l.id left join sys_group s on b.site_group_id=s.id left join sys_group br on b.branch_group_id=br.id left join sys_group r on b.root_group_id=r.id where b.deleted=false and b.used=true'
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
                bus['modelName'] = d[5]
                bus['productNum'] = d[6]
                bus['lineGroupId'] = d[7]
                bus['siteGroupId'] = d[8]
                bus['branchGroupId'] = d[9]
                bus['rootGroupId'] = d[10]
                bus['lineGroupName'] = d[11]
                bus['siteGroupName'] = d[12]
                bus['linker'] = d[13]
                bus['linkerMobile'] = d[14]
                bus['address'] = d[15]
                bus['branchGroupName'] = d[16]
                bus['rootGroupName'] = d[17]
                bus['eventCode'] = '4'
                bus_key = 'bus_' + vin
                for key, value in bus.items():
                    self.rs_conn.hset(name= bus_key, key= key, value= value)
            logging.info('从DB中加载车辆基本信息已成功')
            self.init_rq = True
        except Exception as ex:
            logging.exception('初始化缓存出错')
            logging.exception(ex)
            raise ex
        finally:
            db_curs.close()
            #db_conn.close()
        logging.info('redis_client初始化成功!') 

    def init_db(self):
        try:
            pgconn = pg.connect(database = self.reader.db_database, user = self.reader.db_user, password = self.reader.db_password, host = self.reader.db_host, port = self.reader.db_port)
            curs = pgconn.cursor()
            register_adapter(dict, Json)
            logging.info('postgresql已连接')
            return pgconn, curs
        except Exception as ex:
            logging.exception('postgresql初始化错误') 
            raise ex
    
    def init_redis(self):
        try:
            pool = redis.ConnectionPool(host=self.reader.redis_host,max_connections=1000)
            rs_client = redis.Redis(connection_pool=pool)
            logging.info('redis已连接!')
            rs_client.flushdb()
            return rs_client
        except Exception as ex:
            logging.exception('redis初始化错误')
            logging.exception(ex)
            raise ex

    #初始化rabbitMQ
    def init_rabbitmq(self, rq_host):
        try:
            parameters = pika.ConnectionParameters(rq_host, port=self.reader.rq_port, virtual_host='vsail', credentials=pika.credentials.PlainCredentials(self.reader.rq_user, self.reader.rq_passd), heartbeat=0)
            connection = pika.BlockingConnection(parameters)
            logging.info('rabbitmq已连接')
            self.init_rq = True
            return connection
        except Exception as ex:
            logging.exception('rabbitmq初始化错误')
            logging.exception(ex)
            #异常出错不再往下执行
            raise Exception('rabbitmq初始化错误')
            
    def start(self):
        if self.init_rq:
            self.rt = reactor
            server_url = 'tcp:' + str(self.reader.socket_port)
            endpoints.serverFromString(reactor, server_url).listen(VsailDataFactory(self.db_conn, self.rs_conn, self.reader))
            reactor.run()
            
    def stop(self):
        self.rt.stop()
        self.rs_conn.close()
        self.db_conn.close()


class VsailDataFactory(protocol.Factory):
    def __init__(self, db_conn, rs_conn,reader:VsailConfigReader):
        self.db_conn = db_conn
        self.rs_conn = rs_conn
        self.reader = reader
    def buildProtocol(self, addr):
        return VsailDataHandler(self.db_conn, self.rs_conn , self.reader)


class VsailDataHandler(basic.NetstringReceiver):
    def __init__(self, db_conn, rs_conn, reader:VsailConfigReader):
        self.db_conn = db_conn
        self.rs_conn = rs_conn
        self.reader = reader
    def dataReceived(self, data):
        logging.info('开始接收消息...')
        message = data.decode('utf-8',"ignore")
        logging.info(data)
        logging.info('二进制转换16进制...')
        logging.info(binascii.b2a_hex(data))
        aa = str(binascii.b2a_hex(data), encoding="utf-8")
        bb = []
        for i, x in enumerate(aa):
            if i % 2 != 0:
                bb.append(str(aa[i-1]) + str(x))
        if message == 'exit':
            self.transport.loseConnection()
        else:
            try:
                logging.info(bb)
                v_message = ' '.join(bb)
                logging.info(v_message)
                parser = VsailDataParser(v_message)
                bus_data = parser.translate_to_json()
                #判断报文是否有效
                if parser.is_valid():
                    #判断是历史消息还是实时消息
                    if parser.is_real() is True:
                        #如果是实时消息判断发送时间
                        self.toRedis(parser)
                        #如果是上线或下线 返回结果指令信息
                        if bus_data['type'] == 1 or bus_data['type'] == 2:
                            #self.transport.write('OK'.encode('utf-8'))
                            pass
                    else:
                        #self.channel.basic_publish(exchange=self.reader.rq_ex_hist, routing_key='', body=v_message)
                        pass
                    self.toDB(parser)
                else:
                    logging.warning('无效报文:' + message)
            except Exception:
                pass
    def connectionMade(self):  # 建立连接后的回调函数
        logging.info('客户端已连接')
    def connectionLost(self, reason=connectionDone):  # 断开连接后的反应
        logging.info('客户端已断开')

    def toDB(self, parser):
        try:
            if parser.is_valid():
                with self.db_conn:
                    with self.db_conn.cursor() as curs:
                        d = parser.translate_to_json()
                        #判断是上线还是实时信息
                        if parser.get_data_type() == 3:
                            sql = 'insert into v_bus_data_log(vin, geom, bus_data, sensores, upload_time, data) values(%(vin)s, st_setsrid(st_makepoint(%(lat)s, %(lon)s), 4326), %(bus_data)s::jsonb,%(sensores)s::jsonb[],%(upload_time)s::timestamp, %(data)s)'
                            curs.execute(sql,{'vin': parser.get_bus_vin(),'bus_data':d, 'sensores': d['sensores'], 'upload_time': d['stime'],'data':parser.data,'lon':d['y'], 'lat':d['x']})
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
                            #判断是否有故障 
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
                            curs.execute(sql,{'vin': parser.get_bus_vin(),'bus_data':parser.translate_to_json(), 'upload_time': d['stime'],'data':parser.data})
                            on_sql = 'insert into v_bus_on_off_log(vin, bus_num, driving_num,group_id, group_name, root_group_id, root_group_name, log_time, log_year, log_month, log_day, flag) '
                            on_sql += '(select b.vin, b.bus_num, b.driving_num,b.line_group_id,g.name,b.root_group_id, r.name, %(upload_time)s::timestamp,%(log_year)s,%(log_month)s,%(log_day)s, 0  '
                            on_sql += 'from v_bus b left join sys_group g on b.line_group_id=g.id left join sys_group r on b.root_group_id=r.id where b.vin=%(vin)s and b.deleted=false)'
                            log_time = datetime.strptime(d['stime'],"%Y-%m-%d %H:%M:%S")
                            log_year = log_time.year
                            log_month = log_time.month
                            log_day = log_time.day
                            dp = {'upload_time':d['stime'], 'vin':d['vin'], 'log_year':log_year,'log_month':log_month,'log_day':log_day}
                            curs.execute(on_sql, dp)
                        elif parser.get_data_type() == 2:
                            sql = 'insert into v_bus_data_log(vin, bus_data,upload_time, data) values(%(vin)s, %(bus_data)s::jsonb,%(upload_time)s::timestamp, %(data)s)'
                            curs.execute(sql,{'vin': parser.get_bus_vin(),'bus_data':parser.translate_to_json(), 'upload_time': d['stime'],'data':parser.data})
                            on_sql = 'insert into v_bus_on_off_log(vin, bus_num, driving_num,group_id, group_name, root_group_id, root_group_name, log_time, log_year, log_month, log_day, flag) '
                            on_sql += '(select b.vin, b.bus_num, b.driving_num,b.line_group_id,g.name,b.root_group_id, r.name, %(upload_time)s::timestamp,%(log_year)s,%(log_month)s,%(log_day)s, 1  '
                            on_sql += 'from v_bus b left join sys_group g on b.line_group_id=g.id left join sys_group r on b.root_group_id=r.id where b.vin=%(vin)s and b.deleted=false)'
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
        
    def toRedis(self, parser):
        try:
            if parser.is_valid():
                d = parser.translate_to_json()
                vin = d['vin']
                if parser.get_data_type() == 1:
                    d['eventCode'] = '1'
                elif parser.get_data_type() == 2:
                    d['eventCode'] = '2'
                else:
                    d['eventCode'] = '3'
                    d['sensores'] = str(d['sensores'])
                bus_key = 'bus_' + vin
                self.rs_conn.hmset(bus_key, d)
                #for key, value in d.items():
                    #self.rs_conn.hset(name=bus_key, key=key, value=str(value))
                    #pass
                try:
                    rest_url = self.reader.websocket_url + vin
                    req.post(rest_url)
                except Exception:
                    logging.exception('rest服务发送失败') 
        except Exception as ex:
            logging.exception(ex)
            logging.exception('消息处理失败,忽略') 

if __name__ == '__main__':
    vsail_server = VsailServer()
    vsail_server.start()

