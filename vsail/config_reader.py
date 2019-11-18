import configparser

class VsailConfigReader(object):
    def __init__(self):
        parser = configparser.ConfigParser()
        parser.read('vsail/application.ini')
        
        #----------------------redis服务器----------------------------------------
        #redis服务器地址
        self.redis_host = parser.get('redis', 'host')

        #----------------------rabbitmq配置信息--------------------------
        #服务器地址
        self.rq_host = parser.get('rabbitmq', 'host')
        #实时消息使用exchange名称
        self.rq_ex_real = parser.get('rabbitmq', 'ex_real')
        #历史消息使用exchange名称
        self.rq_ex_hist=  parser.get('rabbitmq', 'ex_hist')
        #redis消费队列名称
        self.rq_queue_redis=  parser.get('rabbitmq', 'queue_redis')
        #db消费队列名称
        self.rq_queue_db=  parser.get('rabbitmq', 'queue_db')
        #rabbitmq管理员用户名
        self.rq_user=  parser.get('rabbitmq', 'user')
        #rabbitmq管理员用户密码
        self.rq_passd=  parser.get('rabbitmq', 'password')

       #----------------------db配置信息--------------------------
        #db地址
        self.db_host = parser.get('postgresql', 'host')
        #db端口
        self.db_port = parser.get('postgresql', 'port')
        self.db_user = parser.get('postgresql', 'user')
        self.db_password = parser.get('postgresql', 'password')
        self.db_database = parser.get('postgresql', 'database')


        #----------------------socket配置信息--------------------------
        #socket地址
        self.socket_host = parser.get('socket', 'host')
        #socket端口
        self.socket_port = int(parser.get('socket', 'port'))

       #----------------------websocket配置信息--------------------------
        #socket地址
        self.websocket_url= parser.get('websocket', 'url')
        
    def get_redis_config(self):
        pass

    def get_rabbitmq_config(self):
        pass

    def get_socket_server_config(self):
        pass

    def get_db_config(self):
        pass

    def get_websocket_config(self):
        pass

if __name__ == '__main__':
    config = VsailConfigReader()
    print(config.db_database)