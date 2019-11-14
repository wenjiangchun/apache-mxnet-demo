
import time
import math
from bitarray import bitarray
from datetime import datetime

#Vsail报文数据解析
class VsailDataParser(object):
    
    #构造函数，传递一个data数据
    def __init__(self, data):
        #self.dts = data.split(' ')
        self.dts = self.str_to_hex(data).split(' ')
        if self.dts[3] != 'fe':
            self.dts[3] = 'fe'
        self.data = data
     
    #TODO 判断数据是否有效 目前只处理车辆登入，登出和车辆位置实时信息报文，其余报文暂不处理
    def is_valid(self):
        return self.get_data_type() != -1;
    
    #获取数据类型 1为登入 2为登出 3为实时车辆位置数据 -1为无效数据
    def get_data_type(self):
        if (self.dts[2] == '01' ):
            return 1
        elif (self.dts[2] == '04'):
            return 2
        #elif (self.dts[2] == '02' and self.dts[30] == '05'):
        elif (self.dts[2] == '02'):
            return 3
        else:
            return -1
    
    #是否为实时数据 True为实时数据 False为历史数据
    def is_real(self):
        return self.dts[0] == '23'
    
    #获取车辆vin
    def get_bus_vin(self):
        vin = ' '.join(self.dts[4:21])
        return ''.join([chr(i) for i in [int(b, 16) for b in vin.split(' ')]])
    
    #获取采集时间
    def get_send_time(self):
        year = 2000 + int(self.dts[24], 16)
        month = int(self.dts[25], 16)
        day = int(self.dts[26], 16)
        d = datetime(year, month, day, int(self.dts[27], 16),int(self.dts[28], 16),int(self.dts[29], 16))
        return str(d)
    
    #获取传感器火警状态或故障状态 TODO 后续获取车辆所有传感器状态
    def get_bus_sensores(self):
        #判断共有多少个传感器
        #每个传感器占用8字节 计算传感器个数
        sensores_data = self.dts[51:len(self.dts)-3]
        size = len(sensores_data) // 8
        datas = []
        #是否包含火警
        is_fire = 0
        #是否包含故障
        is_error = 0
        for i in range(size):
            fire_data = self.convert_bit_to_01(sensores_data[i*8])
            if '1' in fire_data:
                is_fire = 1
            error_data = self.convert_bit_to_01(sensores_data[i*8 + 1])
            if '1' in error_data:
                is_error = 1
            concen_data = int(sensores_data[i*8 + 2], 16) * 4
            temp_data = int(sensores_data[i*8 + 3], 16)
            d = {}
            d['sn'] = i + 1
            d['fire'] = fire_data
            d['error'] = error_data
            d['concen'] = concen_data
            d['temp'] = temp_data
            datas.insert(i, d)
        return datas, is_fire, is_error
    
    #将字节字符转换为二进制字符
    def convert_bit_to_01(self, s):
        a = bitarray(endian='little')
        a.frombytes(bytes.fromhex(s))
        return a.to01()
    
    #解析经纬度 只有车辆位置为实时信息时有效 坐标位置位于32至39
    def get_bus_position(self):
        #如果数据有效并且数据为实时数据则计算坐标，否则返回0,0坐标
        if (self.get_data_type() == 3):
            dx = ' '.join(self.dts[32:36])
            dy = ' '.join(self.dts[36:40])
            lon = (int.from_bytes(bytes.fromhex(dx), byteorder='big'))/math.pow(10, 6)
            lat = (int.from_bytes(bytes.fromhex(dy), byteorder='big'))/math.pow(10, 6)
            return lon, lat
        else:
            return 0, 0

    #将解析结果以字典格式返回
    def translate_to_json(self):
        rs  = {}
        if (self.is_valid()):
            type = self.get_data_type()
            rs["type"] = self.get_data_type()
            rs["vin"] = self.get_bus_vin()
            rs["stime"] = self.get_send_time()
            rs['data'] = self.data
            if (type == 3):
                position = self.get_bus_position()
                rs["x"] = position[0]
                rs["y"] = position[1]
                sensores, is_fire, is_error = self.get_bus_sensores()
                rs["isFire"] = is_fire
                rs["isError"] = is_error
                rs["sensores"] = str(sensores)
        return rs

    def str_to_hex(self, dt):
        return ' '.join([hex(ord(c)).replace('0x', '').zfill(2) for c in dt])

    def hex_to_str(self, s):
        return ''.join([chr(i) for i in [int(b, 16) for b in s.split(r'/x')[1:]]])


