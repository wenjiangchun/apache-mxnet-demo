
import time
import logging
import configparser
import pika
import socketserver
import socket
import pandas as pd
import codecs

# %%
logging.basicConfig(filename="rabbitmq-receiver.log", filemode="w", format="%(asctime)s %(name)s:%(levelname)s:%(message)s", datefmt="%Y-%M-%d %H:%M:%S", level=logging.INFO)


# %%
#创建socket服务
sokt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sokt.connect(('47.94.225.139',9999)) 

#sokt.connect(('localhost',9999)) 
# %%
#user = 'admin#admin'
#sokt.send(user.encode('utf-8')) #发消息
#back_msg = sokt.recv(1024)
#print(back_msg.decode('utf-8'))


# %%
msg1 = b'fasdfasdfsad'
#msg2 = '23 23 02 fe 5f 56 53 41 49 4c 5f 54 45 53 54 5f 30 30 30 31 5f 01 00 2d 13 06 0f 0f 03 1d 05 00 06 f0 2f 20 02 61 0d f8 80 1b 66 19 01 02 03 04 05 06 07 00 00 00 00 00 00 00 00 00 00 29 25 00 00 00 00 83 fd 71'
#msgs = [msg1, msg2]
data_frame = pd.read_excel('vsail/test-data.xlsx', sheet_name='Sheet1')
data_list = []
for data in data_frame.data:
   data_list.append(data + '\r\n')
   #data_list.append(data)
data_list.reverse()
print(len(data_list))
def str_to_hex(dt):
        '''
         将字符串转换为16进制
        '''
        return ' '.join([hex(ord(c)).replace('0x', '').zfill(2) for c in dt])

for i in data_list:
    sokt.send(i.encode('utf-8')) #发消息
        #time.sleep(1)
        #back_msg = sokt.recv(1024)
        #print(back_msg.decode('utf-8'))


# %%
exit_message = 'exit'
sokt.send(exit_message.encode('utf-8'))


# %%
sokt.shutdown(2)
sokt.close()


# %%



# %%


