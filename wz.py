# -*- coding:utf-8 -*-
import os
import sys
import datetime
import socket, sys
import struct
import binascii
import logging



# 第一步，创建一个logger  
logger = logging.getLogger()  
logger.setLevel(logging.INFO)    # Log等级总开关  
  
# 第二步，创建一个handler，用于写入日志文件  
parent_dir = os.path.dirname(__file__)
logfile = os.path.join(parent_dir, 'log/mqtt_passthrough_sub_logger.txt')
fh = logging.FileHandler(logfile, mode='w')  
fh.setLevel(logging.DEBUG)   # 输出到file的log等级的开关  
  
# 第三步，再创建一个handler，用于输出到控制台  
ch = logging.StreamHandler()  
ch.setLevel(logging.WARNING)   # 输出到console的log等级的开关  
  
# 第四步，定义handler的输出格式  
formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")  
fh.setFormatter(formatter)  
ch.setFormatter(formatter)  
  
# 第五步，将logger添加到handler里面  
logger.addHandler(fh)  
logger.addHandler(ch)  
  
# 日志  
logger.debug('this is a logger debug message')  
logger.info('this is a logger info message')  
logger.warning('this is a logger warning message')  
logger.error('this is a logger error message')  
logger.critical('this is a logger critical message') 

#======================================================    

#MQTT Initialize.--------------------------------------
try:
    import paho.mqtt.client as mqtt
except ImportError:
    print("MQTT client not find. Please install as follow:")
    print("git clone http://git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.python.git")
    print("cd org.eclipse.paho.mqtt.python")
    print("sudo python setup.py install")

#======================================================
# def on_connect(mqttc, obj, rc):
def on_connect(client, userdata, flags, rc):
    logger.info("OnConnetc, rc: "+str(rc))

def on_publish(mqttc, obj, mid):
    logger.info("OnPublish, mid: "+str(mid))

def on_subscribe(mqttc, obj, mid, granted_qos):
    logger.info("Subscribed: "+str(mid)+" "+str(granted_qos))

def on_log(mqttc, obj, level, string):
    logger.info("Log:"+string)

def on_message(mqttc, obj, msg):
    curtime = datetime.datetime.now()
    strcurtime = curtime.strftime("%Y-%m-%d %H:%M:%S")
    logger.info(strcurtime + ": " + msg.topic+" "+str(msg.qos)+" "+str(msg.payload))  

    print('-----------------receive time----------------------')
    print(datetime.datetime.now())

    payload_length = len(msg.payload)
    print('-------payload_length---------')
    print(msg.payload)
    print(payload_length)
    un_int = struct.unpack(str(payload_length) + 'B', msg.payload)
    print(un_int)
    uints = list(un_int)
    print(uints)



def on_exec(strcmd):
    logger.debug("Exec:", strcmd)
    strExec = strcmd




#=====================================================
def mqtt_passthrough_sub():

    mqttc = mqtt.Client("stm32.00000001.upstream.passthrough_subscriber")
    mqttc.username_pw_set("iiot", "smartlinkcloud")
    mqttc.on_message = on_message
    mqttc.on_connect = on_connect
    mqttc.on_publish = on_publish
    mqttc.on_subscribe = on_subscribe
    mqttc.on_log = on_log

    #strBroker = "localhost"
    strBroker = "101.200.158.2"

    mqttc.connect(strBroker, 1883, 60)
    # mqttc.subscribe("stm32.00000001.upstream.passthrough", 0)
    mqttc.subscribe("data", 0)
    mqttc.loop_forever()

if __name__ == '__main__':

    mqtt_passthrough_sub()