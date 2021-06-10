#!/usr/bin/env python

import os
import sys
import time, signal
import json
import subprocess
import re
from confluent_kafka import Producer
from Adafruit_ADS1x15 import ADS1x15
import logging

libdir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'lib')
if os.path.exists(libdir):
    sys.path.append(libdir)

from waveshare_TSL2591 import TSL2591

logging.basicConfig(level=logging.INFO)
sensor = TSL2591.TSL2591()

def generate_json(rpi_serial):
    vcg_path = '/opt/vc/bin/vcgencmd'

    outs = []

    outs.append(rpi_serial)
    outs.append(get_output_date()[1:-1])
    outs.append(str(time.time()))

    print("Time before get vis: " + str(time.time()))
    outs.append(get_visible_light())
    print("Time after get vis: " + str(time.time()))

    outs.append(get_output_armclock(vcg_path))
    outs.append(get_temp())

    # Format
    for i in range(len(outs)):
        if('=' in outs[i]):
            outs[i] = outs[i][outs[i].find('=')+1:]
        if(': ' in outs[i]):
            outs[i] = outs[i][outs[i].find(' ')+1:]

    json_file = get_json_from_outputs(outs)
    print(json_file)
    return json_file

def get_visible_light():
    return str(sensor.Read_Visible)

def get_temp():
     out = subprocess.Popen(['/opt/vc/bin/vcgencmd', 'measure_temp'],
             stdout=subprocess.PIPE,
             stderr=subprocess.STDOUT).communicate()[0]
     return out.decode('UTF-8')[:-3]

def get_rpi_serial():
    ps = subprocess.Popen(('cat', '/proc/cpuinfo'), stdout=subprocess.PIPE)
    out = subprocess.check_output(('grep', 'Serial'), stdin=ps.stdout).decode('UTF-8')
    return out[:-1]

def get_output_date():
    out = subprocess.Popen(['date', '+"%Y-%m-%d %H:%M:%S"'],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT).communicate()[0]
    return out.decode('UTF-8')[:-1]

def get_output_armclock(path):
    out = subprocess.Popen([path, 'measure_clock arm'],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT).communicate()[0]
    return out.decode('UTF-8')[:-1]

def get_json_from_outputs(outs):
    data = {
        "rpi_serial": outs[0],
        "date": outs[1],
        "time": outs[2],
        "visible_light": outs[3],
        "armclock": outs[4],
        "temp": outs[5]
        }
    json_outs = json.dumps(data)    
    return json_outs

def send_json(jf):
    producer.produce('fast_light_iot', jf)
    return


producer = Producer({
        'bootstrap.servers': 'vm-int-confluent.westeurope.cloudapp.azure.com:9092',
        'sasl.mechanisms': 'PLAIN',
        'security.protocol': 'PLAINTEXT',
        'sasl.username': '43TQLH7DM56KLFXO',
        'sasl.password': '5FV7liTi+RcfDgFyXW+TDZpPHSIi0dsN9UmCJ6CvEoniY5J8mWbj8Y0lKxU1fn9u'
    })


rpi_serial = get_rpi_serial()

exe_duration = int(input('Enter duration of execution in seconds : '))
timeout = time.time() + exe_duration
cnt = 1

if (exe_duration > 10000):
    sys.exit()

while True:
    jf = generate_json(rpi_serial)
    send_json(jf)
    print("######################################################################################################################## Message  " +  str(cnt) + " sent.")
    cnt = cnt + 1
    if time.time() > timeout:
        break
    time.sleep(0.1)
  

