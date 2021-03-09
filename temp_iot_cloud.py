#!/usr/bin/env python

import os
import sys
from time import sleep
import json
import subprocess
import re
from confluent_kafka import Producer




def generate_json(interval):
    vcg_path = '/opt/vc/bin/vcgencmd'

    commands = ['measure_clock arm', 'measure_clock H264',
                'measure_clock emmc', 'measure_volts core', 'measure_volts sdram_c',
                'measure_volts sdram_i']

    outs = []
    temps = []

    for i in range(interval):
         temps.append(get_temp())
         sleep(1)

    outs.append(get_rpi_serial())
    outs.append(get_output_date()[1:-1])

    for i in range(len(commands)):
        outs.append(get_output_proc(vcg_path, commands[i]))

    for i in range(len(outs)):
        if('=' in outs[i]):
            outs[i] = outs[i][outs[i].find('=')+1:]
        if(': ' in outs[i]):
            outs[i] = outs[i][outs[i].find(' ')+1:]

    for i in range(len(temps)):
         if('=' in temps[i]):
             temps[i] = temps[i][temps[i].find('=')+1:]


    json_file = get_json_from_outputs(outs, temps)
    print(json_file)
    return json_file


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

def get_output_proc(path, command):
    out = subprocess.Popen([path, command],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT).communicate()[0]
    return out.decode('UTF-8')[:-1]

def get_json_from_outputs(outs, temps):
    data = {
        "rpi_serial": outs[0],
        "date": outs[1],
        "temps": temps,
        "clocks": {
            "arm": outs[2],
            "H264": outs[3],
            "emmc": outs[4]
            },
        "volts": {
            "core": outs[5][:-1],
            "sdram_c": outs[6][:-1],
            "sdram_i": outs[7][:-1]
            }
        }

    json_outs = json.dumps(data)
    return json_outs

def send_json(jf):
    producer.produce('rpi', jf)
    return


producer = Producer({
        'bootstrap.servers': 'pkc-lq8gm.westeurope.azure.confluent.cloud:9092',
        'sasl.mechanisms': 'PLAIN',
        'security.protocol': 'SASL_SSL',
        'sasl.username': '<kafka public key>',
        'sasl.password': '<kafka secret key>'
    })

n_messages = int(input('Enter number of messages to send : ')) 
interval = int(input('Interval in seconds : '))


if (n_messages > 10000):
    sys.exit()

for i in range(n_messages):
    jf = generate_json(interval)
    send_json(jf)
    print("### Message  " +  str(i) + " sent.###")

