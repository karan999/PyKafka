#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 17 21:28:33 2018

@author: karan

This script will try to push messages to a KafkaClient 

"""

from pykafka import KafkaClient
import sys
import time
import json 

client = KafkaClient( hosts = "127.0.0.1:9092" )

topic = client.topics[b'test']

with open( 'test.json' , 'r') as f:
    
    data = json.load( f )

          
with topic.get_producer( delivery_reports = True ) as producer:
    
    count = 0 
    
    while True:
        
        for i, j in data.items():
            
            i = i.encode('utf-8')
                        
            producer.produce(i) 
            
            j = bytes(j)
            
            producer.produce(j)
        
            time.sleep( 5 )
        
            count += 1 
        
        if count > 1:
            producer.produce( b'Finished')
            sys.exit()
        