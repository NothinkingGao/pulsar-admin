#!/usr/bin/env python
# coding: utf-8 
# Gao Ming Ming Create At 2020-12-17 17:29:54
# Description:some description

import pulsar

client = pulsar.Client('pulsar://localhost:6650')

topic = 'big_apple'
#print(client.get_topic_partitions(topic))
consumer = client.subscribe(topic,subscription_name='my-sub')

print(consumer)

while True:
    msg = consumer.receive()
    print(msg.data())
    consumer.acknowledge(msg)

client.close()

