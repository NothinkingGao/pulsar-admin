#!/usr/bin/env python
# coding: utf-8 
# Gao Ming Ming Create At 2020-12-17 17:28:11
# Description:some description
import pulsar

client = pulsar.Client('pulsar://localhost:6650')
topic ="topgao"
producer = client.create_producer(topic)

for i in range(10):
    producer.send(('hello-pulsar-%d' % i).encode('utf-8'))

client.close()
