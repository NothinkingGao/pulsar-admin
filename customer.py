#!/usr/bin/env python
# coding: utf-8 
# Gao Ming Ming Create At 2020-12-17 17:29:54
# Description:some description

import pulsar

client = pulsar.Client('pulsar://localhost:6650')
consumer = client.subscribe('my-topic-partition-0',
                            subscription_name='my-sub')

while True:
    msg = consumer.receive()
    print("Received message: '%s'" % msg.data())
    consumer.acknowledge(msg)

client.close()

