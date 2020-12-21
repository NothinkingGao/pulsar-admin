#!/usr/bin/env python
# coding: utf-8 
# Gao Ming Ming Create At 2020-12-18 13:47:18
# Description:封装pulsar的admin的操作
import json
import requests

class Admin(object):
    def __init__(
            self,
            url= None,
            schema = None,
            tenant = None,
            namespace = None
        ):
        self.url = url or "http://localhost:8080/admin/v2"
        self.schema = schema or "persistent"
        self.tenant = tenant or "public"
        self.namespace = namespace or "default"

    @property
    def prev_url(self):
        '''
            请求url前缀
        '''
        url = "{url}/{schema}/{tenant}/{namespace}".format(
            url = self.url,
            schema = self.schema,
            tenant = self.tenant,
            namespace = self.namespace,
        )
        return url
    
    
    def stats(self,topic):
        '''
            查看一个主题的状态
        '''
        url = "{prev_url}/{topic}/stats".format(
            prev_url = self.prev_url,
            topic = topic
        )
        response = requests.get(url)
        return response.json()
    
    def create_topic(self,topic):
        '''
            创建一个主题
        '''
        url = "{prev_url}/{topic}/stats".format(
            prev_url = self.prev_url,
            topic = topic
        )
        response = requests.put(url)
        return response.json()
    
    def create_partition_topic(self,topic,num_partitions):
        '''
            创建分区主题
        '''
        url = "{prev_url}/{topic}/partitions".format(
            prev_url = self.prev_url,
            topic = topic,
        )
        headers = {
            'Content-type':'application/json'
        }
        data = str(num_partitions)
        #data = json.dumps(data)
        response = requests.put(url,data = data,headers = headers)
        print(response.status_code)
        print(response.text)
        #return response.json()
    


def main():
    admin = Admin()
    #stats(topic)
    #create_topic(topic)
    admin.create_partition_topic("topgao",6)


if __name__ == "__main__":
    main()


