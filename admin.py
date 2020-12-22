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

    def list_topics(self):
        '''
            列出topic list
        '''
        return requests.get(self.prev_url)
    
    
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
    
    def delete_topic(self,topic):
        '''
            删除一个主题
        '''
        url = "{prev_url}/{topic}".format(
            prev_url = self.prev_url,
            topic = topic
        )
        response = requests.delete(url)
        return response.json()
    def unload_topic(self,topic):
        '''
            卸载主题
        '''
        url = "{prev_url}/{topic}/unload".format(
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

    def update_partition_topic(self,topic,num_partitions):
        '''
            更新每个主题的partitions数
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


    def delete_partition_topic(self,topic):
        '''
            删除带partition主题
        '''
        url = "{prev_url}/{topic}/partitions".format(
            prev_url = self.prev_url,
            topic = topic,
        )
        headers = {
            'Content-type':'application/json'
        }
        data = {
            "force":True,
            "authoritative":False,
            "deleteSchema":False
        }
        response = requests.delete(url,data = data,headers = headers)
        return response

    def query_partition_status(self,topic,per_partition):
        '''
            统计每个分区主题的状态
        '''
        url = "{prev_url}/{topic}/partitioned-stats".format(
            prev_url = self.prev_url,
            topic = topic,
        )
        headers = {
            'Content-type':'application/json'
        }
        data = str(per_partition)
        #data = json.dumps(data)
        response = requests.get(url,data = data,headers = headers)
        print(response.status_code)
        print(response.text)

    def subscriptions(self,topic):
        '''
            获取订阅
        '''
        url = "{prev_url}/{topic}/subscriptions".format(
            prev_url = self.prev_url,
            topic = topic,
        )
        headers = {
            'Content-type':'application/json'
        }
        #data = json.dumps(data)
        response = requests.get(url,headers = headers)
        print(response.status_code)
        print(response.text)

    def un_subscriptions(self,topic,subscription_name):
        '''
            取消订阅
        '''
        url = "{prev_url}/{topic}/subscription".format(
            prev_url = self.prev_url,
            topic = topic,
        )
        headers = {
            'Content-type':'application/json'
        }
        #data = json.dumps(data)
        response = requests.delete(url,headers = headers)
        print(response.status_code)
        print(response.text)

def main():
    admin = Admin()
    #stats(topic)
    #create_topic(topic)
    admin.create_partition_topic("big_apple",5000)


if __name__ == "__main__":
    main()


