#!/usr/bin/env python
# coding: utf-8 
# Gao Ming Ming Create At 2020-12-24 10:30:29
# Description:some description
from admin import Admin


class TestAdmin(object):
    def __init__(
            self,
            url=None,
            schema=None,
            tenant=None,
            namespace=None
        ):
        self.admin = Admin(url,schema,tenant,namespace)

    def test_create_topic(self):
        topic_name = "mytest_topic"
        self.admin.create_topic(topic_name)
        self.admin.delete_topic(topic_name)

    def test_delete_topic(self):
        topic_name = "mytest_delete_topic"
        self.admin.create_topic(topic_name)
        self.admin.delete_topic(topic_name)

    def test_unload_topic(self):
        topic_name = "mytest_upload_topic"
        self.admin.create_topic(topic_name)
        self.admin.unload_topic(topic_name)
        self.admin.delete_topic(topic_name)


    def test_create_partition_topic(self):
        topic_name = "mytest_partition_topic"
        num_partitions = 5
        self.admin.create_partition_topic(topic_name,num_partitions)
        self.admin.delete_partition_topic(topic_name)

    def test_delete_partition_topic(self):
        topic_name = "mytest_delete_partition_topic"
        self.admin.create_partition_topic(topic_name,5)
        self.admin.delete_partition_topic(topic_name)

    def test_update_partition_topic_nums(self):
        topic_name = "mytest_update_partition_topic_nums10"
        num_partitions = 5
        new_num_partitions = 9
        self.admin.create_partition_topic(topic_name,num_partitions)
        self.admin.update_partition_topic(topic_name,new_num_partitions)
        self.admin.delete_partition_topic(topic_name)

    def test_last_message_id(self):
        topic_name = "mytest_topic_last_message_id"
        self.admin.create_topic(topic_name)
        self.admin.last_message_id(topic_name)
        self.admin.delete_topic(topic_name)


if __name__ == "__main__":
    mytest = TestAdmin()
    mytest.test_create_topic()
    mytest.test_delete_topic()
    mytest.test_unload_topic()
    mytest.test_create_partition_topic()
    mytest.test_delete_partition_topic()
    mytest.test_update_partition_topic_nums()
    #mytest.test_last_message_id()

