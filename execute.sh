#!/bin/bash

nohup /usr/bin/python CreateKafkaConsumer.py >> /home/uttiya1/logs/kafka_consumer.log &


nohup /usr/bin/python CreatePubSubBQPipeline.py test_bqdataset >> /home/uttiya1/logs/pub_sub_bquery.log


