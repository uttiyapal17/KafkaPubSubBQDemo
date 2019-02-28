

import CreateKafkaConsumer
import CreatePubSubBQPipeline
from sys import argv,exit

class Main(object):

    def run(self):

        print("Kafka-PubSub-BigQuery")
        CreateKafkaConsumer.kafka_pubsub_pipeline()

        #if len(argv) != 1:
         #   print("Invalid nuber of arguments passed!")
          #  exit(1)
        #else:
        args="test_bqdataset"
        print("Big Query argument : "+args)
        CreatePubSubBQPipeline.pubSubBQPipeline(args)




if __name__ == '__main__':
    Main().run()
