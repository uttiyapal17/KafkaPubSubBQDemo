from confluent_kafka import Consumer, KafkaError
from google.cloud import pubsub_v1
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/opt/pubsubapikey.json"


def kafka_pubsub_pipeline():

    settings = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'kafka-con-group',
        'client.id': 'client-1',
        'enable.auto.commit': True,
        'session.timeout.ms': 6000,
        'default.topic.config': {'auto.offset.reset': 'smallest'}
    }


    # PubSub Topic details
    project_id = "pubsub-bigquery"
    topic_name = "testkafka-pub1"

    publisher = pubsub_v1.PublisherClient()
    # The `topic_path` method creates a fully qualified identifier
    # in the form `projects/{project_id}/topics/{topic_name}`
    topic_path = publisher.topic_path(project_id, topic_name)

    c = Consumer(settings)
    c.subscribe(['testkafka1'])

    try:
        while True:
            msg = c.poll(0.1)
            if msg is None:
                continue
            elif not msg.error():
            #print('Received message: {0}'.format(msg.value()))

                # Consumed Messages are re Published to PubSub
                data = 'Kafka Sending {0}'.format(str(msg.value()))
                # Data must be a bytestring
                data = data.encode('utf-8')
                # When you publish a message, the client returns a future.
                future = publisher.publish(topic_path, data=data)
                print('Published {} of message ID {}.'.format(data, future.result()))

            elif msg.error().code() == KafkaError._PARTITION_EOF:
                print('End of partition reached {0}/{1}'.format(msg.topic(), msg.partition()))
            else:
                print('Error occured: {0}'.format(msg.error().str()))

    except KeyboardInterrupt:
        pass

    finally:
        c.close()


kafka_pubsub_pipeline()
