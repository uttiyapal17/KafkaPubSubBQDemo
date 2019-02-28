
from google.cloud import pubsub_v1
import os
import time

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/opt/pubsubapikey.json"

def createPubSubTopic():
    # PubSub Topic Creation
    project_id = "pubsub-bigquery"
    topic_name = "testkafka-pub1"

    publisher = pubsub_v1.PublisherClient()
    # The `topic_path` method creates a fully qualified identifier
    # in the form `projects/{project_id}/topics/{topic_name}`
    topic_path = publisher.topic_path(project_id, topic_name)
    topic = publisher.create_topic(topic_path)
    print('Topic created: {}'.format(topic))


def createSubscription():
    #os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/pubsubapikey.json"

    project_id = "pubsub-bigquery"
    topic_name = "testkafka-pub1"
    subscription_name = "testkafka-sub1"

    subscriber = pubsub_v1.SubscriberClient()
    topic_path = subscriber.topic_path(project_id, topic_name)
    # The `subscription_path` method creates a fully qualified identifier
    # in the form `projects/{project_id}/subscriptions/{subscription_name}`
    subscription_path = subscriber.subscription_path(project_id, subscription_name)

    subscription = subscriber.create_subscription(subscription_path, topic_path)
    print('Subscription created: {}'.format(subscription))

    # [END pubsub_create_pull_subscription]

    def callback(message):
        print('Received message: {}'.format(message))
        message.ack()

    subscriber.subscribe(subscription_path, callback=callback)

    # The subscriber is non-blocking. We must keep the main thread from
    # exiting to allow it to process messages asynchronously in the background.
    print('Listening for messages on {}'.format(subscription_path))
    while True:
        time.sleep(60)


createPubSubTopic()
createSubscription()
