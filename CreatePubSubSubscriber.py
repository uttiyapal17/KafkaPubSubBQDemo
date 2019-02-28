import time

from google.cloud import pubsub_v1
import os


def createSubscription():

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/opt/pubsubapikey.json"

    project_id = "pubsub-bigquery"
    topic_name = "testkafka-pub"
    subscription_name = "testkafka-sub"


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







