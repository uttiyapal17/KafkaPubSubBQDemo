import json, time, ast
from google.cloud import pubsub_v1
from google.cloud import storage, bigquery
from sys import argv
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/opt/pubsubapikey.json"

def pubSubBQPipeline(args):

    dataset = args

    client = storage.Client()
    bigquery_client = bigquery.Client()
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path('pubsub-bigquery', 'testkafka-sub1')
    dataset_ref = bigquery_client.dataset(dataset)


    def load_bq(table, data):
        table_ref = dataset_ref.table(table)
        table = bigquery_client.get_table(table_ref)
        errors = bigquery_client.insert_rows(table, [data])

        if len(errors) > 0:
            print
            errors
        else:
            print
            "found 1 record, inserted that in BQ"

        return 0


    def callback(message):

        ts=time.time()
        #data = ( ts , str(message.data) )
        data = ( str(message.data), )
        print(data)

        #for jsn in json.loads(data.strip()):

            #metadata = jsn["metadata"]
            #table = (metadata["TableName"]).split('.')[1] + "_CT"
            #data = jsn["data"]
            #data['REC_TS'] = metadata["COMMIT_TIMESTAMP"]
            #data['IUD_FLAG'] = metadata["OperationName"]
        table ="test_table_1"
        try:
            load_bq(table, data)
            message.ack()
        except Exception as e:
            print(e)
            message.nack()


    subscriber.subscribe(subscription_path, callback=callback)
    # The subscriber is non-blocking, so we must keep the main thread from
    # exiting to allow it to process messages in the background.
    # try:
    #    future.result()
    # except Exception as ex:
    #    print "in Error"
    #    print ex
    #   subscription.close()

    print('Listening for messages on {}'.format(subscription_path))

    while True:
        time.sleep(60)


#pubSubBQPipeline(argv[1])
pubSubBQPipeline("test_bqdataset")
