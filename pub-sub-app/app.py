import os
import json
from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError
import time
import json
import mysql.connector
import ast


# GCP topic, project & subscription ids
PUB_SUB_TOPIC = os.environ.get('PUB_SUB_TOPIC')
PUB_SUB_PROJECT = os.environ.get('PUB_SUB_PROJECT')
PUB_SUB_SUBSCRIPTION = os.environ.get('PUB_SUB_SUBSCRIPTION')
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=os.environ.get('GOOGLE_CREDS_KEY')
filepath = os.environ.get('FILE_PATH')
# Pub/Sub consumer timeout
timeout = 3.0

HOST=os.environ.get('HOST')
USR = os.environ.get('USER'),
PASS = os.environ.get('PASSWORD')
DB = os.environ.get('DATABASE')
TABLE = os.environ.get('TABLE')

def exponential_backoff(max_retries=5, base_delay=1):
    def decorator_retry(func):
        def wrapper(*args, **kwargs):
            retries = 0
            delay = base_delay

            while retries < max_retries:
                try:
                    result = func(*args, **kwargs)
                    return result
                except Exception as e:
                    print(f"Error occurred: {str(e)}")
                    print(f"Retrying in {delay} seconds...")
                    time.sleep(delay)

                    # Increase the delay exponentially for the next retry
                    delay *= 2
                    retries += 1
            else:
                # Handle the case when all retries have failed
                print("All retries failed.")
                return None

        return wrapper

    return decorator_retry


def process_payload(message):
    """Function to handle and process payloads that have been consumed.

    Args:
        message: payload received from pub/sub
    """
    
    print(f"Received {message.data}.")
    message.ack()    
    load_into_mysql(message.data)

@exponential_backoff(max_retries=3, base_delay=1)
def push_payload(payload, topic, project):
    """Function responsible for publishing a message to a topic.

    Args:
        payload: payload that needs to be published
        topic: topic that will receive payload
        project: google project in which google pub/sub is deployed
    """
    try:        
        publisher = pubsub_v1.PublisherClient() 
        topic_path = publisher.topic_path(project, topic)        
        data = json.dumps(payload).encode("utf-8")           
        future = publisher.publish(topic_path, data=data)
        print("Pushed message to topic.")
    except Exception as e:
        print("Error occured while publishing payload to topic")   

@exponential_backoff(max_retries=3, base_delay=1)
def consume_payload(project, subscription, callback, period):
    """Function that retrieves messages from topics within a specified timeout period.

    Args:
        project: google project in which google pub/sub is deployed
        subscription: subscriber from data will be consumed
        callback: function to process payload
        period: deplay between producer and consumer
    """
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project, subscription)
    print(f"Listening for messages on {subscription_path}..\n")
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    # Enclose the subscriber in a 'with' block to ensure the close() method is automatically called upon completion.
    with subscriber:
        try:
            # If the timeout parameter is not provided, the result() function will wait indefinitely until a result is obtained,
            # unless an exception occurs before that.                
            streaming_pull_future.result(timeout=period)
        except TimeoutError:
            streaming_pull_future.cancel()
 
@exponential_backoff(max_retries=3, base_delay=1)               
def payload_producer(filepath):
    """Function that act as a producer for pub/sub

    Args:
        filepath: location to the file that contains data to be produced
    """
    try:
        with open(filepath, 'r') as file:
            for line in file:
                yield line.strip()
    except Exception as e:
        print("Error while producing payload",e)

@exponential_backoff(max_retries=3, base_delay=1)           
def load_into_mysql(stream):
    """Function to presist payload

    Args:
        stream: payload produced by google pub/sub
    """
    try:
        connection = mysql.connector.connect(
            host = HOST,
            user = USR,
            password = PASS,
            database = DB
        )
        cursor = connection.cursor()
        insert_stmt = (
            f"INSERT INTO {TABLE} (AirportCode, CountryName, Region) "
            "VALUES (%s, %s, %s)"
        )
        cursor.execute(insert_stmt, tuple(eval(ast.literal_eval(stream.decode('utf-8'))).values()))
        connection.commit()
        print('Row inserted successfully!')
    except Exception as e:
        print('Error inserting row:', e)



for value in payload_producer(filepath):
    print("===================================")
    payload = value
    print(f"Sending payload: {payload}.")
    push_payload(payload, PUB_SUB_TOPIC, PUB_SUB_PROJECT)
    consume_payload(PUB_SUB_PROJECT, PUB_SUB_SUBSCRIPTION, process_payload, timeout)
    time.sleep(1)