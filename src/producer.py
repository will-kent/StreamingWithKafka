import configparser
import praw
from confluent_kafka import Producer
import socket
import json
from multiprocessing import Process
import logging

# Read passwords and secrets from config file
config_parser = configparser.ConfigParser()
config_parser.read("configuration/config.cfg")

# Set parameters
reddit_client_id = config_parser["praw"]["client_id"]
reddit_client_secret = config_parser["praw"]["client_secret"]
reddit_password = config_parser["praw"]["password"]
reddit_username = config_parser["praw"]["username"]
reddit_agent = config_parser["praw"]["user_agent"] + reddit_username
sub_reddit = config_parser["praw"]["subreddit"]
broker = "broker:29092"
functions = ["producer_submissions", "producer_comments"]


def acknowledged(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))


def create_producer(bootstrap_broker):
    # Create producer
    conf = {'bootstrap.servers': bootstrap_broker,
            'client.id': socket.gethostname()}
    producer = Producer(conf)
    return producer


def producer_submissions(subreddit, broker_name):
    producer = create_producer(bootstrap_broker=broker_name)

    for submission in subreddit.stream.submissions():
        str_submission = str(vars(submission))
        json_submission = json.dumps(str_submission)
        data = json.loads(json_submission)
        producer.produce("submission", key=submission.id, value=data, callback=acknowledged)


def producer_comments(subreddit, broker_name):
    producer = create_producer(bootstrap_broker=broker_name)

    for comment in subreddit.stream.comments():
        str_comment = str(vars(comment))
        json_comment = json.dumps(str_comment)
        data = json.loads(json_comment)
        try:
            producer.produce("comments", key=comment.id, value=data, callback=acknowledged)
        except:
            print("failure")


def run_in_parallel(*fns, subreddit, broker_name):
    proc = []
    for fn in fns:
        msg = "Starting function {}".format(fn)
        logging.info(msg)
        print(msg)
        p = Process(target=fn, args=(subreddit, broker_name))
        p.start()
        proc.append(p)
    for p in proc:
        p.join()


# Establish connection to reddit
reddit = praw.Reddit(
    client_id=reddit_client_id,
    client_secret=reddit_client_secret,
    password=reddit_password,
    user_agent=reddit_agent,
    username=reddit_username,
)

# Assign praw subreddit
subreddit = reddit.subreddit(sub_reddit)

# Start producers
run_in_parallel(producer_submissions, producer_comments, subreddit=subreddit, broker_name=broker)
