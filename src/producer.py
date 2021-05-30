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
        # Create Submissions JSON and produce as submissions topic
        data = {}
        data['author_fullname'] = submission.author_fullname
        data['title'] = submission.title
        data['subreddit_name_prefixed'] = submission.subreddit_name_prefixed
        data['name'] = submission.name
        data['upvote_ratio'] = submission.upvote_ratio
        data['ups'] = submission.ups
        data['score'] = submission.score
        data['author_premium'] = submission.author_premium
        data['created'] = submission.created
        data['domain'] = submission.domain
        data['url_overridden_by_dest'] = submission.url_overridden_by_dest
        data['over_18'] = submission.over_18
        data['subreddit_id'] = submission.subreddit_id
        data['permalink'] = submission.permalink
        data['parent_whitelist_status'] = submission.parent_whitelist_status
        data['url'] = submission.url
        data['created_utc'] = submission.created_utc
        dump_submission = json.dumps(data)
        try:
            producer.produce("submissions", key=submission.id, value=dump_submission, callback=acknowledged)
        except Exception as e:
            logging.error(e)


def producer_comments(subreddit, broker_name):
    # Create Comments JSON and produce as comments topic
    producer = create_producer(bootstrap_broker=broker_name)

    for comment in subreddit.stream.comments():
        data = {}
        data['id'] = comment.id
        data['subreddit_id'] = comment.subreddit_id
        data['body'] = comment.body
        data['link_title'] = comment.link_title
        data['name'] = comment.name
        data['permalink'] = comment.permalink
        data['link_permalink'] = comment.link_permalink
        data['link_author'] = comment.link_author
        data['link_url'] = comment.link_url
        data['created'] = comment.created
        data['ups'] = comment.ups
        dump_comment = json.dumps(data)
        try:
            producer.produce("comments", key=comment.id, value=dump_comment, callback=acknowledged)
        except Exception as e:
            logging.error(e)


def run_in_parallel(*fns, subreddit, broker_name):
    # Run functions in parallel
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
