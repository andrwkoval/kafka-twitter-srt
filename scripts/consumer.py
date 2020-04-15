import datetime
import pandas as pd
from json import dumps, loads, dump
from kafka import KafkaConsumer, TopicPartition
from producer import SERVERS
import fire
import boto3
from botocore.exceptions import NoCredentialsError


def create_report(n4: int = 3, n5: int = 3):
    users_consumer = KafkaConsumer(
        bootstrap_servers=SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="my-group",
        value_deserializer=lambda x: x.decode('utf-8')
    )

    tweets_consumer = KafkaConsumer(
        bootstrap_servers=SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    )

    report_time = datetime.datetime.now()

    question1 = list_all_accounts(users_consumer)
    upload_result("question_1.json", question1)

    question2 = tweets_from_active(tweets_consumer, report_time)
    upload_result("question_2.json", question2)

    question3 = aggregated_statistics(tweets_consumer, report_time)
    upload_result("question_3.json", question3)

    question4 = most_producing_accounts(tweets_consumer, report_time, n4)
    upload_result("question_4.json", question4)

    question5 = most_popular_hashtags(tweets_consumer, report_time, n5)
    upload_result("question_5.json", question5)


def upload_result(filename, result):
    with open(filename, "w") as write_file:
        dump(result, write_file)

    # specify credentials
    s3 = boto3.client('s3', aws_access_key_id="",
                      aws_secret_access_key="",
                      aws_session_token="")
    try:
        # specify bucket name
        s3.upload_file(filename, "", filename)
        print("Uploaded!")
    except FileNotFoundError:
        print("The file was not found")
    except NoCredentialsError:
        print("Credentials not available")


def all_users(consumer: KafkaConsumer):
    partitions = [TopicPartition("users", partition) for partition in consumer.partitions_for_topic("users")]
    consumer.assign(partitions)
    sizes = partition_sizes(consumer, partitions)
    while sum(sizes.values()) > 0:
        data = next(consumer)
        if sizes[data.partition] <= 0: continue
        sizes[data.partition] -= 1
        yield data


def all_tweets(consumer: KafkaConsumer):
    partitions = [TopicPartition("tweets", partition) for partition in consumer.partitions_for_topic("tweets")]
    consumer.assign(partitions)
    sizes = partition_sizes(consumer, partitions)
    while sum(sizes.values()) > 0:
        data = next(consumer)
        if sizes[data.partition] <= 0: continue
        sizes[data.partition] -= 1
        yield data


def partition_sizes(consumer, partitions):
    consumer.seek_to_end()
    sizes = {partition.partition: consumer.position(partition) for partition in partitions}
    consumer.seek_to_beginning()
    for partition in partitions: sizes[partition.partition] -= consumer.position(partition)
    return sizes


def list_all_accounts(consumer):
    users = set()
    for user in all_users(consumer):
        users.add(user.value)
    return list(users)


def tweets_from_active(consumer, report_time: datetime.datetime):
    users = dict()
    for record in all_tweets(consumer):
        tweet = record.value
        if tweet["user_id"] not in users.keys():
            users[tweet["user_id"]] = {"tweets": [], "count": 0}
        user_tweets = users[tweet["user_id"]]

        date = tweet["created_at"].split(".")[0]
        created_at = datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
        diff = report_time - created_at
        if diff.seconds < 3 * 60 * 60:
            user_tweets["count"] += 1

        if len(user_tweets["tweets"]) == 10:
            user_tweets["tweets"] = user_tweets["tweets"][1:]
        user_tweets["tweets"].append(tweet["content"])

        users[tweet["user_id"]] = user_tweets

    result = sorted(users.items(), key=lambda x: x[1]["count"], reverse=True)[:10]
    return {record[0]: record[1]["tweets"] for record in result}


def aggregated_statistics(consumer, report_time: datetime.datetime):
    users = dict()
    for record in all_tweets(consumer):
        tweet = record.value
        if tweet["user_id"] not in users.keys():
            users[tweet["user_id"]] = [0, 0, 0]
        user_tweet_freq = users[tweet["user_id"]]

        date = tweet["created_at"].split(".")[0]
        created_at = datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
        diff = report_time - created_at
        if diff.seconds >= 3 * 60 * 60:
            continue
        if diff.seconds < 60 * 60:
            user_tweet_freq[0] += 1
        elif diff.seconds < 2 * 60 * 60:
            user_tweet_freq[1] += 1
        else:
            user_tweet_freq[2] += 1
        users[tweet["user_id"]] = user_tweet_freq
    return users


def most_producing_accounts(consumer, report_time: datetime.datetime, n: int):
    users = dict()
    for record in all_tweets(consumer):
        tweet = record.value
        if tweet["user_id"] not in users.keys():
            users[tweet["user_id"]] = 0

        date = tweet["created_at"].split(".")[0]
        created_at = datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
        diff = report_time - created_at
        if diff.seconds < n * 60 * 60:
            users[tweet["user_id"]] += 1

    top_users = sorted(users.items(), key=lambda x: x[1], reverse=True)[:20]
    return [i[0] for i in top_users]


def most_popular_hashtags(consumer, report_time: datetime.datetime, n: int):
    hashtags = dict()
    for record in all_tweets(consumer):
        tweet = record.value
        date = tweet["created_at"].split(".")[0]
        created_at = datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
        diff = report_time - created_at
        if diff.seconds >= n * 60 * 60:
            continue
        local_tags = list(filter(lambda x: x.startswith("#"), tweet["content"].split()))
        for tag in local_tags:
            if tag not in hashtags.keys():
                hashtags[tag] = 0
            hashtags[tag] += 1

    top_hashtags = sorted(hashtags.items(), key=lambda x: x[1], reverse=True)[:10]
    return [i[0] for i in top_hashtags]


if __name__ == '__main__':
    fire.Fire(create_report)
