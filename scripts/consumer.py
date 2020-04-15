import datetime
import pandas as pd
from json import dumps, loads
from kafka import KafkaConsumer, TopicPartition
from producer import SERVERS
import fire


def create_report(task: int, n: int = 3):
    if not isinstance(task, int) or task not in list(range(1, 6)):
        raise ValueError("Wrong task number")

    users_consumer = KafkaConsumer(
        "users",
        bootstrap_servers=SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda x: x.decode('utf-8')
    )

    tweets_consumer = KafkaConsumer(
        "tweets",
        bootstrap_servers=SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    )

    questions = [list_all_accounts]
    answer = questions[task - 1]

    consumer = tweets_consumer
    if task == 1:
        consumer = users_consumer

    print(answer(consumer))


def all_tweets(consumer: KafkaConsumer):
    partitions = [TopicPartition("tweets", partition) for partition in consumer.partitions_for_topic("tweets")]
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
    for user in consumer:
        users.add(user)

    return list(users)


def tweets_from_active(consumer, report_time: datetime.datetime):
    users = dict()
    for tweet in all_tweets(consumer):
        if tweet.user_id not in users.keys():
            users[tweet.user_id] = {"tweets": [], "count": 0}
        user_tweets = users[tweet.user_id]

        created_at = datetime.datetime.strptime(tweet["time"], "%d/%m/%Y %H:%M:%S")
        diff = report_time - created_at
        if diff.seconds < 3 * 60 * 60:
            user_tweets["count"] += 1

        if len(user_tweets["tweets"]) == 10:
            user_tweets["tweets"] = user_tweets["tweets"][1:]
        user_tweets["tweets"].append(tweet.content)

        users[tweet.user_id] = user_tweets

    result = sorted(users.items(), key=lambda x: x[1]["number"], reverse=True)[:10]
    return {record[0]: record[1]["tweets"] for record in result}


def aggregated_statistics(consumer, report_time: datetime.datetime):
    users = dict()
    for tweet in all_tweets(consumer):
        if tweet.user_id not in users.keys():
            users[tweet.user_id] = [0, 0, 0]
        user_tweet_freq = users[tweet.user_id]
        created_at = datetime.datetime.strptime(tweet["created_at"], "%d/%m/%Y %H:%M:%S")
        diff = report_time - created_at
        if diff.seconds >= 3 * 60 * 60:
            continue
        if diff.seconds < 60 * 60:
            user_tweet_freq[0] += 1
        elif diff.seconds < 2 * 60 * 60:
            user_tweet_freq[1] += 1
        else:
            user_tweet_freq[2] += 1
        users[tweet.user_id] = user_tweet_freq
    return users


def most_producing_accounts(consumer, report_time: datetime.datetime, n: int):
    users = dict()
    for tweet in all_tweets(consumer):
        if tweet.user_id not in users.keys():
            users[tweet.user_id] = 0
        created_at = datetime.datetime.strptime(tweet["created_at"], "%d/%m/%Y %H:%M:%S")
        diff = report_time - created_at
        if diff.seconds < n * 60 * 60:
            users[tweet.user_id] += 1

    top_users = sorted(users.items(), key=lambda x: x[1], reverse=True)[:20]
    return [i[0] for i in top_users]


def most_popular_hashtags(consumer, report_time: datetime.datetime, n: int):
    hashtags = dict()
    for tweet in all_tweets(consumer):
        created_at = datetime.datetime.strptime(tweet["created_at"], "%d/%m/%Y %H:%M:%S")
        diff = report_time - created_at
        if diff.seconds >= n * 60 * 60:
            continue
        local_tags = list(filter(lambda x: x.startswith("#"), tweet.content.split()))
        for tag in local_tags:
            if tag not in hashtags.keys():
                hashtags[tag] = 0
            hashtags[tag] += 1

    top_hashtags = sorted(hashtags.items(), key=lambda x: x[1], reverse=True)[:10]
    return [i[0] for i in top_hashtags]

if __name__ == '__main__':
    fire.Fire(create_report)
