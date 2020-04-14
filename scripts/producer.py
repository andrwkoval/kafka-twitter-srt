from time import sleep
import datetime
import pandas as pd
from json import dumps
from kafka import KafkaProducer

SERVERS = ["52.30.185.141:9092", "18.203.224.177:9092", "52.31.188.175:9092"]
DATASET = "../training.1600000.processed.noemoticon.csv"

users_producer = KafkaProducer(bootstrap_servers=SERVERS,
                               value_serializer=lambda x: x.encode('utf-8'))
tweets_producer = KafkaProducer(bootstrap_servers=SERVERS,
                                value_serializer=lambda x: dumps(x).encode('utf-8'))

df = pd.read_csv(DATASET, chunksize=40, names=['target',
                                               'id',
                                               'date',
                                               'flag',
                                               'user',
                                               'text'])

if __name__ == '__main__':
    unique_users = set()
    for i in df:
        rows = i.iterrows()
        for j, data in rows:
            if data.user not in unique_users:
                users_producer.send("users", value=data.user).add_errback(lambda x: print(f"#error# {x}"))
                unique_users.add(data.user)

            tweets_producer.send("tweets", value={
                "user_id": data.user,
                "created_at": str(datetime.datetime.now()),
                "content": data.text
            }).add_errback(lambda x: print(f"#error# {x}"))

            sleep(1)

    users_producer.flush()
    tweets_producer.flush()