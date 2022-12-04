

from cassandra.cluster import Cluster
import pandas as pd

cluster = Cluster()

session = cluster.connect("demo_twitter")

rows = session.execute("select * from demo_twitter_7")

cols = ['uuid', 'screen_name', 'created_at', 'followers', 'location', 'favorite', 'retweet', 'source', 'text', 'statuses']
# get dataframe on pandas
def process_results(rows):

    twitter_list = []
    count = 0
    for row in rows:
        twitter_list.append([row.uuid, row.screen_name, row.created_at, row.followers, row.location, row.favorite, row.retweet, row.source, row.description, row.statuses])
    data_set = pd.DataFrame(twitter_list, columns=cols)
    return data_set

#Preprocessing data
df_process = process_results(rows)
df_process.to_csv("./dataset/dataset_5.csv")
# print(process_results(rows))
# process_results(rows)
print("Finished")