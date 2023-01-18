from cassandra.cluster import Cluster #python3 -m pip install cassandra-driver
import pandas as pd
import streamlit as st#python3 -m pip install streamlit
import plotly.express as px#python3 -m pip install plotly-express
import numpy as np
from datetime import datetime
from datetime import time
import re
import matplotlib.pyplot as plt
import missingno as msno
import re
from datetime import datetime
import seaborn as sns
import squarify
from PIL import Image
from wordcloud import WordCloud , STOPWORDS, ImageColorGenerator
from plotly.subplots import make_subplots
import plotly.graph_objects as go
from textblob import Word, TextBlob #python3 -m pip install textblob
from PIL import Image

#set config
# st.set_page_config( page_title="Bitcoin Twitter Dashboard", layout="wide", initial_sidebar_state='expanded')
st.set_page_config( page_title="Bitcoin Twitter Dashboard", initial_sidebar_state='expanded')

# get dataframe on pandas
cluster = Cluster()
session = cluster.connect("twitter")
rows = session.execute("select * from demo_twitter_7")
cols = ['uuid', 'screen_name', 'created_at', 'followers', 'location', 'favorite', 'retweet', 'source', 'text', 'statuses']
def process_results(rows):

    twitter_list = []
    count = 0
    for row in rows:
        twitter_list.append([row.uuid, row.screen_name, row.created_at, row.followers, row.location, row.favorite, row.retweet, row.source, row.description, row.statuses])
    data_set = pd.DataFrame(twitter_list, columns=cols)
    return data_set


# read data
df = process_results(rows)

#preprocess data
###Not null
df = df.replace(r'', np.NaN)
df.dropna(subset=['uuid', 'screen_name', 'created_at', 'followers', 'favorite', 'retweet', 'source', 'text', 'statuses'], inplace=True)
df['location'].fillna("Others", inplace=True)
###Conversation
df['followers'] = df['followers'].astype(int)
df['favorite'] = df['favorite'].astype(int)
df['retweet'] = df['retweet'].astype(int)
df['statuses'] = df['statuses'].astype(int)
df['created_at'] = df['created_at'].apply(lambda x: (datetime.strptime(x, '%Y-%m-%d %H:%M:%S')))
df['created_at'] = pd.to_datetime(df['created_at'])
df['tweet_hour'] = df['created_at'].apply(lambda x: x.strftime('%H'))
df['tweet_hour'] = df['tweet_hour'].astype(int)
df['created_at'] = df['created_at'].apply(lambda x: x.strftime('%Y-%m-%d'))
#Remove special character
df = df.drop(df[df.location.str.contains(r'[^0-9a-zA-Z]', na=False)].index)
df["location"] = df["location"].apply(lambda x: re.sub("[0-9]", "Others", x)).str.title()
def clean_text(text):
  text = re.sub('#[A-Za-z0-9]+', 'Bitcoin', text) #removes '#' from srings
  text = re.sub('\\n', '', text) #removes '\n' from bitcoin
  text = re.sub('RT[\s]+','',text) 
  text = re.sub('https?:\/\/\s+', '', text) #removes hyperlinks
  return text
df["clean_text"] = df["text"].apply(clean_text)

#Sentiment
#---------------------------------------------------------------------------------------------------------------------------------------------------------------------
# st.markdown("<h2 style='text-align: center; color: red;'>Visualization based on user emotions</h2>", unsafe_allow_html=True)

df_bitcoin = df[["clean_text"]]
df_bitcoin.head(5)

df_bitcoin["Polarity"] = df_bitcoin['clean_text'].apply(lambda x: TextBlob(x).sentiment[0])
df_bitcoin["Subjectivity"] = df_bitcoin['clean_text'].apply(lambda x: TextBlob(x).sentiment[1])
df_bitcoin.head(5)

def getSentiment(score):
  if score < 0:
    return "Negative"
  elif score == 0:
    return "Neutral"
  else:
    return "Positive"

df_bitcoin["Sentiment"] = df_bitcoin["Polarity"].apply(getSentiment)
# st.dataframe(df_bitcoin)

# st.dataframe(df)

# slide bar

st.sidebar.header("Filter here: ")

tweepy_top = st.sidebar.slider(
    "Schedule your top:",
    int(0), int(20),
    value=(int(0), int(20)))

tweepy_hour = st.sidebar.slider(
    "Schedule your twitter hour:",
    int(0), int(24),
    value=(int(0), int(24)))

container = st.sidebar.container()
all = st.sidebar.checkbox("Select all location", value=True)
 
if all:
    tweepy_location =  st.sidebar.multiselect(
        "Select the Location:",
        options=df['location'].unique(),
        default=df['location'].unique()
    )
else:
    tweepy_location =  st.sidebar.multiselect(
        "Select the Location:",
        options=df['location'].unique()
    )

df_selection = df.query(
    "tweet_hour >= @tweepy_hour[0] & tweet_hour <= @tweepy_hour[1] & location == @tweepy_location"
)


#main page
image = Image.open('index.png')
col1, col2 = st.columns((2,8))

with col1:
    st.image(image, width=100)

with col2:
    # st.sidebar.write('Bitcoin Twitter Dashboard')
    st.title("Bitcoin Twitter Dashboard")
# st.sidebar.image(image, caption='Twitter')

# st.title(":bar_chart: Bitcoin Twitter Dashboard")
st.markdown("""---""")

col1, col2, col3= st.columns(3)

with col1:
    twitter_location = df_selection['location'].value_counts().reset_index()
    twitter_location.columns = ['Location', 'No of twitter']
    twitter_location_top10 = twitter_location.head(10)

    fig_twitter_location = plt.figure()#figsize=(15,10)
    ax_twitter_location = fig_twitter_location.add_subplot(1,1,1)
    squarify.plot(sizes=twitter_location_top10['No of twitter'],label=twitter_location_top10['Location'],ax=ax_twitter_location,pad=True,
                value=twitter_location_top10['No of twitter'])
    plt.title("Top " + str(tweepy_top[1])+" popular locations in Twitter")
    st.pyplot(fig_twitter_location)

with col2:
    fig_scatter = plt.figure()#figsize=(15,10)
    # plt.figure(figsize=(10,6))
    plt.scatter(df_bitcoin["Polarity"], df_bitcoin["Subjectivity"], color = "purple")
    plt.title("The sentiment analysis scatter plot of bitcoin from Twitter")
    plt.xlabel("Polarity")
    plt.ylabel("Subjectivity")
    st.pyplot(fig_scatter)
with col3:
    bitcoin_counts = df_bitcoin["Sentiment"].value_counts()
    fig_sentiment, ax_sentiment = plt.subplots()
    ax_sentiment = bitcoin_counts.plot.bar(color='blue', edgecolor = 'black',linewidth = 1.25)
    ax_sentiment.set_title("Sentiment Analysis Bar Plot of Bitcoin from Twitter")
    ax_sentiment.set_xlabel("Sentiment")
    ax_sentiment.set_ylabel("The number of Tweets")
    st.pyplot( fig_sentiment)

st.markdown("<h3 style='text-align: center; color: black;'>Top " + str(tweepy_top[1])+" most used sources on Twitter</h3>", unsafe_allow_html=True)
source_counts = (
        df_selection["source"].value_counts().sort_values()[-(tweepy_top[1]):]
)
bottom = [index for index, item in enumerate(source_counts.index)]
y_labels = ["%s %.1f%%" % (item, 100.0*source_counts[item]/len(df_selection)) for index,item in enumerate(source_counts.index)]

fig_source_counts = px.bar(
        source_counts,
        x="source",
        y=y_labels,
        orientation="h",
        color_discrete_sequence=["#ff6347"] * len(source_counts),
        template="plotly_white",
    )
fig_source_counts.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis=(dict(showgrid=False)),
        width=500,
        height=500
    )
fig_source_counts.update_traces( marker_color= 'rgb(255, 99, 71)',marker_line_color='rgb(101, 39, 27)',
                    marker_line_width=1.5, opacity=0.6
                    )
st.plotly_chart(fig_source_counts, theme="streamlit", use_container_width=True)



# tab1, tab2 = st.tabs(["Top " + str(tweepy_top[1])+" most used sources on Twitter (default)", "Top " + str(tweepy_top[1])+" popular locations in Twitter"])
# with tab1:
#     # Use the Streamlit theme.
#     # This is the default. So you can also omit the theme argument.
#     st.markdown("<h3 style='text-align: center; color: black;'>Top " + str(tweepy_top[1])+" most used sources on Twitter</h3>", unsafe_allow_html=True)
#     source_counts = (
#         df_selection["source"].value_counts().sort_values()[-(tweepy_top[1]):]
#     )
#     bottom = [index for index, item in enumerate(source_counts.index)]
#     y_labels = ["%s %.1f%%" % (item, 100.0*source_counts[item]/len(df_selection)) for index,item in enumerate(source_counts.index)]

#     fig_source_counts = px.bar(
#         source_counts,
#         x="source",
#         y=y_labels,
#         orientation="h",
#         color_discrete_sequence=["#ff6347"] * len(source_counts),
#         template="plotly_white",
#     )
#     fig_source_counts.update_layout(
#         plot_bgcolor="rgba(0,0,0,0)",
#         xaxis=(dict(showgrid=False)),
#         width=500,
#         height=500
#     )
#     fig_source_counts.update_traces( marker_color= 'rgb(255, 99, 71)',marker_line_color='rgb(101, 39, 27)',
#                     marker_line_width=1.5, opacity=0.6
#                     )
#     st.plotly_chart(fig_source_counts, theme="streamlit", use_container_width=True)
# with tab2:
#     # Use the native Plotly theme.
#     st.markdown("<h3 style='text-align: center; color: black;'>Top " + str(tweepy_top[1])+" popular locations in Twitter</h3>", unsafe_allow_html=True)

#     twitter_location = df_selection['location'].value_counts().reset_index()
#     twitter_location.columns = ['Location', 'No of twitter']
#     twitter_location_top10 = twitter_location.head(10)

#     fig_twitter_location = plt.figure()#figsize=(15,10)
#     ax_twitter_location = fig_twitter_location.add_subplot(1,1,1)
#     squarify.plot(sizes=twitter_location_top10['No of twitter'],label=twitter_location_top10['Location'],ax=ax_twitter_location,pad=True,
#                 value=twitter_location_top10['No of twitter'])

#     st.pyplot(fig_twitter_location)

# st.markdown("""---""")


# #Histogram of Favorite, Retweet, Status and Followers from Twitter
# favorite_counts = df_selection["favorite"].value_counts().sort_values()[-(tweepy_top[1]):]
# retweet_counts = df_selection["retweet"].value_counts().sort_values()[-(tweepy_top[1]):]
# status_counts = df_selection["statuses"].value_counts().sort_values()[-(tweepy_top[1]):]
# follower_counts = df_selection["followers"].value_counts().sort_values()[-(tweepy_top[1]):]

# st.markdown("<h3 style='text-align: center; color: black;'>Histogram of Favorite, Retweet, Status and Followers from Twitter</h3>", unsafe_allow_html=True)
# fig, axes = plt.subplots(2, 2, figsize=(15, 10), sharey=True)

# plt.subplot(2,2,1)
# plt.title('Favorite')
# sns.distplot(favorite_counts, color='blue', kde = False, bins=20)

# plt.subplot(2,2,2)
# plt.title('Retweet')
# sns.distplot(retweet_counts, color='red', kde = False, bins=20)

# plt.subplot(2,2,3)
# plt.title('Status')
# sns.distplot(status_counts, color='green', kde = False, bins=20)

# plt.subplot(2,2,4)
# plt.title('Followers')
# sns.distplot(follower_counts, color='orange', kde = False, bins=20)

# st.pyplot(plt)
# st.markdown("""---""")


# #Hourly statistical probability density based on creation date
# tab1, tab2 = st.tabs(["Hourly statistical probability density based on creation date (default)", "Word Cloud about Twitter texts"])
# with tab1:
#     plt.figure(figsize=(10,6))
#     sns.distplot(df_selection['tweet_hour'], kde=True, color='blue')

#     st.pyplot(plt)
# with tab2: 
#     text = " ".join(i for i in df_selection.text)
#     wc = WordCloud(background_color='white').generate(text)
#     plt.figure(figsize=(10,6))
#     plt.imshow(wc, interpolation='bilinear')
#     plt.axis('off')
#     plt.tight_layout(pad = 0)

#     st.pyplot(plt)

# st.markdown("""---""")

# #---------------------------------------------------------------------------------------------------------------------------------------------------------------------
# st.markdown("<h2 style='text-align: center; color: red;'>Visualization based on user emotions</h2>", unsafe_allow_html=True)

# df_bitcoin = df[["clean_text"]]
# df_bitcoin.head(5)

# df_bitcoin["Polarity"] = df_bitcoin['clean_text'].apply(lambda x: TextBlob(x).sentiment[0])
# df_bitcoin["Subjectivity"] = df_bitcoin['clean_text'].apply(lambda x: TextBlob(x).sentiment[1])
# df_bitcoin.head(5)

# def getSentiment(score):
#   if score < 0:
#     return "Negative"
#   elif score == 0:
#     return "Neutral"
#   else:
#     return "Positive"

# df_bitcoin["Sentiment"] = df_bitcoin["Polarity"].apply(getSentiment)
# st.dataframe(df_bitcoin)

# tab1, tab2 = st.tabs(["The sentiment analysis scatter plot of bitcoin from Twitter (default)", "Sentiment Analysis Bar Plot of Bitcoin from Twitter"])
# with tab1:
#     fig_scatter = plt.figure()#figsize=(15,10)
#     # plt.figure(figsize=(10,6))
#     plt.scatter(df_bitcoin["Polarity"], df_bitcoin["Subjectivity"], color = "purple")
#     plt.title("The sentiment analysis scatter plot of bitcoin from Twitter")
#     plt.xlabel("Polarity")
#     plt.ylabel("Subjectivity")
#     st.pyplot(fig_scatter)
# with tab2:
#     bitcoin_counts = df_bitcoin["Sentiment"].value_counts()
#     fig_sentiment, ax_sentiment = plt.subplots(figsize=(10, 6))
#     ax_sentiment = bitcoin_counts.plot.bar(color='blue', edgecolor = 'black',linewidth = 1.25)
#     ax_sentiment.set_title("Sentiment Analysis Bar Plot of Bitcoin from Twitter")
#     ax_sentiment.set_xlabel("Sentiment")
#     ax_sentiment.set_ylabel("The number of Tweets")
#     st.pyplot( fig_sentiment)




