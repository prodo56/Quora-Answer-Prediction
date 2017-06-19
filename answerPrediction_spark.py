from pyspark import SparkContext
import json
import string
import pandas as pd
from pyspark.sql import SQLContext,Row
from pyspark.sql.functions import udf
from gensim.models import Word2Vec as wv
from nltk.corpus import stopwords
from pyspark.sql.types import StringType
from pyspark.sql.functions import lit,col
from itertools import chain


punctuations = set(string.punctuation)
stopword = set(stopwords.words('english'))
def cleanText(sentence):
   # punctuations = V.value
    print sentence
    stopword_removal = " ".join([word.lower() for word in sentence.split() if word not in stopwords.words('english')])
   # punctuation_removal = ''.join(char for char in stopword_removal if char not in punctuations)
    #return stopword_removal

def getData(x):
    question = x["question_text"]
    question_key = x["question_key"]
    ans=x["__ans__"]
    anonymous = x["anonymous"]
    if x["context_topic"] != "null" and x["context_topic"]:
        context_follwers=x["context_topic"]["followers"]
        context_name = x["context_topic"]["name"]
    else:
        context_follwers = 0
        context_name = None
    topicNames=[]
    topicFollowers = 0
    if x["topics"] != "null":
        for topic in x["topics"]:
            topicFollowers = topicFollowers + topic["followers"]
            topicNames.append(topic["name"])
    return [question,context_follwers,context_name, topicFollowers, topicNames, question_key, ans, anonymous]

with open("answered_data_10k.in") as f:
    data = f.readlines()

N = int(data[0])
data = data[1:]

print N, len(data), data[0]

sc= SparkContext()
sqlContext = SQLContext(sc)
V = sc.broadcast(punctuations)
r = sc.parallelize(data)

r = r.map(lambda s:  s.strip()).map(json.loads).map(getData)
r = r.take(10)
df = sqlContext.createDataFrame(r,["question_text","context_topic_followers","context_topic_names","topics_followers","topics_name","question_key","__ans__","anonymous"])
df.show()
rdd = df.select("question_text").rdd
print rdd.take(2)
row = Row("cleaned_text")
k = rdd.map(lambda d: d["question_text"].lower()).map(lambda word: " ".join([str(w) for w in word.split() if not w in stopword])).map(lambda word: ''.join(char for char in word if char not in punctuations)).collect()
d = df.toPandas()
d['cleaned_question_text'] = k
d['cleaned_context_topic_names'] = df.select("context_topic_names").rdd.map(lambda d: d["context_topic_names"].lower() if d["context_topic_names"] else None).map(lambda word: " ".join([str(w) for w in word.split() if not w in stopword]) if word else None).map(lambda word: ''.join(char for char in word if char not in punctuations) if word else None).collect()
d['cleaned_topic_names'] = df.select("topics_name").rdd.map(lambda d: [w.lower() for w in d["topics_name"]] if d["topics_name"] else None).map(lambda word: [" ".join([str(w) for w in strings.split() if not w in stopword]) for strings in word] if word else None).map(lambda word: [''.join(char for char in strings if char not in punctuations) for strings in word] if word else None).collect()
d.anonymous = d.anonymous.map({True:1,False:0})
d.__ans__ = d.__ans__.map({True:1,False:0})
rdd = sqlContext.createDataFrame(d)
rdd.show()
