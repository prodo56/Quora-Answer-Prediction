from gensim.models import Word2Vec as wv
from nltk.corpus import stopwords
import json
import pandas as pd
import numpy as np
import string
from sklearn.linear_model import LogisticRegression as LR
import sklearn.model_selection as modelselection


df = pd.DataFrame(columns=("question_text","context_topic_followers","context_topic_names","topics_followers","topics_name","question_key","__ans__","anonymous"))
vectorised_df = pd.DataFrame(columns=("question_text","context_topic_followers","context_topic_names","topics_followers","topics_name","question_key","__ans__","anonymous"))
punctuations = set(string.punctuation)
text_Data=[]

def cleanText(sentence):
    stopword_removal = " ".join([word.lower() for word in sentence.split() if word not in stopwords.words('english')])
    punctuation_removal = ''.join(char for char in stopword_removal if char not in punctuations)
    return punctuation_removal

with open("answered_data_10k.in") as f:
    N = int(f.readline())
    for i in xrange(30):
        topicFollowers = 0
        topicNames=[]
        record = json.loads(f.readline())
        data = []
        question=cleanText(record["question_text"])
        data.append(question.lower())
        text_Data.append(question.lower())
        print record
        if record["context_topic"] != "null" and record["context_topic"]:
            data.append(record["context_topic"]["followers"])
            name = cleanText(record["context_topic"]["name"])
            data.append(name.lower())
            text_Data.append(name.lower())
        else:
            data.append(0)
            data.append(None)
        if record["topics"]!="null":
            for topic in record["topics"]:
                topicFollowers = topicFollowers + topic["followers"]
                name = cleanText(topic["name"])
                topicNames.append(name.lower())
                text_Data.append(name.lower())
        data.append(topicFollowers)
        data.append(topicNames)
        data.append(str(record["question_key"]))
        data.append(record["__ans__"])
        data.append(record["anonymous"])
        df.loc[-1] = data  # adding a row
        df.index = df.index + 1


def vectoriseData(data):
    newData =[]
    questionVector = np.zeros(200,dtype=np.float64)
    count = 0
    for word in data["question_text"].split():
        if word in model.wv.vocab.keys():
            questionVector = np.add(questionVector,model[word])
            count+=1
    if count != 0:
        questionVector = questionVector/count
    newData.append(questionVector)
    newData.append(data["context_topic_followers"])
    contextTopicNameVector = np.zeros(200,dtype=np.float64)
    count = 0
    if data["context_topic_names"]:
        for word in data["context_topic_names"].split():
            if word in model.wv.vocab.keys():
                contextTopicNameVector = np.add(contextTopicNameVector, model[word])
                count += 1
    if count != 0:
        contextTopicNameVector = contextTopicNameVector / count
    newData.append(contextTopicNameVector)
    newData.append(data["topics_followers"])
    topics_nameVector = np.zeros(200, dtype=np.float64)
    count = 0
    if data["topics_name"]:
        for word in data["topics_name"]:
            for w in word.split():
                if w in model.wv.vocab.keys():
                    topics_nameVector = np.add(topics_nameVector, model[w])
                    count += 1
    if count!=0:
        topics_nameVector = topics_nameVector / count
    newData.append(topics_nameVector)
    newData.append(data["question_key"])
    newData.append(data["__ans__"])
    newData.append(data["anonymous"])
    return newData


print "starting word2vec training"
training_list_word2vec = []
for text in text_Data:
    training_list_word2vec.append(text.split())
model = wv(training_list_word2vec, min_count=1,size=200)
vocab = list(model.wv.vocab.keys())

print "word2vec training complete!"
#print df.dtypes
df.anonymous = df.anonymous.map({True:1,False:0})
df.__ans__ = df.__ans__.map({True:1,False:0})


for index, row in df.iterrows():
    print row,type(row),index
    vectorised_df.loc[-1] = vectoriseData(row)
    vectorised_df.index = vectorised_df.index + 1


print vectorised_df




#print df.head(5)



