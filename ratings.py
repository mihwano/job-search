""" Objective: examine the differences in culture between Seattle and 
    San Francisco, and Portland when it comes to companies advertising for
    data scientists. Use of Latent Dirichlet Allocation for identifying topics"""

from pymongo import MongoClient
import pandas as pd
import numpy as np
import pdb, sys, os
import matplotlib.pyplot as plt
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from wordcloud import WordCloud, STOPWORDS
from collections import Counter
from IPython.display import Image
from IPython import display
import heapq
from nltk.stem.porter import PorterStemmer
stemmer = PorterStemmer()
from nltk.stem.snowball import SnowballStemmer
import gensim
from gensim import corpora, models
from nltk.tokenize import RegexpTokenizer
tokenizer = RegexpTokenizer(r'\w+')

# connect to the mongo database where indeed reviews and ratings are stored
client = MongoClient()
indeed_db = client.indeed
indeed_reviews = indeed_db.reviews

def gather_reviews(location):
  """create a dictionary for the location and store the reviews with
     the key-value being companyname-(rating,review)"""
  reviews = {}
  for review in indeed_reviews.find():
    if (location.lower() in review['location'].lower()) and (review['company'] not in reviews.keys()):
      reviews[review['company']] = [[review['rating'],review['review_text']]]
    elif (location.lower() in review['location'].lower()) and (review['company'] in reviews.keys()):
      reviews[review['company']].append([review['rating'],review['review_text']])
  return reviews

def gather_company_reviews(company):
  """gather the reviews for a given company, regardless of location"""
  reviews = {}
  for review in indeed_reviews.find():
    if (company.lower() in review['company'].lower()) and (review['company'] not in reviews.keys()):
      reviews[review['company']] = [[review['rating'],review['review_text']]]
    elif (company.lower() in review['company'].lower()) and (review['company'] in reviews.keys()):
      reviews[review['company']].append([review['rating'],review['review_text']])
  return reviews


def average_rating(location):
  """ average star rating, standard deviation, and histogram for the location"""
  rating = []
  for item in location:
    if location[item]:
      for value in location[item]:
        if value[0]:  
          rating.append(float(value[0]))
  avg = np.mean(rating)
  std = np.std(rating)
  hist = np.histogram(rating,bins=5)
  return avg,std,hist

def define_stopwords():
  """all the stop words to remove from the corpus text"""
  stop = stopwords.words('english')
  stop += ['.',',','(',')',"'",'"',':','','...','-','``',';',";'",'&','#']
  stop += ["'s","n't"]
  stop = set(stop)   #faster to search a set than a list
  return stop

def create_corpus(location,stop):
  """get all reviews to create a word corpus for a given location,
     stopwords removed. Here location is the dictionary previously created with gather reviews"""
  corpus = Counter()
  for review in location.keys():
    if location[review]:
      for text in location[review]:
        words = text[1].split()
        for word in words:
          if word.lower() not in stop:
            for char in ['.',',','(',')',"'",'"',':','','...','-','``',';',";'",'&','#']:
              word = word.replace(char,'')
            corpus[word.lower()] += 1
  return corpus

def corpus_text(corpus):
  """whole corpus as one string to be ingested by WordCloud"""
  text = ''
  for item in corpus:
    for i in xrange(int(corpus[item])):
      text += item + ' '
  return text

def show_wordcloud(location_corpus,cut):
  """for a given location, show the wordcloud"""
  top_cut = heapq.nlargest(cut,location_corpus.items(),key=lambda x:x[1])
  for item in top_cut:
    if item[0] in location_corpus.keys():
      location_corpus.pop(item[0])
  location_text = corpus_text(location_corpus)
  location_wordcloud = WordCloud(stopwords=STOPWORDS).generate(location_text)
  plt.imshow(location_wordcloud)
  plt.axis('off')
  plt.show()
  return

def show_topics_wordcloud(topics):
  """for extracted topics, show the wordcloud"""
  
  # ==> to do: transform topics from ldamodel.print_topics() into a single string with correct
  #            number of word frequency
  # then do the wordcloud

  return

def document_for_LDA(corpus,stop):
  """from the dictionary of reviews, create a list of text documents"""
  texts = []   # this list will contain all tokens (words) extracted from the reviews
  for item in corpus:
    # clean and tokenize document string
    for document in corpus[item]:
      raw = document[1].lower()
      tokens = tokenizer.tokenize(raw)
      #remove stop words
      stopped_tokens = [token for token in tokens if not token in stop]
      #stem tokens
      stemmed_tokens = [stemmer.stem(token) for token in stopped_tokens]
      #add tokens to list
      texts.append(stemmed_tokens)
  #assign each term in text to a unique ID (to build the vector of words later)
  dictionary = corpora.Dictionary(texts)
  # convert tokenized documents into a document-term matrix
  text_matrix = [dictionary.doc2bow(text) for text in texts]
  return text_matrix, dictionary


######################################################################################
######################################################################################

stop = define_stopwords()

#dictionary of reviews for a specific company
github = gather_company_reviews('GitHub')
salesforce = gather_company_reviews('Salesforce')

text_matrix_github,dictionary_github = document_for_LDA(github,stop)
text_matrix_salesforce,dictionary_salesforce = document_for_LDA(salesforce,stop)

# generate LDA model
ldamodel_github = gensim.models.ldamodel.LdaModel(text_matrix_github, num_topics=2, id2word = dictionary_github, passes=5)
ldamodel_salesforce = gensim.models.ldamodel.LdaModel(text_matrix_salesforce, num_topics=2, id2word = dictionary_salesforce, passes=5)

sys.exit()

github_corpus = create_corpus(github,stop)
top_50_github = heapq.nlargest(50,github_corpus.items(),key=lambda x:x[1])
show_wordcloud(github_corpus,0)

sys.exit()

# dictionary of reviews for each location
SanFran = gather_reviews('San Francisco')
Seattle = gather_reviews('Seattle')
Portland = gather_reviews('Portland')
CA = gather_reviews(', CA')
WA = gather_reviews(', WA')
OR = gather_reviews(', OR')

#corpus for each location

text_matrix_SF,dictionary_SF = document_for_LDA(SanFran,stop)
ldamodel_SF = gensim.models.ldamodel.LdaModel(text_matrix_SF, num_topics=2, id2word = dictionary_SF, passes=4)

text_matrix_S,dictionary_S = document_for_LDA(Seattle,stop)
ldamodel_S = gensim.models.ldamodel.LdaModel(text_matrix_S, num_topics=2, id2word = dictionary_S, passes=4)


sys.exit()

SanFran_corpus = create_corpus(SanFran,stop)
Seattle_corpus = create_corpus(Seattle,stop)
Portland_corpus = create_corpus(Portland,stop)
CA_corpus = create_corpus(CA,stop)
WA_corpus = create_corpus(WA,stop)
OR_corpus = create_corpus(OR,stop)

#display wordcloud
top_50_SanFran = heapq.nlargest(50,SanFran_corpus.items(),key=lambda x:x[1])
cut = 1   #top words are similar (work...), try to get differences for less frequent words
show_wordcloud(SanFran_corpus,cut)

top_50_Seattle = heapq.nlargest(50,Seattle_corpus.items(),key=lambda x:x[1])
show_wordcloud(Seattle_corpus,cut)

top_50_Portland = heapq.nlargest(50,Portland_corpus.items(),key=lambda x:x[1])
show_wordcloud(Portland_corpus,cut)

sys.exit()

SanFran_avg, SanFran_std, SanFran_hist = average_rating(SanFran)
SanFran_avg = round(SanFran_avg,1)
plt.bar(SanFran_hist[1][:-1],SanFran_hist[0])
plt.title('San Francisco average rating: %s' %SanFran_avg)
plt.show() 

Seattle_avg, Seattle_std, Seattle_hist = average_rating(Seattle)
Seattle_avg = round(Seattle_avg,1)
plt.bar(Seattle_hist[1][:-1],Seattle_hist[0])
plt.title('Seattle average rating: %s' %Seattle_avg)
plt.show() 

Portland_avg, Portland_std, Portland_hist = average_rating(Portland)
Portland_avg = round(Portland_avg,1)
plt.bar(Portland_hist[1][:-1],Portland_hist[0])
plt.title('Portland average rating: %s' %Portland_avg)
plt.show() 

CA_avg, CA_std, CA_hist = average_rating(CA)
CA_avg = round(CA_avg,1)
plt.bar(CA_hist[1][:-1],CA_hist[0])
plt.title('CAlifornia average rating: %s' %CA_avg)
plt.show() 

WA_avg, WA_std, WA_hist = average_rating(WA)
WA_avg = round(WA_avg,1)
plt.bar(WA_hist[1][:-1],WA_hist[0])
plt.title('Washington average rating: %s' %WA_avg)
plt.show() 

OR_avg, OR_std, OR_hist = average_rating(OR)
OR_avg = round(OR_avg,1)
plt.bar(OR_hist[1][:-1],OR_hist[0])
plt.title('Oregon average rating: %s' %OR_avg)
plt.show() 