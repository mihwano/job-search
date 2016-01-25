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
  hist = np.histogram(rating,bins=5,density=True)
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

