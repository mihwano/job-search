import pdb,sys,os, sqlite3,nltk, urllib,sets,requests,re,datetime
import pandas as pd
from bs4 import BeautifulSoup
from collections import Counter
import numpy as np
from lxml import html


def connection():
  """ connect to the database on disk """
  db = sqlite3.connect('ads_data.db')
  cur = db.cursor()
  return cur

def helper_1(bits):
  cts = 0
  for bit in bits:
    if cts == 0:
      holder = bit
    else:
      holder = holder + ',' +bit
    cts += 1
  url = 'http://api.geonames.org/search?q=%s&maxRows=5&country=&username=harlock78' %holder
  return url

def retrieve_basics():
  cur = connection()
  command = 'SELECT ID, NUM_REFERENCES,ABSTRACT_LEN,NUM_KEYWORDS,KEYWORD_1,KEYWORD_2,KEYWORD_3,KEYWORD_4,KEYWORD_5,KEYWORD_6,KEYWORD_7,KEYWORD_8,NUM_CITATIONS,ABSTRACT_TEXT,AUTHOR_1,AUTHOR_2,AUTHOR_3,LAST_AUTHOR from METADATA'
  cur.execute(command)
  id_nbrs = cur.fetchall()
  return id_nbrs

def retrieve_data(n):
  """ n is the number in country_1, country_2 etc..."""
  cur = connection()
  command = 'SELECT COUNTRY_%s from METADATA' %n
  cur.execute(command)    
  affiliations = cur.fetchall()
  return affiliations

def sort_countries():
  """ countries of origin have not been sorted properly. Trying a fix 
      Build a new dictionary key - values with institutes - country"""

  """pandas dataframe"""
  columns = ['ID','num_ref','abstract_len','num_keywords',
            'keyword_1','keyword_2','keyword_3','keyword_4','keyword_5','keyword_6','keyword_7',
            'keyword_8','num_citations','abstract_text','author_1','author_2','author_3','last_author',
            'affiliation_1','country_1','affiliation_2','country_2',
            'affiliation_3','country_3','affiliation_4','country_4','affiliation_5','country_5',
            'affiliation_6','country_6','affiliation_7','country_7','affiliation_8','country_8',
            ]
  ads_df = pd.DataFrame(columns=columns,index=range(108656))
  ads_df.fillna('N.A',inplace=True)

#  cur = connection()
#  affiliations = []
#  for i in xrange(8):
#    command = 'SELECT COUNTRY_%s from METADATA' %(i+1)
#    cur.execute(command)
#  affiliations.extend(cur.fetchall())

  #use geoname app to retrieve country code for a place or university
  #http://api.geonames.org/search?q=placename&maxRows=10&username=demo

  affiliations = retrieve_data(1)
  basic_data = retrieve_basics()

  country_list = []
  item_nbr = 1
  for item in affiliations:        # loop over country_1, country_2 etc...

    vec = [x for x in basic_data[item_nbr-1]]
    ads_df.loc[item_nbr-1][['ID','num_ref','abstract_len','num_keywords',
            'keyword_1','keyword_2','keyword_3','keyword_4','keyword_5','keyword_6','keyword_7',
            'keyword_8','num_citations','abstract_text','author_1','author_2','author_3','last_author']] = vec

    if item[0] == 'N.A':
      country_list.append((['N.A'],item_nbr))

    else:
      for institution in item:              # loop over the research institute
        L = institution.replace('(','').replace(')',',').split(';')   #if several institution, split it
        country_url = []
        for placename in L:
          placename = ''.join([i for i in placename if not i.isdigit()])
          bits = placename.split(',')
          if '' in bits:
            bits.remove('')
          for i in xrange(len(bits)):       #loop over elements of institute name to tyr finding one that works for geoname
            country = []
            url = helper_1(bits[i:])
            try:
              response = urllib.urlopen(url)
            except:
              continue
            html = response.read()
            soup = BeautifulSoup(html,'xml')
            if soup.find_all('totalResultsCount')[0].text.strip() == '0':
              continue
            else:
              for nbr in soup.find_all('countryName'):
                country.append(nbr.text.strip())
              country_tup = Counter(country)
              country_tup = country_tup.most_common()[0]
              country_url.append(country_tup[0])
              break

          # PROBLEM IF LAST ELEMENT OF INSTITUTE NAME HAS NUMBERS AND IS NOt RECOGNIZABLE
          # THEN country_url = []

          country_list.append(([x for x in set(country_url)],item_nbr))
#          country_list.append(set(country_url))
  
    #fill pandas dataframe
    if item[0] != 'N.A':
      for p in xrange(len(country_list[-1][0])):
        for idx in xrange(7):
          if ads_df.loc[item_nbr-1]['country_%s' %(idx+1)] == 'N.A':
            ads_df.loc[item_nbr-1]['country_%s' %(idx+1)] = country_list[-1][0][p]
            ads_df.loc[item_nbr-1]['affiliation_%s' %(idx+1)] = L[p]
            break
          else:
            continue

    item_nbr += 1
#    print country_list[-1]
    if item_nbr%50 == 0:
      print country_list[-1]
#      pdb.set_trace()
  pdb.set_trace()

  #list of world universities
  uni = pd.read_csv('university_list.csv')
  universities = {}
  for i in xrange(len(uni)):
    try:
      universities[uni.iloc[i]['NAME'].encode('utf-8')]=uni.iloc[i]['CC']
    except:
      continue

  uni_list = []
  for university in universities.keys():
    #tokenize university name and make it a set
    w_university = nltk.word_tokenize(university)
#    w_university =[x for x in w_university if x.isalpha()]
    s_university = set(w_university)
    uni_list.append((s_university,university))

  institutes = {}
  reste = []
  new_affiliations = []


  #for item in affiliations, if item is a recognized university, put in dictionary
  for item in affiliations:
    if 'N.A' in item[0]:
      continue
    #tokenize item name
    w_item = nltk.word_tokenize(item[0])
#    w_item =[x for x in w_item if x.isalpha()]
    s_item = set(w_item)

    code = 'none'
    for university in uni_list:
      if university[0].issubset(s_item) or s_item.issubset(university[0]):
        code = 'found'
        if university[1] not in institutes:
          institutes[university[1]] = universities[university[1]]
          break

    if code == 'none' and item not in new_affiliations:
      new_affiliations.append(item)

  #additional comb for institutions not found
  df_countries = pd.read_csv('country_list.dat')
  country_list = list(df_countries['name'])
  state_list = states.keys()
  state_list.extend(states.values())

  new_new_affiliations = []
  for item in new_affiliations:
    code = 'none'
    name = str(item[0].split(',')).replace('[','').replace(']','').replace('(','').replace(')','').replace('u','').replace("'",'')
    for country in country_list:
        if country in item[0] and name not in institutes:
          institutes[name] = country
          code = 'found'
          break
    if code == 'none':
      new_new_affiliations.append(item)

  for item in new_new_affiliations:
    code = 'none'
    name = str(item[0].split(',')).replace('[','').replace(']','').replace('(','').replace(')','').replace('u','').replace("'",'')
    for state in state_list:
      if state in item[0] and name not in institutes:
        institutes[name] = 'USA'
        code = 'found'
        break

    if code == 'none' and name not in institutes.keys():
      reste.append(item[0])

  return institutes, reste


#------------------------------------------------------------------------
dic,reste = sort_countries()
for item in reste:
  if 'sinica' in item.lower():
    dic[item] = 'China'
    reste.remove(item)
  elif 'cnrs' in item.lower():
    dic[item] = 'France'
    reste.remove(item)
  elif 'ukrain' in item.lower():
    dic[item] = 'Ukraine'
    reste.remove(item)
  elif 'moscow' in item.lower():
    dic[item] = 'Russia'
    reste.remove(item)
  elif 'russia' in item.lower():
    dic[item] = 'Russia'
    reste.remove(item)
  elif 'chinese' in item.lower():
    dic[item] = 'China'
    reste.remove(item)
  elif 'petersburgh' in item.lower():
    dic[item] = 'Russia'
    reste.remove(item)
  elif 'cardiff' in item.lower():
    dic[item] = 'UK'
    reste.remove(item)
  elif 'nakanoshima' in item.lower():
    dic[item] = 'Japan'
    reste.remove(item)
  elif 'glasgow' in item.lower():
    dic[item] = 'UK'
    reste.remove(item)
  elif 'yoshinodai' in item.lower():
    dic[item] = 'Japan'
    reste.remove(item)
  elif 'nobeyama' in item.lower():
    dic[item] = 'Japan'
    reste.remove(item)
  elif 'tokyo' in item.lower():
    dic[item] = 'Japan'
    reste.remove(item)
  elif 'liverpool' in item.lower():
    dic[item] = 'UK'
    reste.remove(item)
  elif 'edinburgh' in item.lower():
    dic[item] = 'UK'
    reste.remove(item)
  elif 'manchester' in item.lower():
    dic[item] = 'UK'
    reste.remove(item)
  elif 'cambridge' in item.lower():
    dic[item] = 'UK'
    reste.remove(item)
  elif 'brighton' in item.lower():
    dic[item] = 'UK'
    reste.remove(item)
  elif 'london' in item.lower():
    dic[item] = 'UK'
    reste.remove(item)

