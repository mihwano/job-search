"""
Scripts to parse the ads database and populate sqlite3 database

journals= ApJ, A&A,Icarus, AJ, Science, Nature, MNRAS,
database fields: nbr of authors, nbr of citations,publication year,journal name,
                 affiliations,name of first author, name of last author, name of
                 authors 2,3,4,5,6,7,8.
                 how many references,
"""

import os,sys,pdb,requests,re,nltk,sqlite3,datetime,re
from bs4 import BeautifulSoup
import pandas as pd
from collections import Counter
from lxml import html

"http://labs.adsabs.harvard.edu/adsabs/search/?q=year%3A2005&month_from=&year_from=&month_to=&year_to=&db_f=astronomy&nr=&bigquery=&prop_f=-%28%22notrefereed%22%29&bib_f=%28%22ApJ%22+OR+%22A%26A%22+OR+%22MNRAS%22+OR+%22ApJL%22+OR+%22AJ%22+OR+%22Ap%26SS%22+OR+%22Icar%22+OR+%22Natur%22+OR+%22ApJS%22+OR+%22PASJ%22+OR+%22Sci%22%29"

"""
Build a database of articles
Start at year 1990 (25 files)
"""

def build_url(year,page):
  """url for search ADS 2.0 for all articles in a given year"""
  return 'http://labs.adsabs.harvard.edu/adsabs/search/?q=year%3A'+str(year)+'&month_from=&year_from=&month_to=&year_to=&db_f=astronomy&nr=500&bigquery=&prop_f=-("notrefereed")&bib_f=("ApJ"+OR+"A%26A"+OR+"MNRAS"+OR+"ApJL"+OR+"AJ"+OR+"Ap%26SS"+OR+"Icar"+OR+"Natur"+OR+"ApJS"+OR+"PASJ"+OR+"Sci")&page='+str(page)

def count_pages(year,url):
  """how many pages were returned"""
  r = requests.get(url)
  html = r.text
  soup = BeautifulSoup(html)
  page_nbr_paragraph = soup.findAll('span',{'class':'help-inline hidden-phone results_count'})
  text1 = page_nbr_paragraph[0].text
  total_count = int((text1.split('of')[1]).split('\n')[0])
  return (total_count/500)+1

def get_articles_list(tree):
  """retrieve list of articles in the page"""
  return tree.xpath('.//div[contains(@class,"span3 bibcode")]/a/@href')

def get_metadata(article):
  """ get all info from the article and put in mongodb"""
  tree = html.fromstring(requests.get(article).text)
  title = tree.xpath('.//div[contains(@class,"span12 abstractTitle")]')[0].text
  authors = tree.xpath('.//div[contains(@class,"span12 abstractAuthors")]/span[contains(@class,"author")]/span[contains(@class,"authorName")]')
  authors = [x.text for x in authors]
  affiliations = tree.xpath('.//div[contains(@class,"span12 abstractAuthors")]/span[contains(@class,"author")]/span[contains(@class,"authorAffiliation strHidden")]')
  affiliations = [x.text.replace('\n','').replace('\t','') for x in affiliations]
  affiliations = list(set(affiliations))
  pub_date = tree.xpath('.//div[contains(@class,"abstractPubdate")]/span[contains(@class,"pubdate")]')[0].text
  pub_month = pub_date[-8:-5]
  pub_year = pub_date[-4:]
  journal = tree.xpath('.//div[contains(@class,"span12")]/div[contains(@class,"abstractJournal")]')
  journal = journal[0].text.replace('\n','').replace('\t','').split(',')[0]
  ref_tab = tree.xpath('//ul[contains(@class,"nav nav-tabs")]/li/a')
  for item in ref_tab:
    if "References" in item.text:
      n_ref = int(item.text.split()[1].replace('(','').replace(')',''))
    elif "Citations" in item.text:
      n_cit = int(item.text.split()[1].replace('(','').replace(')',''))

  #to do next   can't find a way to get all in same page
  citations_list = get_all_citations()
  references_list = get_all_references()
  ###

  #may be issue here when several types o keywords
  keywords = tree.xpath('.//div[contains(@class,"keywordGroup")]/span[contains(@class,"keywordKey")]')
  keywords = [x.text for x in keywords]
  ###
  
  return

def fill_database(year):
  """load articles in mongodb"""
  url = build_url(year,1)
  tree=html.fromstring(requests.get(url).text)
  n_pages = count_pages(year,url)
  
  liste = get_articles_list(tree)
  for item in liste:
    get_metadata('http://labs.adsabs.harvard.edu'+item)
  
  for i in range(2,6):
    url = build_url(year,i)
    tree = html.fromstring(requests.get(url).text)    

  return n_pages

def retrieve_article_data(article,dic,index,adjacency_articles, adjacency_authors):
  """
  extract metadata from the abstract page of article
  How many references in article, affiliations, country, nbr of words in abstract,
  keywords, nbr of keywords,name of first, second and last author, link to references,
  max citations of reference, avg citations of references, std of citations of references,
  ID list of references,nbr of pages

  Note: idea about which graph or network the research is inserted into (page rank system)

  Put each article in basic database to a dictionary with a key ID? Identify the key in references?
  """
  url = article
  try:
    r = requests.get(url)
    html = r.text
    soup = BeautifulSoup(html)
  except:
    return adjacency_articles, adjacency_authors
  # of references
  references_text = soup.findAll('ul',{'class':'nav nav-tabs'})
  if 'class="disabled"><a> References' in str(references_text):
    return adjacency_articles, adjacency_authors
  num_references = re.search(r'References..(\d+).<',str(references_text).replace('\t','').replace('\n',''),re.DOTALL)
  try:
    num_references = num_references.group(1)
  except:
    num_references = 0
  #number of citations
  citations = re.search(r'Citations..(\d+)?',str(references_text).replace('\t','').replace('\n',''),re.DOTALL)
  try:
    citations = int(citations.group(1))
  except:
    citations = 0

  #affiliations and countries of origin
  affiliations_text = soup.findAll('span',{'class':'authorAffiliation strHidden'})
  affiliations_list = []
  country_list = []
  for item in affiliations_text:
    affiliations = re.search(r'>(.+)?<',str(item).replace('\t','').replace('\n',''),re.DOTALL)
    affiliations = affiliations.group(1)
    if affiliations not in affiliations_list:
      affiliations_list.append(affiliations)
    country = affiliations.replace(')','').split(',')[-1]
    if country not in country_list:
      country_list.append(country)

  #authors list
  author_list = []
  authors_text = soup.findAll('span',{'class':'authorName'})

  for item in authors_text:
    author = re.search(r'>(.+)?<',str(item).replace('\t','').replace('\n',''),re.DOTALL)
    author = author.group(1)
    author_list.append(author)

  #initialize adjacency list for authors and articles
  node_article = index
  adjacency_articles[node_article] = []

  for node_author in author_list: 
    adjacency_authors[node_author] = []

  #of words in the abstract
  abstract_text = soup.findAll('span',{'class':'abstract'})
  abstract_text = str(abstract_text).replace('\t','').replace('\n','')
  abstract_text = abstract_text[24:-18]
#  abstract_text = abstract_text.encode('utf-8')
  tokenizer = nltk.tokenize.RegexpTokenizer(r'\w+')
  tokens = tokenizer.tokenize(abstract_text)
  abstract_len = len(tokens)
  try:
    abstract_text = abstract_text.decode('utf-8')
  except:
    abstract_text = 'N.A'
#  b = bitarray.bitarray()
#  b.fromstring(abstract_text)

  #keywords
  try:
    keywords_text = soup.findAll('span',{'class':'keywordKey'})
    keywords = re.findall(r'keywordKey">(.+?)?</span',str(keywords_text).replace('\t','').replace('\n',''),re.DOTALL)
  except:
    keywords = []
  num_keywords = len(keywords)

  #index list of references (if they belong to the astronomy dictionary (some will not)
  link_ref =  re.search(r'href..(.+)?">.References',str(references_text).replace('\t','').replace('\n',''),re.DOTALL)
  try:
    link_ref = 'http://labs.adsabs.harvard.edu' + link_ref.group(1)
    r2 = requests.get(link_ref)
    html2 = r2.text
    soup2 = BeautifulSoup(html2)
  except:
    return adjacency_articles, adjacency_authors

  #references in abstract (for adjacency list of articles and authors)
  list_index = []
  list_authors_cited = []
  list_metadata = soup2.findAll('div',{'class':'row-fluid searchresult'})
  for item in list_metadata:
    # article title /weblink
    try:
      title_text = item.findAll('div',{'class':'span12 title'})
      title = re.search(r'/">(.+)?</a',str(title_text),re.DOTALL)
      title = title.group(1)

      weblink = re.search(r'/">(.+)?</a',str(title_text),re.DOTALL)
      link = re.search(r'href="(.+)?/">',str(title_text),re.DOTALL)
      link = 'http://labs.adsabs.harvard.edu' + link.group(1)
    except:
      title = 'unknown'

    #first author
    author_text = item.findAll('div',{'class':'span12 author'})
    author_text = str(author_text).replace('\t','').replace('\n','')
    if '<em>' in author_text:
      coauthors = re.search(r'<em>and(.+)?coauthors',author_text,re.DOTALL)
      coauthors = int(coauthors.group(1))
      authors = re.search(r'">(.+)?<em',author_text,re.DOTALL)
      authors = (authors.group(1)).split(';')
    else:
      coauthors = 0
      try:
        authors = re.search(r'">(.+)?</div',author_text,re.DOTALL)
        authors = (authors.group(1)).split(';')
      except:
        authors = ['unknown']
    for auth in authors:
      auth = auth.lstrip().rstrip()
      if auth not in list_authors_cited:
        list_authors_cited.append(auth)

    # last author from weblink
    try:
      r3 = requests.get(link)
      html3 = r3.text
      soup3 = BeautifulSoup(html3)
      last_author_text = soup3.findAll('span',{'class':'authorName'})
      last_author_text = str(last_author_text).replace('\t','').replace('\n','')
      full_authors_list = re.findall(r'">(.+?)?</span',last_author_text,re.DOTALL)
      last_author = full_authors_list[-1]
    except:
      last_author = 'N.A.'
    #publication date
    try:
      pubdate_text = item.findAll('div',{'class':'span12 pubdate'})
      pubdate = re.search(r'Published in (.+)?</em',str(pubdate_text).replace('\t','').replace('\n',''),re.DOTALL)
      pub_month = pubdate.group(1).split(' ')[0]
      pub_year = int(pubdate.group(1).split(' ')[1])
    except:
      pub_year = 'unknown'

    if (title,str(pub_year)) in dic.keys():
      index = dic[(title,str(pub_year))]
      list_index.append(index)

  #adds to author and article adjacency list
  adjacency_articles[node_article].extend(list_index)
  for node_auth in author_list:
    adjacency_authors[node_auth].extend(list_authors_cited)

  #fill sqlite3 database
  # a) affiliations
  if len(affiliations_list) < 8:
    for i in xrange(len(affiliations_list)):
      exec 'affil_%s = affiliations_list[%s]' %(i+1,i)
    for i in xrange(8-len(affiliations_list)):
      exec 'affil_%s = "N.A"' %(i+len(affiliations_list)+1)
  else:
    for i in range(8):
      exec 'affil_%s = affiliations_list[%s]' %(i+1,i)
  # b) countries
  if len(country_list)<8:
    for i in xrange(len(country_list)):
      exec 'country_%s = affiliations_list[%s]' %(i+1,i)
    for i in xrange(8-len(country_list)):
      exec 'country_%s = "N.A"' %(i+len(country_list)+1)
  else:
    for i in range(8):
      exec 'country_%s = affiliations_list[%s]' %(i+1,i)
  # c) authors
  if len(author_list) < 4 and len(author_list)>0:
    for i in xrange(len(author_list)):
      exec 'author_%s = author_list[%s]' %(i+1,i)
    author_last = author_list[-1]
    for i in xrange(3-len(author_list)):
      exec 'author_%s = "N.A"' %(i+len(author_list)+1)
  elif len(author_list)>3:
    for i in range(3):
      exec 'author_%s = author_list[%s]' %(i+1,i)
    if len(author_list) > 0:
      author_last = author_list[-1]
    else:
      author_last = 'N.A.'
  elif len(author_list) == 0:
    author_1 = 'N.A'
    author_2 = 'N.A'
    author_3 = 'N.A'
    author_last = 'N.A'

  # d) keywords
  if len(keywords) < 8:
    for i in xrange(len(keywords)):
      exec 'keyword_%s = keywords[%s]' %(i+1,i)
    for i in xrange(8-len(keywords)):
      exec 'keyword_%s = "N.A"' %(i+len(keywords)+1)
  else:
    for i in range(8):
      exec 'keyword_%s = keywords[%s]' %(i+1,i)

  conn = sqlite3.connect('ads_data.db')
  try:
    conn.execute("INSERT INTO METADATA VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (node_article,
                               int(num_references),affil_1,country_1,affil_2,country_2,affil_3,country_3,affil_4,country_4,affil_5,country_5,
                              affil_6,country_6,affil_7,country_7,affil_8,country_8,author_1,author_2,author_3,last_author,
                              abstract_len,num_keywords,keyword_1,keyword_2,keyword_3,keyword_4,keyword_5,keyword_6,keyword_7,
                              keyword_8,abstract_text,citations));
  except:
#    abstract_text = 'not read'
#    conn.execute("INSERT INTO METADATA VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (index,
#                               int(num_references),affil_1,country_1,affil_2,country_2,affil_3,country_3,affil_4,country_4,affil_5,country_5,
#                              affil_6,country_6,affil_7,country_7,affil_8,country_8,author_1,author_2,author_3,last_author,
#                              abstract_len,num_keywords,keyword_1,keyword_2,keyword_3,keyword_4,keyword_5,keyword_6,keyword_7,
#                              keyword_8,abstract_text,citations));
    print 'error - ignoring'
  conn.commit()
  conn.close()

  #note at the very end, do for each row of adjacency_authors: l = Counter(adjacency_authors[key])
  print 'article %s entered' %node_article
  return adjacency_articles, adjacency_authors


def retrieve(year,index,data):
  """
  scrap data from a given year
  """
  page = 1
  page_max = 100
  while page <= page_max:
    url = 'http://labs.adsabs.harvard.edu/adsabs/search/?q=year%3A'+ '%s' %year + '&month_from=&year_from=&month_to=&year_to=&db_f=astronomy&nr=2000&bigquery=&prop_f=-%28%22notrefereed%22%29&bib_f=%28%22ApJ%22+OR+%22A%26A%22+OR+%22MNRAS%22+OR+%22ApJL%22+OR+%22AJ%22+OR+%22Ap%26SS%22+OR+%22Icar%22+OR+%22Natur%22+OR+%22ApJS%22+OR+%22PASJ%22+OR+%22Sci%22%29&page='+'%s' %page
    r = requests.get(url)
    html = r.text
    soup = BeautifulSoup(html)

    # Determine page_max from total number of articles
    if page == 1:
      page_nbr_paragraph = soup.findAll('span',{'class':'help-inline hidden-phone results_count'})
      text1 = page_nbr_paragraph[0].text
      total_count = int((text1.split('of')[1]).split('\n')[0])
      page_max = (total_count/2000)+1

    # Retrieve first metadata layer for each article (all fit in one file
    list_metadata = soup.findAll('div',{'class':'row-fluid searchresult'})
    for item in list_metadata:
      # article title
      try:
        title_text = item.findAll('div',{'class':'span12 title'})
        title = re.search(r'/">(.+)?</a',str(title_text),re.DOTALL)
        title = title.group(1)
      except:
        title = 'unknown'

      #authors list
      author_text = item.findAll('div',{'class':'span12 author'})
      author_text = str(author_text).replace('\t','').replace('\n','')
      if '<em>' in author_text:
        coauthors = re.search(r'<em>and(.+)?coauthors',author_text,re.DOTALL)
        coauthors = int(coauthors.group(1))
        authors = re.search(r'">(.+)?<em',author_text,re.DOTALL)
        authors = (authors.group(1)).replace(' ','').split(';')
      else:
        coauthors = 0
        try:
          authors = re.search(r'">(.+)?</div',author_text,re.DOTALL)
          authors = (authors.group(1)).replace(' ','').split(';')
        except:
          authors = ['unknown']
      first_author = authors[0]
      total_authors_count = len(authors) + coauthors

      #publication date
      try:
        pubdate_text = item.findAll('div',{'class':'span12 pubdate'})
        pubdate = re.search(r'Published in (.+)?</em',str(pubdate_text).replace('\t','').replace('\n',''),re.DOTALL)
        pub_month = pubdate.group(1).split(' ')[0]
        pub_year = int(pubdate.group(1).split(' ')[1])
      except:
        pub_month = 'unknown'; pub_year = year
      #number of citations
      try:
        citations_text = item.findAll('div',{'class':'span2 citation_count'})
        citations = re.search(r'Cited by (\d+)?',str(citations_text).replace('\t','').replace('\n',''),re.DOTALL)
        citations = int(citations.group(1))
      except:
        citations = 0
      #Journals
      try:
        journal_text = item.findAll('div',{'class':'span3 bibcode'})
        journal = re.search(r'%s(.+)?">' %year,str(journal_text).replace('\t','').replace('\n',''),re.DOTALL)
        journal = journal.group(1).split('...')[0] 
      except:
        journal = 'unknown'
      #Link to abstract
      try:
        link_text = item.findAll('div',{'class':'span12 title'})
        link = re.search(r'href="(.+)?/">',str(title_text),re.DOTALL)
        link = 'http://labs.adsabs.harvard.edu' + link.group(1)
      except:
        link = 'unkown'
      #enter metadata into pandas
      data.loc[index] = ['%s' %title,'%s' %first_author,'%s' %total_authors_count,'%s' %pub_month,'%s' %pub_year,'%s' %citations,'%s' %journal,'%s' %link]
      index += 1

    page += 1
  return data,index

def retrieve_index():
  dic = {}
  f = open('index.csv','r')
  for item in f.readlines():
    dic[(item.split(',')[1],item.split(',')[-1].replace('\n',''))]=item.split(',')[0]
  f.close()
  return dic

#################################################################################

#################################################################################


# List of journals considered
#PUBLICATIONS = ['ApJS','A&amp;A Rev','ApJ','PASJ','MNRAS','A&amp;A','AJ','Icar','Natur','Sci']

#basic database
#index = 0
#columns = ['title','first_author','number_of_authors','pubmonth','pubyear','citations_nbr','journal','link']
#data = pd.DataFrame(columns = columns)

#for year in range(1990,2016):
#  data, index = retrieve(str(year),index,data)

#data.to_csv('1990-2015_data.csv')

# Build sqlite3 database
#conn = sqlite3.connect('ads_data.db')
#conn.execute('''CREATE TABLE METADATA
#       (ID INT PRIMARY KEY, NUM_REFERENCES INT, AFFILIATION_1 CHAR, COUNTRY_1 CHAR,
#       AFFILIATION_2 CHAR, COUNTRY_2 CHAR, AFFILIATION_3 CHAR, COUNTRY_3 CHAR,
#       AFFILIATION_4 CHAR, COUNTRY_4 CHAR, AFFILIATION_5 CHAR, COUNTRY_5 CHAR,
#       AFFILIATION_6 CHAR, COUNTRY_6 CHAR, AFFILIATION_7 CHAR, COUNTRY_7 CHAR,
#       AFFILIATION_8 CHAR, COUNTRY_8 CHAR, AUTHOR_1 CHAR, AUTHOR_2 CHAR, AUTHOR_3 CHAR,
#       LAST_AUTHOR CHAR, ABSTRACT_LEN INT, NUM_KEYWORDS INT, KEYWORD_1 CHAR,KEYWORD_2 CHAR,
#       KEYWORD_3 CHAR,KEYWORD_4 CHAR,KEYWORD_5 CHAR,KEYWORD_6 CHAR,KEYWORD_7 CHAR,KEYWORD_8 CHAR, 
#       ABSTRACT_TEXT TEXT, NUM_CITATIONS INT);''')
#conn.close()
#sys.exit()

#more metadata from the abstract (build graph of articles and authors)
#dic = retrieve_index()
#df = pd.read_csv('1990-2015_data.csv')
#initialize adjacency list for authors and articles
#adjacency_articles = {}
#adjacency_authors = {}
#all_index = [] #range(144793)
#all_index = []
#for index in xrange(144794,155808):           #144794 => 155809 bug
#  if index not in all_index:
#    all_index.append(index)
#    article = df.loc[index]['link']
#    adjacency_articles, adjacency_authors = retrieve_article_data(article, dic, index, adjacency_articles, adjacency_authors)