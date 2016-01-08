""" Tool to help in job search.
   1) Scrap list of job offers from indeed and reviews of the company
   2) for each job, scrap the reviews for the company from glassdoor
   3) store all info in a mongoDB database (noSQL)
   4) tools to explore the data(vizualization and histograms)
"""

# ADD a function to get salary as well #


import datetime
import requests
from lxml import html
import re, pdb

base_url = "http://www.indeed.com"

def build_url(url, *filters):
    """build the url for inded api, using parameters and their values, dfined by the user"""
    return url + ''.join(["&" + param + "=" + val for param,val in filters])

def build_search_url(keywords, location):
    """ use the options for the api: keyword_filter, location_filter,date_filter"""
    query = "/jobs?"
    keyword_filter = "q", keywords
    location_filter = "l", location
    date_filter = 'fromage', 'last' # only get the latest postings
    return build_url(base_url + query, keyword_filter, location_filter, date_filter)

def parse_xpath(xpath, type=unicode, unique=True):
    """helper function to avoid errors due to inconstensies in indeed return response"""
    if len(xpath)== 0:
        #print "No elements found by xpath."
        return ''
    if unique and len(xpath) > 1:
        print "xpath expected 1 element, but found:", str(xpath)

    if unique:
        return type(xpath[0])
    else:
        return [type(x) for x in xpath]

def parse_date(job_date):
    hours = re.match(r"(\d+) hours", job_date)
    days = re.match(r"(\d+) days", job_date)
    if hours:
        hours = int(hours.group(1))
    else:
        hours = 0
    if days:
        days = int(days.group(1))
    else:
        days = 0

    job_date_obj = datetime.datetime.today() - datetime.timedelta(days=days, hours=hours)
    return job_date_obj

def parse_job(job,exclude_kws):
    """ for each job ad found, parse title, url, location etc... into a json format suitable
        for the mongoDB database"""
    job_title = parse_xpath(job.xpath('.//h2[contains(@class,"jobtitle")]/a/@title'))
    job_url = parse_xpath(job.xpath('.//h2[contains(@class,"jobtitle")]/a/@href'))
    job_date = parse_date(parse_xpath(job.xpath('.//span[contains(@class,"date")]/text()')))
    text = job.xpath('.//span[contains(@class,"company")]/span[contains(@itemprop,"name")]//text()')
    company = parse_xpath([x.replace('  ','').replace('\n','') for x in text if x.replace('  ','').replace('\n','')!=''])
    location = parse_xpath(job.xpath('.//span[contains(@class,"location")]//text()'))
    review_url = parse_xpath(job.xpath('.//a[contains(@data-tn-element,"reviewStars")]/@href'))

    requirement_kw = 'no'
    if exclude_kws != None:
      job_ad_html = html.fromstring(requests.get(base_url + job_url).text)
      html_string = job_ad_html.xpath("string()")
      for kw in exclude_kws:
        if kw.lower() in html_string.lower():
          requirement_kw = 'yes'    

    return {'job_url':job_url, 'job_title':job_title, 'company':company,
            'location':location, 'review_url':review_url, 'job_date':job_date, 'excluded_kw':requirement_kw}

def get_jobs(keywords, location, jobs_db, max_pages=1,exclude_kws=None):
  """ Cast the root element of the HTML tree into the variable tree"""
  tree = html.fromstring(requests.get(build_search_url(keywords, location)).text)

  jobs = []
  for i in range(max_pages):
      """create a list of all the div tags containing the JobPosting word"""
      jobs_divs = tree.xpath('//div[contains(@itemtype,"JobPosting")]')
      for job in jobs_divs:
      #for each tag, cast the json dictionary containing the relevant data into a mongoDB database
        p_j = parse_job(job,exclude_kws)
        jobs_db.insert(p_j)
        jobs.append(parse_job(job,exclude_kws)) #this is what is shown to user when she requests results

      next_page = tree.xpath('//div[contains(@class,"pagination")]//span[contains(text(),"Next")]/../../@href')
      if len(next_page) == 0:
        print "Last page: ", i + 1
        break
      else: 
        next_page = base_url + next_page[0]

      tree = html.fromstring(requests.get(next_page).text)

  return jobs   #also the list of jobs can be used to obtain reviews for companies


def get_stars(span_style):
    width_val = re.match(r"width:(\d+\.\d+)", span_style)
    if not width_val:
        print "No width style found"
        return 0
    return int(round(float(width_val.group(1))/17.2))

def review_rating(review):
    overall_rating = parse_xpath(review.xpath('.//span[contains(@class,"rating")]//@title'))
    rating_categories = parse_xpath(review.xpath('.//table[contains(@class,"ratings_expanded")]//text()'), unicode, False)
    expanded_ratings_styles = parse_xpath(review.xpath('.//table[contains(@class,"ratings_expanded")]//span[contains(@class,"rating")]/@style'), unicode,False)
    stars = {key:val for (key,val) in zip(rating_categories, [get_stars(s) for s in expanded_ratings_styles])}
    return overall_rating, stars

def parse_review(review):
    company_name = parse_xpath(review.xpath('//div[contains(@id,"cmp-name-and-rating")]/h2[contains(@itemprop,"name")]//text()'))
    overall_rating, stars = review_rating(review)
    job_title = parse_xpath(review.xpath('.//span[contains(@class,"cmp-reviewer-job-title")]/span[contains(@class,"cmp-reviewer")]/text()'))
    employment_status = parse_xpath(review.xpath('.//span[contains(@class,"reviewer_job_title")]/text()'))
    location = parse_xpath(review.xpath('.//span[contains(@class,"location")]/text()'))
    date = parse_xpath(review.xpath('.//span[contains(@class,"cmp-review-date-created")]/text()'))
    date = datetime.datetime.strptime(date, '%B %d, %Y')
    review_title = parse_xpath(review.xpath('.//div[contains(@class,"cmp-review-title")]/text()'))
    review_text = unicode(review.xpath('string(.//div[contains(@class,"content")]/div[contains(@class,"description")])'))
    review_pros = unicode(review.xpath('string(.//div[contains(@class,"content")]/div[@class="review_pros"])'))
    review_cons = unicode(review.xpath('string(.//div[contains(@class,"content")]/div[@class="review_cons"])'))

    return {'company':company_name,'rating': overall_rating, 'stars': stars, 'job_title':job_title,
            'employment_status':employment_status, 'location':location,
            'date':date, 'review_title':review_title, 'review_text':review_text,
            'review_pros':review_pros, 'review_cons':review_cons}

def get_all_reviews(review_url, reviews_db, max_pages=100):
    """For each url containing a review, parse the html"""
    tree = html.fromstring(requests.get(base_url + review_url).text)
    for i in range(max_pages):
        reviews_divs = tree.xpath('//div[contains(@class,"cmp-review-container")]')

        for review in reviews_divs:
            p_r = parse_review(review)
            reviews_db.insert(p_r)

        next_page = tree.xpath('//div[contains(@id,"pagination")]//span[contains(text(),"Next")]/../@href')
        if len(next_page) == 0:
            print "No more pages", i
            break
        else: next_page = base_url + next_page[0]
        tree = html.fromstring(requests.get(next_page).text)
    return

def get_all_company_reviews(jobs_list, reviews_db, max_pages=100):
  """take a list of jobs (obtained from keyword search) and obtain url for reviews of the companies"""
  visited_urls = []
  for job in jobs_list:
    url = job["review_url"]
    if not url or url in visited_urls: continue
    get_all_reviews(url, reviews_db, max_pages)
    visited_urls.append(url)
  return