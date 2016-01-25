import os
import re
from datetime import datetime
from pymongo import MongoClient
import indeed, glassdoor

import httplib
from httplib import HTTPConnection, HTTPS_PORT
import ssl, socket

class HTTPSConnection(HTTPConnection):
    "This class allows communication via SSL."
    default_port = HTTPS_PORT

    def __init__(self, host, port=None, key_file=None, cert_file=None,
            strict=None, timeout=socket._GLOBAL_DEFAULT_TIMEOUT,
            source_address=None):
        HTTPConnection.__init__(self, host, port, strict, timeout,
                source_address)
        self.key_file = key_file
        self.cert_file = cert_file

    def connect(self):
        "Connect to a host on a given (SSL) port."
        sock = socket.create_connection((self.host, self.port),
                self.timeout, self.source_address)
        if self._tunnel_host:
            self.sock = sock
            self._tunnel()
        # this is the only line we modified from the httplib.py file
        # we added the ssl_version variable
        self.sock = ssl.wrap_socket(sock, self.key_file, self.cert_file, ssl_version=ssl.PROTOCOL_TLSv1)

#now we override the one in httplib
httplib.HTTPSConnection = HTTPSConnection
# ssl_version corrections are done

#now we override the one in httplib
httplib.HTTPSConnection = HTTPSConnection
# ssl_version corrections are done

""" 1) Scrap indeed.com for jobs listings
    2) Fill mongo b with results and reviews from indeed
    3) For each company found, scrap glassdoor for additional reviews
"""

# Search settings
KEYWORD_FILTER = "Data Scientist"
LOCATION_FILTER = "Boston, MA"
KWFLAGS = ["Hadoop", "years experience", "years' experience","years of experience"]

# Other settings
MAX_PAGES_COMPANIES = 500
MAX_PAGES_REVIEWS = 500

# DB settings
client = MongoClient()
indeed_db = client.indeed      #use indeed_db database
indeed_jobs = indeed_db.jobs   #create collection for jobs ads
indeed_reviews = indeed_db.reviews  # create collection for company reviews

"""1) scrap indeed for jobs"""
jobs = indeed.get_jobs(KEYWORD_FILTER, LOCATION_FILTER, indeed_jobs, MAX_PAGES_COMPANIES,KWFLAGS)

"""2) Get companies reviews into mongodb"""
indeed.get_all_company_reviews(jobs, indeed_reviews, MAX_PAGES_REVIEWS)

