# -*- coding: utf-8 -*-
import itertools
import re

import boto
# import boto3
import warc
import gzip
import traceback

from boto.s3.key import Key
from gzipstream import GzipStreamFile
import mrjob
import mrjob.util as util
from mrjob.job import MRJob
from mrjob.protocol import RawProtocol

# from spacy.en import English
# from spacy.tokens.doc import Doc

import tldextract
# import ftfy
import sys
import socket

# import unicodedata
# import codecs

import logging

logging.basicConfig(
    format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

# mrjob.util.log_to_stream(stream=sys.stderr, level=logging.DEBUG, debug=True)

domains = ['fortune', 'readwrite', 'recode',
           'techcrunch', 'thenextweb', 'venturebeat']


class MRArticleExtractor(MRJob):

    def process_record(self, record):
        # logging.debug("Processing record")
        try:
            if record['content-type'] != 'text/plain':
                logging.debug("Skipping: " + record['content-type'])
                return
            url = record.header.get('warc-target-uri', None)
            # logging.debug("Processing record with URL " + url)
            if url:
                domain = tldextract.extract(url).domain
                if domain in domains:
                    logging.debug("Matches domain list: " + domain)
                    payload = record.payload.read().decode('utf-8', errors='strict')
                    # payload = record.payload.read()
                    # logging.info("Text encoding: " + str(type(payload)))
                    # payload = deaccent(payload)
                    # yield {"domain": domain}, transform_doc(nlp(payload))
                    logging.debug(
                        "Successfully extracted paylor for domain: " + domain)
                    yield {"domain": domain}, payload
        except IOError:
            logging.error("WARC IO Error: " + str(IOError))
            yield {'domain': 'error'}, ''
        except Exception as e:
            logging.error("WARC IO Error: " +
                          traceback.format_exception(*sys.exc_info()))
            yield {'domain': 'error'}, ''

    def mapper(self, _, line):
        # if we're running on Amazon (in EMR), retrieve files
        # from aws-publicdatasets
        # logging.error("FQDN: " + socket.getfqdn())
        fqdn = socket.getfqdn()
        if fqdn.find('ec2') != -1:
            logging.debug("Running on ec2 with fqdn of: " + fqdn)
            HADOOP_INPUT_FORMAT = 'org.apache.hadoop.mapred.lib.NLineInputFormat'
            INPUT_PROTOCOL = RawProtocol
            try:
                # Connect to Amazon S3 using anonymous credentials
                conn = boto.connect_s3(anon=True, host='s3.amazonaws.com')
                logging.debug("Successfully opened connection to s3")

                pds = conn.get_bucket('aws-publicdatasets', validate=False)
                logging.debug("Bucket is aws-puiblicdatasets")

                # Start a connection to one of the WARC files
                # k = boto3.resource('s3').Object('aws-publicdatasets', line).get()['Body']
                k = Key(pds, line)
                logging.debug("Got key for line: " + line)
                f = warc.WARCFile(fileobj=GzipStreamFile(k))
                logging.debug("Got a warc file stream for: " + line)
            except Exception:
                logging.error("WARC Open from S3 Error for line {}:\n{}"
                              .format(line,  traceback.format_exception(*sys.exc_info())))
                yield {'domain': 'error'}, ['']

        If we're local, use files on the local file system
        requires absolute path to files
        else:
            try:
                logging.info('Loading local file {}'.format(line))
                f = warc.WARCFile(fileobj=gzip.open(line, 'rb'))
                logging.debug("Got a warc file stream for: " + line)
            except Exception as e:
                logging.error("WARC Open from S3 Error for line {}:\n{}"
                              .format(line,  traceback.format_exception(*sys.exc_info())))
                yield {'domain': 'error'}, ['']


        logging.debug("Iterating on f for line: " + line)
        try:
            for record in f:
                # logging.debug("Processing record for line: " + line)
                for key, value in self.process_record(record):
                    logging.debug(u"Got a key value pair for line: {}\nkey: {}"
                                  .format(line, key))
                    yield key, value
        except Exception:
            logging.error("WARC iterate error for line {}:\n{}"
                          .format(line,  traceback.format_exception(*sys.exc_info())))
            yield {'domain': 'error'}, ['']

    def reducer(self, domain, values):
        logging.debug("Running reducer for key: " + str(domain))
        yield domain, {"text": list(values)}

if __name__ == '__main__':
    MRArticleExtractor.run()
