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

# from spacy.en import English
# from spacy.tokens.doc import Doc

import tldextract
# import ftfy
import sys
import socket
import ujson
import hashlib
import uuid

from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool

# import unicodedata
# import codecs

import logging

# Retrieve from local file or S3?
REMOTE = False
OUTPUT_PATH = './out'

logging.basicConfig(
    format='%(asctime)s : %(levelname)s : %(message)s', level=logging.DEBUG)

domains = ['fortune', 'readwrite', 'recode',
           'techcrunch', 'thenextweb', 'venturebeat']


def process_record(record):
    # logging.debug("Processing record")
    if record['content-type'] != 'text/plain':
        logging.debug("Skipping: " + record['content-type'])
        return(False)
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
            return(dict([('domain', domain), ('payload', payload)]))
    else:
        return(False)



def process_file(line):
    output_contents = []
    line = line.strip()
    if REMOTE:
        try:
            # Connect to Amazon S3 using anonymous credentials
            conn = boto.connect_s3(anon=True, host='s3.amazonaws.com')
            logging.debug("Successfully opened connection to s3")

            pds = conn.get_bucket('aws-publicdatasets', validate=False)
            logging.debug("Bucket is aws-puiblicdatasets")

            # Start a connection to one of the WARC files
            k = Key(pds, line)
            logging.debug("Got key for line: " + line)
            f = warc.WARCFile(fileobj=GzipStreamFile(k))
            logging.debug("Got a warc file stream for: " + line)
        except Exception:
            logging.error("WARC Open from S3 Error for line {}:\n{}"
                          .format(line,  traceback.format_exception(*sys.exc_info())))
            return(dict([('line', line), ('success', False)]))
        # If we're local, use files on the local file system
        # requires absolute path to files
    else:
            try:
                logging.info('Loading local file {}'.format(line))
                f = warc.WARCFile(fileobj=gzip.open(line, 'rb'))
                logging.debug("Got a warc file stream for: " + line)
            except Exception as e:
                logging.error("WARC Open from S3 Error for line {}:\n{}"
                              .format(line,  traceback.format_exception(*sys.exc_info())))
                return(dict([('line', line), ('success', False)]))

    logging.debug("Iterating on f for line: " + line)
    try:
        for record in f:
            # logging.debug("Processing record for line: " + line)
            value = process_record(record)
            if value:
                logging.debug("Got content from for domain: {}\nline: {}"
                              .format(value['domain'], line))
                output_contents.append(value)
        if len(output_contents) > 0:
            out_filename = "{}/{}".format(OUTPUT_PATH,
                                          hashlib.md5(line).hexdigest())
            with open(out_filename, 'wb') as out_file:
                ujson.dump(output_contents, out_file)
    except Exception:
        logging.error("WARC iterate error for line {}:\n{}"
                      .format(line,  traceback.format_exception(*sys.exc_info())))
        return(dict([('line', line), ('success', False)]))

    return(dict([('line', line), ('success', True)]))


if __name__ == '__main__':
    lines = sys.stdin.readlines()

    pool = Pool(10)

    results = pool.map(process_file, lines)
    log_filename = "{}/{}.log".format(OUTPUT_PATH,
                                  hashlib.md5(''.join(lines)).hexdigest())
    with open(log_filename, 'wb') as out_file:
        ujson.dump(results, out_file)

    pool.close()
    pool.join()
