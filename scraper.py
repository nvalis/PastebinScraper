#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
from pymongo import MongoClient

import json
import time
from datetime import datetime
import re
from multiprocessing import Process, Queue
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)-12s (%(levelname)s) - %(message)s')#, filename='scraper.log')
logger = logging.getLogger(__name__)

def access_denied(text):
	return re.match('THIS IP: [0-9\.]+ DOES NOT HAVE ACCESS\. VISIT: http:\/\/pastebin\.com\/scraping TO GET ACCESS!', text)

def ip_is_whitelisted():
	r = requests.get('http://pastebin.com/api_scraping.php')
	if r.status_code == requests.codes.ok:
		return not access_denied(r.text)
	else:
		logger.critical('Pastebin returned code {}: {}'.format(r.status_code, r.text))	

def whitelist_ip_if_needed():
	if not ip_is_whitelisted():
		my_ip = requests.get('http://ipecho.net/plain').text
		logger.debug('Got my IP: {}'.format(my_ip))
		r = requests.post('http://pastebin.com/api_scraping_faq.php', data={'submit_hidden':'submit_hidden', 'whitelist_ip':my_ip, 'submit':'Whitelist My IP'})
		logger.info('Whitelisted IP')
	else:
		logger.debug('Did not need to whitelist IP')

def fill_queue(queue):
	db_client = MongoClient()
	collection = db_client.pastebin_scraper.pastes

	logger = logging.getLogger('QueueFiller')
	while True:
		logger.info('Get paste index')
		r = requests.get('http://pastebin.com/api_scraping.php')
		if access_denied(r.text):
			logger.warning('Problem with IP whitelisting detected! Trying to re-whitelist')
			whitelist_ip_if_needed()
			continue
		if r.status_code == requests.codes.ok:
			new_pastes = json.loads(r.text)
			for paste in new_pastes:
				if not collection.find({'key':paste['key']}).count():
					queue.put(paste)
			logger.info('New queue size: {}'.format(queue.qsize()))
		else:
			logger.critical('Pastebin returned code {}: {}'.format(r.status_code, r.text))
			continue

		time.sleep(30)

def fetch_pastes(queue):
	db_client = MongoClient()
	collection = db_client.pastebin_scraper.pastes

	logger = logging.getLogger('Scraper')
	while True:
		if queue.empty():
			time.sleep(5)
			continue

		paste = queue.get()
		if collection.find({'key':paste['key']}).count() > 0:
			logger.info('Skipping paste {}'.format(paste['key']))
			continue
		logger.info('Scraping paste {}'.format(paste['key']))
		# insert meta data
		now = datetime.utcnow()
		collection.update_one(
			{'key':paste['key']},
			{
				'$setOnInsert':{'first_seen':now},
				'$set':{
					'last_seen':now, 'key':paste['key'], 'full_url':paste['full_url'],
					'scrape_url':paste['scrape_url'], 'date':datetime.utcfromtimestamp(int(paste['date'])),
					'size':int(paste['size']), 'expire':datetime.utcfromtimestamp(int(paste['expire'])),
					'title':paste['title'], 'syntax':paste['syntax'], 'user':paste['user']
				}
			},
			upsert=True
		)

		# get paste content
		tries = 0
		while tries < 10:
			r = requests.get(paste['scrape_url'])
			if r.status_code != requests.codes.ok:
				tries += 1
				logger.warning('Got status code {} from pastebin.com, retrying in 10 seconds'.format(r.status_code))
				time.sleep(10)
			if access_denied(r.text):
				logger.warning('Problem with IP whitelisting detected! Trying to re-whitelist')
				whitelist_ip_if_needed()
				tries += 1
				continue
			paste['content'] = r.text
			break

		if tries == 10:
			logger.critical('Max number of tries exceeded! Stopping')
			return

		# insert paste content
		collection.update_one({'key':paste['key']}, {'$set':{'content':paste['content']}})
		logger.debug('Scraping paste {} done'.format(paste['key']))
		time.sleep(2)

def main():
	paste_queue = Queue()
	queue_process = Process(target=fill_queue, args=(paste_queue,))
	scraper_process = Process(target=fetch_pastes, args=(paste_queue,))

	whitelist_ip_if_needed()
	queue_process.start()
	scraper_process.start()

	queue_process.join()
	scraper_process.join()
	logger.info('Done')

if __name__=='__main__':
	main()