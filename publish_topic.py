import os
import sys
from requests import Session
from time import sleep
import logging
from google.cloud import pubsub_v1
from concurrent import futures

GOOGLE_CREDENTIALS = os.getenv('JSON_CREDENTIAL_FILE')
PROJECT_ID = os.getenv('PROJECT_ID')
TOPIC_ID = os.getenv('TOPIC_ID')

class PublishToPubsub:
	def __init__(self):
		self.project_id = PROJECT_ID
		self.topic_id = TOPIC_ID
		self.publisher = pubsub_v1.PublisherClient()
		self.topic_path = self.publisher.topic_path(self.project_id, self.topic_id)
		self.publish_futures = []

	def retrieve_crypto(self) -> str:
		"""Get real time data from selected crypto tickers"""
		url="https://api.nomics.com/v1/currencies/ticker"
		crypto_config = {"tickers":"BTC,ETH,DOGE","currency":"USD"}

		params = {
            "key": os.environ.get("Api_key", ""),
            "ids": crypto_config['tickers'],
            "convert": crypto_config['currency'],
            'interval': "1d",
            "per-page" : "100",
            "page": "1"
        }

		ses = Session()
		res = ses.get(url)

		if 200 <= res.status_code < 400:
			logging.info("Data fetched successfully")
			return res.text
		else:
			raise Exception("Failed to fetch API data")

	def get_callback(self, publish_future, data):
		def callback(publish_future):
			try:
				#wait 60 seconds for the publish call to succeed
				logging.info(publish_future.result(timeout=60))
			except futures.TimeoutError:
				logging.error("Publishing time out error")

		return callback

	def publish_Message_To_Topic(self, data):
		"""Publish message to a pubsub topic with a error handler"""

		#When you publish a message, the client returns a future
		publish_future = self.publisher.publish(self.topic_path, data.encode("utf-8"))

		# Non-blocking. Publish failures are handled in the callback function
		publish_future.add_done_callback(self.get_callback(publish_future, data))
		self.publish_futures.append(publish_future)

		# Wait for all the publish futures to resolve before existing
		futures.wait(self.publish_futures, return_when=futures.ALL_COMPLETED)

		logging.info("Published message to Topi")


if __name__ == "__main__":

	logging.basicConfig(level=logging.INFO)

	svc = PublishToPubsub()

	for i in range(24):
		message = svc.retrieve_crypto()
		svc.publish_Message_To_Topic(message)
        sleep(120)