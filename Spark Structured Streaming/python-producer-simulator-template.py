"""
Produce simulated stock quotes as a Kafka Producer
"""
import argparse
from kafka import KafkaProducer

import json
import random
import time

iextoken = ""
stock_list = [
	"msft", "ba", "jnj", "f", "tsla", "bac", "ge", "mmm", "intc", "wmt"
]
stock_stats = {
	"msft": {"mean": 152, "stddev": 5}, "ba": {"mean": 345, "stddev": 10},
	"jnj": {"mean": 141, "stddev": 10}, "f": {"mean": 9, "stddev": 1},
	"tsla": {"mean": 330, "stddev": 10}, "bac": {"mean": 34, "stddev": 1},
	"ge": {"mean": 11, "stddev": 1}, "mmm": {"mean": 168, "stddev": 5},
	"intc": {"mean": 55, "stddev": 2}, "wmt": {"mean": 120, "stddev": 3},
}
kafka_brokers = [
	"wn0-kafkas.cdpy143v1neujejbg4fhxrukga.bx.internal.cloudapp.net:9092",
]
kafka_topic = "stockVals"

parser = argparse.ArgumentParser()
parser.add_argument("-n", type=int, default=1000)
args = parser.parse_args()
print(f"Number of loops is {args.n}")

print("Running in simulated mode")

request_params = {"token": iextoken, "symbols": ",".join(stock_list)}

producer = KafkaProducer(
	bootstrap_servers=kafka_brokers,
	key_serializer=lambda k: k.encode("ascii", "ignore"),
	value_serializer=lambda x: json.dumps(x).encode("utf-8")
)


def main():
	"""
	Main routine produce `args.loops` simulated list of stock prices
	to `kafka_topic`, one per second.
	"""
	for x in range(args.n):
		response_list = simulated_response()

		print(json.dumps(response_list, indent=2))
		for stock in response_list:
			future = producer.send(
				kafka_topic, key=stock["symbol"], value=stock,
			)
			response = future.get(timeout=10)
			print(response.topic)
			print(response.partition)
			print(response.offset)
		time.sleep(1)


def simulated_response():
	"""
	Define simulator method: returns list with artificial stock data
	"""
	response_list = []
	for stock in stock_list:
		stock_record = {
			"symbol": stock.upper(),
			"time": current_milli_time(),
			"price": round(
				random.gauss(
					stock_stats[stock]["mean"],
					stock_stats[stock]["stddev"]
				), 3
			),
			"size": random.randint(1, 500),
		}
		response_list.append(stock_record)

	return response_list


def current_milli_time():
	"""
	returns time in milliseconds
	"""
	return round(time.time() * 1000)


main()
