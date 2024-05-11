import queue
import threading
from lxml import html
import requests
import datetime
import random
import time


class YahooFinancePriceScheduler(threading.Thread):
    def __init__(self, input_queue,output_queue, **kwargs):
        super(YahooFinancePriceScheduler, self).__init__(**kwargs)
        self._input_queue = input_queue
        tmp_queue = output_queue
        if type(tmp_queue) != list:
            tmp_queue = [tmp_queue]
        self._output_queue = tmp_queue
        self.start()

    def run(self):
        while True: # it's going to be waiting for values to come out of the queues and read them
            try:
                val = self._input_queue.get(timeout=10)
            except queue.Empty:
                print('Yahoo scheduler queue is empty, stopping....')
                break
            if val == 'DONE':
                break

            yahooFinancePriceWorker = YahooFinanceWorker(symbol=val)
            price = yahooFinancePriceWorker.get_price()
            for output_queue in self._output_queue:
                output_values = (val, price, datetime.datetime.utcnow())
                output_queue.put(output_values)
            # print(val, price)
            time.sleep(random.random())

        # for output_queue in self._output_queue:
        #     for i in range(20):
        #         output_queue.put('DONE')


class YahooFinanceWorker():
    def __init__(self, symbol):
        # super(YahooFinanceWorker, self).__init__(**kwargs)
        self._symbol = symbol
        base_url = 'https://finance.yahoo.com/quote/'
        self._url = f'{base_url}{self._symbol}'

    def get_price(self):
        # time.sleep(20 * random.random())
        response = requests.get(self._url)
        if response.status_code != 200:
            return
        page_contents = html.fromstring(response.content)
        data = page_contents.xpath("//fin-streamer[contains(@class, 'livePrice')]/span")
        if (len(data) >= 1):
            price = float(data[0].text.replace(",", ""))
            # price = float(data[0].text)
            return price

# price: //fin-streamer[contains(@class, 'livePrice')]/span
# change: //fin-streamer[contains(@class, 'priceChange')][2]/span[contains(@class,'change')]
