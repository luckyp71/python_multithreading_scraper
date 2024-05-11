# yahoo finance url: https://finance.yahoo.com/quote/AAPL
import threading

from bs4 import BeautifulSoup
from lxml import etree
import requests


class WikiWorkerMasterScheduler(threading.Thread):
    def __init__(self, output_queue, **kwargs):
        if 'input_queue' in kwargs:
            kwargs.pop('input_queue')

        self._input_values = kwargs.pop('input_values')
        tmp_queue = output_queue
        if type(tmp_queue) != list:
            tmp_queue = [tmp_queue]
        self._output_queues = tmp_queue
        super(WikiWorkerMasterScheduler, self).__init__(**kwargs)
        self.start()

    def run(self):
        for entry in self._input_values:
            wikiWorker = WikiWorker(entry)
            # symbol_counter = 0
            for data in wikiWorker.get_sp_500_companies():
                for output_queue in self._output_queues:
                    output_queue.put(data['symbol'])

        # for output_queue in self._output_queues:
        #     for i in range(20):
        #         output_queue.put('DONE')

class WikiWorker():
    def __init__(self, url):
        self._url = url

    @staticmethod
    def _extract_company_symbols(page_html):
        soup = BeautifulSoup(page_html, 'html.parser')
        dom = etree.HTML(str(soup))
        rows = dom.xpath("//table[contains(@id,'constituents')]/tbody/tr")
        for row in rows[1:]:
            symbol = row.xpath(".//td[1]/a/text()")[0]
            company_name = row.xpath(".//td[2]/a/text()")[0]

            yield {
                "symbol": symbol,
                "company_name": company_name
            }

    def get_sp_500_companies(self):
        response = requests.get(self._url)
        if response.status_code != 200:
            print("Couldn't get entries")
            return []

        yield from self._extract_company_symbols(response.text)
