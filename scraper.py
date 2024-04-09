import requests
import html_to_json
import threading
from queue import Queue
import time
from tqdm import tqdm

def get_page(page:int):
    url = f'https://multimedia.europarl.europa.eu/en/search?tab=streaming&orderBy=newest&q=&page={page}'
    response = requests.get(url)
    return response.status_code, response.text

def parse_list(response_text:str):
    j = html_to_json.convert(response_text)
    l = j['html'][0]['body'][0]['div'][0]['div'][0]['main'][0]['div'][0]['div'][0]['div'][1]['div'][0]['div'][0]['div'][1]['div'][0]['div'][1]['section'][0]['div'][0]['div'][0]['div']
    return l

def parse_link(x):
    return 'https://multimedia.europarl.europa.eu' + x['div'][0]['div'][0]['div'][2]['a'][0]['_attributes']['href']

class Scraper(threading.Thread):
    def __init__(self, input_queue:Queue, output_queue:Queue):
        threading.Thread.__init__(self)
        self.queue = input_queue
        self.output = output_queue

    def run(self):
        while True:
            page = self.queue.get()
            status_code, response_text = get_page(page)
            if status_code == 200:
                l = parse_list(response_text)
                links = [parse_link(x) for x in l]
                self.output.put(links)
            self.queue.task_done()

class Writer(threading.Thread):
    def __init__(self, output_queue:Queue):
        threading.Thread.__init__(self)
        self.output = output_queue
        
    def run(self):
        global counter, counter_lock
        while True:
            links = self.output.get()
            l = len(links)
            with open('links.txt', 'a') as f:
                print(*links, sep='\n', file=f)
            with counter_lock:
                counter += l
            self.output.task_done()

class Counter(threading.Thread):
    def __init__(self, total:int):
        threading.Thread.__init__(self)
        self.pbar = tqdm(total=total)
        self.last = 0
        
    def run(self):
        global counter, counter_lock
        while True:
            with counter_lock:
                self.pbar.update(counter - self.last)
                self.last = counter
            time.sleep(0.1)

counter = 0
counter_lock = threading.Lock()

def main(num_of_links:int, num_of_pages:int, num_of_threads:int):
    input_queue = Queue()
    output_queue = Queue()

    for i in range(1, 1+num_of_pages):
        input_queue.put(i)

    for i in range(num_of_threads-2):
        t = Scraper(input_queue, output_queue)
        t.daemon = True
        t.start()

    w = Writer(output_queue)
    w.daemon = True
    w.start()
    
    c = Counter(num_of_links)
    c.daemon = True
    c.start()

    input_queue.join()
    output_queue.join()

if __name__ == '__main__':
    num_of_links = 30010
    num_of_pages = 1668
    num_of_threads = 16
    main(num_of_links, num_of_pages, num_of_threads)
