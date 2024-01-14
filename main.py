import threading
import advertools as adv
from requests_html import HTMLSession
from requests.exceptions import SSLError, ConnectionError
import csv
import time
import os

sitemap = adv.sitemap_to_df('https://www.storyly.io/sitemap.xml')

threads = list()
data = list()

total_page = len(sitemap['loc'])
thread_count = 3
sleep_time = 2 # if you got SSLError or connection error increase the sleep time according to your need.

def scrapper(url, sleep_time=sleep_time):
    print(f"Thread {threading.current_thread().name} is processing {url}")
    with HTMLSession() as session:
        try:
            response = session.get(url)
            if response.status_code == 200:
                title_xpath = response.html.xpath('//title//text()')
                description_xpath = response.html.xpath('//meta[@name="description"]/@content')
                canonical_xpath = response.html.xpath("//link[@rel='canonical']/@href")
                title = title_xpath[0] if title_xpath else 'title missing'
                description = description_xpath[0] if description_xpath else 'description missing'
                canonical = canonical_xpath[0] if canonical_xpath else 'canonical missing'
            else:
                print(url, response.status_code, 'HTTP Status ERROR')
        except SSLError and ConnectionError:
            time.sleep(sleep_time)
            scrapper(url)
        else:
            data.append({'url':url, 'title':title, 'description':description, 'canonical':canonical})

for loop in range(int(total_page / thread_count)):
    for index in range(loop * thread_count, (loop + 1) * thread_count):
        if index < total_page:
            url = sitemap['loc'][index].strip()
            thread = threading.Thread(target=scrapper, args=(url,))
            threads.append(thread)
            thread.start()

for t in threads:
    t.join()


csv_path = os.path.join(os.getcwd(), 'scrapper-results.csv')
with open(csv_path, 'w', encoding='utf-8') as file:
    csv_file = csv.DictWriter(file, fieldnames=['url', 'title', 'description', 'canonical'])
    csv_file.writeheader()
    csv_file.writerows(data)
