from airflow import DAG
from datetime import timedelta

from  airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

import urllib.request
import time
import glob
import os
import json

def catalog():

    def pull(url):
        response = urllib.request.urlopen(url).read()
        data = response.decode('utf-8')
        return data

    def store(data, file):
        with open("dags/data/" + file, 'w+') as file:
            file.write(data)
            

    with open("dags/data/00_urls.txt", 'r') as file:
        lines = file.readlines()
        urls = [line.strip() for line in lines]

    for url in urls:
        data = pull(url)
        
        index = url.rfind('/') + 1
        file = url[index:]
        store(data, file)

        print('pulled: ' + file)
        print('--- waiting ---')
        time.sleep(15)

def combine():
    with open('dags/combo.txt', 'w+') as outfile:
        for file in glob.glob("dags/data/*.html"):
            with open(file) as infile:
                outfile.write(infile.read())

def titles():
    from bs4 import BeautifulSoup

    def store_json(data,file):
        with open(file, 'w', encoding='utf-8') as f:
           json.dump(data, f, ensure_ascii=False, indent=4)
           print('wrote file: ' + file)

    with open('dags/combo.txt', 'r') as html:

        html = html.read().replace('\n', ' ').replace('\r', '')

        #the following creates an html parser
        soup = BeautifulSoup(html, "html.parser")
        results = soup.find_all('h3')
        titles = []

        # tag inner text
        for item in results:
            titles.append(item.text)

        store_json(titles, 'dags/titles.json')

def clean():
    def store_json(data,file):
        with open(file, 'w', encoding='utf-8') as f:
           json.dump(data, f, ensure_ascii=False, indent=4)
           print('wrote file: ' + file)

    with open("dags/titles.json") as file:
        titles = json.load(file)

        # remove punctuation/numbers
        for index, title in enumerate(titles):
            punctuation= '''!()-[]{};:'"\,<>./?@#$%^&*_~1234567890'''
            translationTable= str.maketrans("","",punctuation)
            clean = title.translate(translationTable)
            titles[index] = clean

        # remove one character words
        for index, title in enumerate(titles):
            clean = ' '.join( [word for word in title.split() if len(word)>1] )
            titles[index] = clean

        store_json(titles, 'dags/titles_clean.json')

def count_words():
    from collections import Counter

    def store_json(data,file):
        with open(file, 'w', encoding='utf-8') as f:
           json.dump(data, f, ensure_ascii=False, indent=4)
           print('wrote file: ' + file)

    with open("dags/titles_clean.json") as file:
        titles = json.load(file)
        words = []

        # extract words and flatten
        for title in titles:
            words.extend(title.split())

        # count word frequency
        counts = Counter(words)
        store_json(counts, 'dags/words.json')


with DAG(
   "assignment",
   start_date=days_ago(1),
   schedule_interval="@daily",catchup=False,
) as dag:

    # ts are tasks
    t0 = BashOperator(
        task_id='task_zero',
        bash_command='pip install beautifulsoup4',
        retries=2
    )
    t1 = PythonOperator(
        task_id='task_one',
        depends_on_past=False,
        python_callable=catalog
    )

    t2 = PythonOperator(
        task_id='task_two',
        depends_on_past=False,
        python_callable=combine
    ) 

    t3 = PythonOperator(
        task_id='task_three',
        depends_on_past=False,
        python_callable=titles
    ) 

    t4 = PythonOperator(
        task_id='task_four',
        depends_on_past=False,
        python_callable=clean
    ) 

    t5 = PythonOperator(
        task_id='task_five',
        depends_on_past=False,
        python_callable=count_words
    ) 

    t0>>t1>>t2>>t3>>t4>>t5