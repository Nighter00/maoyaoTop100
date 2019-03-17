import requests
from requests.exceptions import RequestException
import re
import json
from multiprocessing import Pool
import pymongo

def get_one_page(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.text
        return None
    except RequestException:
        return None


def parse_one_page(html):
    pattern = re.compile(
        '<dd>.*?board-index.*?>(\d+)</i>.*?data-src="(.*?)".*?title="(.*?)".*?star">(.*?)</p>.*?releasetime">(.*?)</p>.*?integer">(.*?)</i>.*?fraction">(.*?)</i>.*?</dd>',
        re.S)
    items = re.findall(pattern, html)
    for item in items:
        yield {
            'index': item[0],
            'image': item[1],
            'title': item[2],
            'actor': item[3].replace(' ', '').replace('\n', ''),
            'time': item[4],
            'score': item[5] + item[6]
        }


def write_to_file(content):
    with open('result.txt', 'a', encoding='utf-8') as f:
        f.write(json.dumps(content, ensure_ascii=False) + '\n')
        f.close()


def main(offset):
    url = 'http://maoyan.com/board/4?offset=' + str(offset)
    html = get_one_page(url)
    items = parse_one_page(html)
    myclient = pymongo.MongoClient('mongodb://localhost:27017/')
    mydb = myclient['maoYan100']
    mycol = mydb['top100']
    for item in items:
        write_to_file(item)
        mycol.insert_one(item)


if __name__ == '__main__':
    pool = Pool()
    pool.map(main, [i*10 for i in range(10)])
