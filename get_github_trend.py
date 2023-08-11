# -*- coding: utf-8 -*-
import datetime
from codecs import open
import requests
from pyquery import PyQuery
from mysql_util import insert_table_by_batch


def scrape(language):
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.7; rv:11.0) Gecko/20100101 Firefox/11.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip,deflate,sdch',
        'Accept-Language': 'zh-CN,zh;q=0.8'
    }
    today_str = datetime.datetime.now().strftime('%Y-%m-%d')
    url = 'https://github.com/trending/{language}'.format(language=language)
    r = requests.get(url, headers=HEADERS)
    assert r.status_code == 200

    d = PyQuery(r.content)
    items = d('div.Box article.Box-row')
    ds = []
    for item in items:
        i = PyQuery(item)
        title = i(".lh-condensed a").text()
        description = i("p.col-9").text()
        url = i(".lh-condensed a").attr("href")
        url = "https://github.com" + url
        star_fork = i(".f6 a").text().strip()
        star, fork = star_fork.split()
        new_star = i(".f6 svg.octicon-star").parent().text().strip().split()[1]
        star = int(star.replace(',', ''))
        fork = int(fork.replace(',', ''))
        new_star = int(new_star.replace(',', ''))
        ds.append([today_str, language, title, url, description, star, fork, new_star])
    sql = '''
    replace into github.ods_github_trend
    values (%s, %s,%s,%s,%s, %s,%s,%s)
    '''
    insert_table_by_batch(sql, ds)


def job():
    # write markdown
    scrape('')  # full_url = 'https://github.com/trending?since=daily'
    scrape('python')
    scrape('java')
    scrape('javascript')
    scrape('go')
    scrape('scala')

if __name__ == '__main__':
    job()
    