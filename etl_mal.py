# -*- coding: utf-8 -*-
"""
Created on Tue Sep 13 15:58:00 2022

@author: Dorian
"""

import json
import time
import random

from bs4 import BeautifulSoup
import cloudscraper

scraper = cloudscraper.create_scraper(delay=2)  # returns a CloudScraper instance

from airflow import DAG
from datetime import date, timedelta, datetime

from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook

DAG_DEFAULT_ARGS = {
    'owner': 'admin',
    'start_date': datetime.today() - timedelta(days=1),
    'schedule_interval': '@daily'
}


def extract_topmanga_mal(pages=20):
    url = "https://myanimelist.net/topmanga.php?limit="

    data = {"series": []}

    for i in range(pages):
        print("Page", str(i+1))
        time.sleep(1)
        html_page = BeautifulSoup(scraper.get(url + str(i * 50)).text, 'html.parser')

        # liste de <tr class="ranking-list"></tr>
        list_tr_page = html_page.findAll("tr", {"class": "ranking-list"})

        # each tr == a row
        for tr in list_tr_page:
            # each td == a column
            rank = int(tr.find("td", {"class": "rank ac"}).text.replace('\n', ''))
            title = tr.find("h3", {"class": "manga_h3"}).text
            url_serie = tr.find("h3", {"class": "manga_h3"}).find("a")["href"]

            data["series"].append({'Rank': rank,
                                   'Title': title,
                                   'URL': url_serie})

    print(len(data["series"]), "series à extraire")

    return data


def extract_stats_tab(url, stats):
    stats["Summary Stats"] = {}
    stats["Score Stats"] = {}
    html_page = BeautifulSoup(scraper.get(url+"/stats").text, 'html.parser')

    # Summary Stats
    right_side_div = html_page.find("div", {"class": "rightside js-scrollfix-bottom-rel"})
    data_div = right_side_div.findAll("div", {"class": "spaceit_pad"})

    for div in data_div:
        if div.findAll("span", {"class": "dark_text"}):
            div_text = div.text
            k = div_text.split(": ")[0]
            v = int(div_text.split(": ")[1].replace(",", ""))

            stats["Summary Stats"][k] = v

    # Score Stats
    table_score = right_side_div.find("table", {"class": "score-stats"})
    tr_score = table_score.findAll("tr")

    for tr in tr_score:
        td = tr.findAll("td")

        score = int(td[0].text)
        value = td[1].text.replace(u'\xa0', u' ').strip().replace("% ", "_").split("_")
        percentage = float(value[0].replace("%", ""))
        n_votes = int(value[1].replace("(", "").replace(")", "").split(" ")[0])

        stats["Score Stats"][score] = {"percentage":percentage, "votes":n_votes}

    return stats


def extract_characters_tab(url):
    time.sleep(random.random())
    characters = []

    html_page = BeautifulSoup(scraper.get(url+"/characters").text, 'html.parser')
    list_t = html_page.findAll("table", {"class": "js-manga-character-table"})

    for table in list_t:
        dict_char = {}
        list_div_info = table.findAll("div", {"class": "spaceit_pad"})
        # name
        dict_char["name"] = list_div_info[0].text.strip()
        # type of character
        dict_char["type"] = list_div_info[1].text.strip()
        # number of favorites (ex: 72049 Favorites)
        dict_char["n_favorites"] = int(list_div_info[2].text.strip().split()[0])
        img = table.find("img")
        # https://cdn.myanimelist.net/r/42x62/images/characters/9/347984.webp?s=188ec3609ba7a95abff7bed1264dd61e
        url1 = img["data-src"].split("?")[0]
        # https://cdn.myanimelist.net/r/42x62/images/characters/9/347984.webp
        url2 = url1.replace("r/42x62/", "")
        # https://cdn.myanimelist.net/images/characters/9/347984.webp
        url3 = url2.replace(".webp", ".jpg")
        dict_char["url_image"] = url3
        characters.append(dict_char)

    return characters


def extract():
    data = extract_topmanga_mal()

    data_plus = {"series": []}

    for serie in data['series']:
        time.sleep(random.random())
        print("Extraction", serie["Rank"], serie["Title"])
        html_page = BeautifulSoup(scraper.get(serie['URL']).text, 'html.parser')

        left_side_div = html_page.find("div", {"class": "leftside"})
        data_div = left_side_div.findAll("div", {"class": "spaceit_pad"})

        alternative_titles = {}
        is_alternative_titles = True
        information = {}
        is_information = False
        statistics = {}
        is_statistics = False

        for div in data_div:
            key_value = div.text.split(":")
            k = key_value[0].strip()
            v = key_value[1].strip()

            if '\n' in key_value[0]:
                k = k.replace('\n', '')

            # Alternative Titles
            # Information
            if k == "Type":
                is_alternative_titles = False
                is_information = True
                is_statistics = False

            # Statistics
            if k == "Score":
                is_alternative_titles = False
                is_information = False
                is_statistics = True

            if is_statistics:
                statistics[k] = v

            if is_information:
                information[k] = v

            if is_alternative_titles:
                alternative_titles[k] = v

        data_plus["series"].append({"Rank": serie["Rank"],
                                    "Title": serie["Title"],
                                    "URL": serie["URL"],
                                    "Alternative titles": alternative_titles,
                                    "Information": information,
                                    "Statistics": extract_stats_tab(serie["URL"], stats=statistics),
                                    "Characters": extract_characters_tab(serie["URL"])})

    with open('extract.json', 'w') as f:
        json.dump(data_plus, f)


def transform_genres(genres_txt):
    tmp1 = genres_txt.split(',')
    return [token.split('\n\n')[0].strip() for token in tmp1]


def transform_themes(themes_txt):
    tmp1 = themes_txt.split(',')
    return [token.split('\n\n')[0].strip() for token in tmp1]


def transform_demographic(demographic_txt):
    tmp1 = demographic_txt.split(',')
    return [token.split('\n\n')[0].strip() for token in tmp1]


def transform_authors(authors_txt):
    obj = {'Story': [], 'Art': []}

    new_authors_txt = authors_txt.replace('),', ') /')
    tmp1 = new_authors_txt.split(" / ")

    for token in tmp1:
        if "(Art)" in token:
            obj['Art'] = token.split('(Art)')[0].strip()
        if "(Story)" in token:
            obj['Story'] = token.split('(Story)')[0].strip()
        if "(Story & Art)" in token:
            obj['Art'] = token.split('(Story & Art)')[0].strip()
            obj['Story'] = token.split('(Story & Art)')[0].strip()

    return obj


def transform_members(members_txt):
    return int(members_txt.replace(',', ''))


def transform_favorites(favorites_txt):
    return int(favorites_txt.replace(',', ''))


def transform_score(score_txt):
    return float(score_txt.split(" ")[0][:-1])


def transform_ranked(ranked_txt):
    return int(ranked_txt.split(" ")[0][1:-1])


def transform_popularity(popularity_txt):
    return int(popularity_txt.replace("#", ""))


def transform():
    with open('extract.json', 'r') as f:
        data = json.load(f)

    for serie in data['series']:
        # Clean
        if "Genre" in serie["Information"]:
            serie["Information"]["Genres"] = serie["Information"].pop("Genre")
        if "Theme" in serie["Information"]:
            serie["Information"]["Themes"] = serie["Information"].pop("Theme")
        if "Demographic" in serie["Information"]:
            serie["Information"]["Demographics"] = serie["Information"].pop("Demographic")

        # Transformation
        if "Genres" in serie["Information"]:
            serie["Information"]["Genres"] = transform_genres(serie["Information"]["Genres"])
        if "Themes" in serie["Information"]:
            serie["Information"]["Themes"] = transform_themes(serie["Information"]["Themes"])
        if "Demographics" in serie["Information"]:
            serie["Information"]["Demographics"] = transform_demographic(serie["Information"]["Demographics"])

        serie["Information"]["Authors"] = transform_authors(serie["Information"]["Authors"])
        serie["Statistics"]["Members"] = transform_members(serie["Statistics"]["Members"])
        serie["Statistics"]["Favorites"] = transform_favorites(serie["Statistics"]["Favorites"])
        serie["Statistics"]["Score"] = transform_score(serie["Statistics"]["Score"])
        serie["Statistics"]["Ranked"] = transform_ranked(serie["Statistics"]["Ranked"])
        serie["Statistics"]["Popularity"] = transform_popularity(serie["Statistics"]["Popularity"])

    with open('transform.json', 'w') as f:
        json.dump(data, f)


def load():
    mongohook = MongoHook(conn_id="mongo_getmangas")

    mongohook.delete_many(mongo_collection="mal",
                          mongo_db="getmanga_db",
                          filter_doc={})

    with open('transform.json', 'r') as f:
        data = json.load(f)

    mongohook.insert_many(mongo_collection="mal",
                          mongo_db="getmanga_db",
                          docs=data["series"])


with DAG(dag_id='etl_mal',
         default_args=DAG_DEFAULT_ARGS,
         description='ETL from MyAnimeList.net') as dag:

    extract = PythonOperator(task_id="extract",
                             python_callable=extract)

    transform = PythonOperator(task_id="transform",
                               python_callable=transform)

    load = PythonOperator(task_id="load",
                          python_callable=load)

    extract >> transform >> load