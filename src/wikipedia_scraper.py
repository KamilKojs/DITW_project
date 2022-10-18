import requests  # http requests
from bs4 import BeautifulSoup  # xml parsing
import re  # regular expressions
import urllib  # converting URLs properly
import pandas as pd
import json


def get_URL(title, base_url='https://en.wikipedia.org/wiki/'):

    new_title = ''.join(map(urllib.parse.quote, title))
    return base_url + new_title


def get_htmltext(url):

    response = requests.get(url)
    status_code = response.status_code

    if status_code != 200:
        return None
    else:
        return response.text


def get_actors(infobox_data):

    infobox_trs = infobox_data.find_all('tr')
    re_actorurl = re.compile('<li><a href=(.+?)"')

    for tr in infobox_trs:
        tr_string = str(tr)
        is_starringsection = re.search('Starring', tr_string)
        if is_starringsection:
            actor_lines = tr.find_all('li')
            actors = [line.a['title'] if line.a is not None
                      else line.string for line in actor_lines]  # manage cases when there is no title and just name written
            actor_urls = [
                'https://en.wikipedia.org' + line.a['href'] if line.a is not None else '' for line in actor_lines]  # manage cases when there is no href
    return actors, actor_urls


def get_poster(infobox_data):

    img_url = 'https:' + infobox_data.find_all('img', limit=1)[0]['src']
    return img_url


def get_year(soup):
    year = soup.find_all(
        'span', attrs={'class': 'bday dtstart published updated'},  limit=1)[0].string.split('-')[0]
    return year


def scrape_wikipedia(film_title):

    url = get_URL(film_title)
    html_text = get_htmltext(url)

    soup = BeautifulSoup(html_text, features="html.parser")

    # Find tr tags from infobox common in all film wiki pages
    infobox_data = soup.find_all(
        'table', attrs={'class': 'infobox vevent'},  limit=1)[0]

    # year
    year = get_year(soup)

    # poster image
    poster_image = get_poster(infobox_data)

    # actor info
    actors, actors_urls = get_actors(infobox_data)

    return film_title, url, year, poster_image, actors, actors_urls


def main():

    films = pd.read_csv('./data/movies_minimalversion.csv', sep=';')
    films_metadata = {}

    for index, row in films.iterrows():

        film_title, url, year, poster_image, actors, actors_urls = scrape_wikipedia(
            row['film_name'])
        film_dict = {
            'title': film_title,
            'url': url,
            'year': year,
            'poster': poster_image,
            'actors': actors,
            'actors_urls': actors_urls}

        films_metadata[row['id']] = film_dict

        # Save images
        urllib.request.urlretrieve(
            poster_image, './data/img/' + str(row['id']) + '_poster.jpg')

    # Save metadata
    with open("./data/metadata/movies_metadata.json", "w") as write_file:
        json.dump(films_metadata, write_file, indent=4, ensure_ascii=False)

    return


if __name__ == '__main__':

    main()
