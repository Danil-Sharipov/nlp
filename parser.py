import requests
from bs4 import BeautifulSoup
import yaml
import pandas as pd


class NewsPage:

    def __init__(self, work_base, locations) -> None:
        self.__locations = locations
        base_element = work_base.find(*self.__locations['NEWS_SHORT']['BASE_ELEMENT'])

        self.__url = base_element.find(*self.__locations['NEWS_SHORT']['URL']).get('href')
        self.__title = base_element.find(*self.__locations['NEWS_SHORT']['TITLE']).text
        self.__date = base_element.find(*self.__locations['NEWS_SHORT']['DATE']).text
        self.__text = self.__find_text()

    def __hash__(self):
        return self.__url

    def __find_text(self) -> str:
        response = requests.get(self.__url)
        temp = BeautifulSoup(response.text, 'lxml')
        base_element = temp.find_all(*self.__locations['NEWS_TEXT']['BASE_ELEMENT'])
        text = []
        for item in base_element:
            text.append(item.text)
        result = ' '.join(text)
        return result

    def get_info(self) -> pd.DataFrame:
        info = {
            'URL': [self.__url],
            'TITLE': [self.__title],
            'DATE': [self.__date],
            'TEXT': [self.__text]
        }
        result = pd.DataFrame(info)
        return result


class Parser:
    __INFO = {
        'rbc': 'https://www.rbc.ru/short_news',
        'ria': 'https://ria.ru/politics/'
    }

    def __init__(self, website: str) -> None:
        if website.lower() not in self.__INFO:
            raise 'Сайта нет (check web_info)'

        response = requests.get(self.__INFO[website])

        self.__data_frame = pd.DataFrame(columns=['URL', 'TITLE', 'DATE', 'TEXT'])
        self.__work_base = BeautifulSoup(response.text, 'lxml')
        self.__locations = yaml.safe_load(open('locations.yaml'))[website]

    def update_data_frame(self, time=30) -> pd.DataFrame:
        new_page = self.__get_last_page().get_info()
        self.__data_frame = pd.concat([self.__data_frame, new_page], ignore_index=True)

        return self.__data_frame

    def __get_last_page(self) -> NewsPage:
        last_page = NewsPage(self.__work_base, self.__locations)
        return last_page
