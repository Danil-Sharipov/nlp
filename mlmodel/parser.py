import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import bs4
from time import sleep
import asyncio


class NewsPageRBC:
    __PATTERN_1 = r'datetime=".*"'
    __PATTERN_2 = re.compile(r'^h[1-6]$|p|ul|strong')

    def __init__(self, url: str) -> None:
        response = requests.get(url)

        self.__work_base = BeautifulSoup(response.text, 'html.parser')
        self.__base_element = self.__work_base.find('article', class_='quote-style-t82ts6')

        self.__url = url
        self.__title = self.__base_element.find('h1',
                                                class_='MuiTypography-root MuiTypography-h1 quote-style-1u667we').text

        self.__title_add = self.__base_element.find('div', class_='MuiGrid-root quote-style-s7va7x')
        if self.__title_add is not None:
            self.__title_add = self.__title_add.text

        self.__text = self.__find_text()

    def __hash__(self):
        return self.__url

    def __find_text(self) -> str:
        result_text = []
        base_for_text = self.__work_base.find('div', class_='MuiGrid-root quote-style-x73tr6')
        text_mass = base_for_text.find_all('div', class_='')

        for item in text_mass:
            temp = item.find(self.__PATTERN_2, class_='')
            answer = self.__recurs(temp)
            result_text.append(''.join(answer))

        result_text = '\n'.join(result_text)
        return result_text

    def get_info(self) -> dict:
        info = {
            'URL': self.__url,
            'TITLE': self.__title,
            'TITLE_ADD': self.__title_add,
            'TEXT': self.__text
        }
        return info

    def __recurs(self, tag):
        answer = []
        for element in tag.contents:
            if type(element) != bs4.element.Tag:
                answer.append(element)
                continue
            answer.extend(self.__recurs(element))
        else:
            return answer


class Parser:
    __INFO = {
        'rbc': 'https://quote.ru/news/article/'
    }

    def __init__(self, website: str = 'rbc') -> None:
        if website.lower() not in self.__INFO:
            raise 'Сайта нет (check web_info)'

        self.__website = self.__INFO[website]
        self.__data_frame = None

    def update_data_frame(self, data_frame: pd.DataFrame) -> pd.DataFrame:
        url_list = []
        title_list = []
        title_add_list = []
        text_list = []

        self.__data_frame = data_frame

        for page_id in data_frame['ID'].values:
            new_page = self.__get_last_page(page_id).get_info()
            sleep(0.3)
            url_list.append(new_page['URL'])
            title_list.append(new_page['TITLE'])
            title_add_list.append(new_page['TITLE_ADD'])
            text_list.append(new_page['TEXT'])

        self.__data_frame.insert(loc=0, column='URL',
                                 value=pd.Series(url_list), allow_duplicates=True)
        self.__data_frame.insert(loc=0, column='TITLE',
                                 value=pd.Series(title_list), allow_duplicates=True)
        self.__data_frame.insert(loc=0, column='TITLE_ADD',
                                 value=pd.Series(title_add_list), allow_duplicates=True)
        self.__data_frame.insert(loc=0, column='TEXT',
                                 value=pd.Series(text_list), allow_duplicates=True)

        return self.__data_frame

    def __get_last_page(self, page_id) -> NewsPageRBC:
        last_page = NewsPageRBC(self.__website + page_id)
        return last_page
