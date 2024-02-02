import requests
import pandas as pd
import datetime
from math import floor
from time import sleep
import asyncio
import parser
import client

def get_json(x: int, y: int) -> dict:
    return requests.get(f'https://quote.ru/api/v1/news/for-main-page?latestNewsTime={x}&limit={y}').json()


def preparing_oh(x: int, y: int = 100) -> pd.DataFrame:
    """
    Получить DataFrame от x с y элементами
    """
    df = pd.DataFrame(get_json(x, y)['data'])[['id', 'publishDateTimestamp']]
    df['Time'] = pd.to_datetime(df.publishDateTimestamp, unit='ms')
    return df


def check_last(df, last):
    for indx, i in df.iterrows():
        if i['publishDateTimestamp'] < last:
            return indx, True
    return 0, False


def time_right_now() -> int:
    return (floor(datetime.datetime.now().timestamp()) - 1) * 1000


async def generator(x: int, y: int, last: int = 0, batch_size:int = 10) -> pd.DataFrame:
    while y > 0:
        temp = preparing_oh(x, min(batch_size, y))
        if last:
            indx, nil = check_last(temp, last)
            if nil:
                yield temp.iloc[:indx]
                return
        yield temp
        x = temp.publishDateTimestamp.iloc[-1] - 1
        y -= batch_size
        await asyncio.sleep(1)

async def check(df):
    """
    типо предобработка и отправка
    """
    df.rename(columns={"Time": "DATE", "id": "ID"},inplace=True)
    df.drop('publishDateTimestamp',axis=1,inplace=True)
    result = parser.Parser('rbc').update_data_frame(df)
    await client.main(result)
    # print(result)
    
    
async def main(x: int, y: int = 100, last: int = 0, batch_size = 10):
    background_tasks = set()
    async for i in generator(x,y,last,batch_size):
              task = asyncio.create_task(check(i))
              background_tasks.add(task)
              task.add_done_callback(background_tasks.discard)
    await asyncio.gather(*background_tasks)

asyncio.run(main(time_right_now(), 110))