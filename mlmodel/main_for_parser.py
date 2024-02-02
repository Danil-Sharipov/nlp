import requests
import pandas as pd
import datetime
from math import floor
from time import sleep
import asyncio
import parser as pr
import client
import argparse

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

# from deque import deque
# ls = deque()
# df = None
# setting = set()
# async def concat():
#     if len(ls) > 1:
#         if df is None:
#             df = ls.popleft()
#         else:
#             df = pd.concat([df,ls.popleft()], ignore_index=True)


async def check(df):
    """
    типо предобработка и отправка
    """
    df.rename(columns={"Time": "DATE", "id": "ID"},inplace=True)
    df.drop('publishDateTimestamp',axis=1,inplace=True)
    result = pr.Parser('rbc').update_data_frame(df)
    # ls.append(result)
    # task = asyncio.create_task(concat())
    # setting.add(task)
    # task.add_done_callback(setting.discard)
    await client.main(result)
    
    
async def main(x: int, y: int = 100, last: int = 0, batch_size = 10):
    background_tasks = set()
    logger = asyncio.create_task(client.logger())
    async for i in generator(x,y,last,batch_size):
              task = asyncio.create_task(check(i))
              background_tasks.add(task)
              task.add_done_callback(background_tasks.discard)
    await asyncio.gather(*background_tasks)
    await logger

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='command',help='List of commands',required = True)

    # send (start: int = time_right_now(), count: int: 100, end: int = None)
    send_parser = subparsers.add_parser('send',help='Send news after parsing to server')

    send_parser.add_argument(
        '-s',
        '--start',
        action = 'store',
        help = 'Start datetime(default right now)',
        default = time_right_now(),
        type = int
    )

    send_parser.add_argument(
        '-c',
        '--count',
        action = 'store',
        help = 'Count of data(default 100)',
        default = 100,
        type = int
    )

    send_parser.add_argument(
        '-b',
        '--batch_size',
        action = 'store',
        help = 'batch_size(maximum 100, default 10)',
        default = 10
    )

    send_parser.add_argument(
        '-e',
        '--end',
        action = 'store',
        help = 'End datetime(not required)',
        default = None
    )
    # it
    it = subparsers.add_parser('it',help='Send news after parsing to server')


    args = parser.parse_args()
    if args.command == 'send':
        if args.end is not None:
            asyncio.run(
                main(
                    x = args.start,
                    y = 1e8,
                    batch_size=args.batch_size,
                    last = args.end
                )
            )
        else:
            args.batch_size = min(100, args.batch_size)
            if args.count < args.batch_size:
                raise Exception('The value of count must be greater than or equal to batch_size')
            asyncio.run(
                main(
                    x = args.start,
                    y = args.count,
                    batch_size=args.batch_size
                )
            )

