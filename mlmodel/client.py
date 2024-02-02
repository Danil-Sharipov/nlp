import pandas as pd
import pickle
import asyncio
import grpc
from proto.pipe_pb2_grpc import PipeServerStub
from proto.pipe_pb2 import PipeRequest, PipeReply
from time import perf_counter
import logging
import configparser

logging.basicConfig(level=logging.INFO)

async def client(df):
    config = configparser.ConfigParser()
    config.readfp(open(r'server.config'))
    ip = config.get('Scrapping Section','ip')
    port = config.get('Scrapping Section','port')
    async with grpc.aio.insecure_channel(f'{ip}:{port}') as channel:
        stub = PipeServerStub(channel)
        start = perf_counter()

        res: PipeReply = await stub.inference(
            PipeRequest(new=[pickle.dumps(df, protocol=4)])
        )
        logging.info(f"res = {res.ans} in {1000*(perf_counter()-start):.2f} ms")


async def logger():
    from psutil import cpu_percent, virtual_memory
    while 1:
        logging.info(f'CPU: {cpu_percent()}')
        logging.info(f'Memory: {virtual_memory()}')
        await asyncio.sleep(10)

async def main(df):
    # await asyncio.gather(client(df),logger())
    await asyncio.gather(client(df))

# if __name__ == '__main__':
#     asyncio.run(main())

        