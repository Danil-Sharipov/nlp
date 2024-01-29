import pandas as pd
import pickle
import asyncio
import grpc
from pipe_pb2_grpc import PipeServerStub
from pipe_pb2 import PipeRequest, PipeReply
from time import perf_counter
import logging
import configparser

logging.basicConfig(level=logging.INFO)


async def get_data():
    """
    Дописать надо
    """
    df = pd.DataFrame({'A':[1,2,3],'B':[4,5,6]})
    my_bytes = pickle.dumps(df, protocol=4)
    yield my_bytes
    

async def client():
    config = configparser.ConfigParser()
    config.readfp(open(r'server.config'))
    ip = config.get('Scrapping Section','ip')
    port = config.get('Scrapping Section','port')
    async with grpc.aio.insecure_channel(f'{ip}:{port}') as channel:
        stub = PipeServerStub(channel)
        async for i in get_data():
            start = perf_counter()
            
            res: PipeReply = await stub.inference(
                PipeRequest(new=[i])
            )
            logging.info(f"res = {res.ans} in {1000*(perf_counter()-start):.2f} ms")
        
        
async def logger():
    from psutil import cpu_percent, virtual_memory
    while 1:
        logging.info(f'CPU: {cpu_percent()}')
        logging.info(f'Memory: {virtual_memory()}')
        await asyncio.sleep(10)

async def main():
    await asyncio.gather(client(),logger())
    
if __name__ == '__main__':
    asyncio.run(main())




if __name__ == '__main__':
    asyncio.run(main())


        