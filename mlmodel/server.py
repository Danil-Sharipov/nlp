import asyncio
import grpc
from io import BytesIO
from pipe_pb2_grpc import PipeServer, add_PipeServerServicer_to_server
from pipe_pb2 import PipeRequest, PipeReply
import logging
from time import perf_counter
import configparser
import pickle

logging.basicConfig(level=logging.INFO)

class PipeServer(PipeServer):
    def __init__(self):
        self.__counter = 0
        
    @staticmethod
    def answer(new):
        """
        Дописать надо
        """
        print(pickle.loads(new[0]))
        return True
    async def inference(self, request: PipeRequest, context) -> PipeReply:
        """
        Основная логика получения данных
        """
        logging.info("New request")
        start = perf_counter() # запускаем счетчик
        ans = self.answer(request.new)
        logging.info(f'Done in {1000*(perf_counter()-start):.2f} ms')
        return PipeReply(ans=[ans])
    
async def serve():
    server = grpc.aio.server()
    add_PipeServerServicer_to_server(PipeServer(), server)
    config = configparser.ConfigParser()
    config.readfp(open(r'server.config'))
    ip = config.get('Server Section','ip')
    port = config.get('Server Section','port')
    server.add_insecure_port((temp:=f'{ip}:{port}'))
    logging.info(f"Server started {temp}")
    await server.start()
    await server.wait_for_termination()
    
async def logger():
    from psutil import cpu_percent, virtual_memory
    while 1:
        logging.info(f'CPU: {cpu_percent()}')
        logging.info(f'Memory: {virtual_memory()}')
        await asyncio.sleep(10)
    
async def main():
    await asyncio.gather(serve(),logger())
    
if __name__ == '__main__':
    asyncio.run(main())
    
        
        