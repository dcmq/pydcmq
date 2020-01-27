import asyncio
from dcmq import async_consumer

def dcmhandler(ds):
    pass
        
if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    loop.create_task(
        async_consumer(loop, 
            server="amqp://guest:guest@127.0.0.1/",
            methods=['stored.dicom.series'],
            dcmhandler=dcmhandler
        )
    )
    loop.run_forever()