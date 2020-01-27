import asyncio
from dcmq import consumer_loop

def dcmhandler(ds, uri):
    pass
        
if __name__ == '__main__':
    consumer_loop(
        server="amqp://guest:guest@127.0.0.1/",
        methods=['stored.dicom.series'],
        dcmhandler=dcmhandler
    )