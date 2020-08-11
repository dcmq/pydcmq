import argparse
import asyncio
from pydicom import dcmread
from pydicom.dataset import Dataset
from pydcmq import *
import time
from datetime import datetime, timedelta

server = "amqp://guest:guest@127.0.0.1/"

def find_loop_generator(args):
    while True:
        now = datetime.now() # current date and time
        onehourago = now - timedelta(minutes=1*60)
        bodyparts = ['HEAD','BRAIN']
        for modality in args.modalities.split(","):
            for bodypart in bodyparts:
                ds = Dataset()
                ds.Modality = modality
                ds.StudyDate = onehourago.strftime("%Y%m%d") + '-'
                ds.StudyTime = onehourago.strftime("%H%M%S") + '-'
                ds.SeriesDate = ''
                ds.SeriesTime = ''
                ds.InstitutionName = 'www.neuroradiologie-mannheim.de'
                ds.BodyPartExamined = bodypart
                yield ds
        time.sleep(60)


async def async_publish_find(server, generator):
    loop = asyncio.get_running_loop()
    connection = await connect_robust(server, loop=loop)
    async with connection:
        print(f"dcmq: connected to {server}")
        channel = await connection.channel()
        for ds in generator:
            await publish(channel, "find.series", ds)

        
if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="parse args")

    parser.add_argument('-m','--modalities', default="MR,CT", type=str, help='comma separated list of modalities to query')

    args = parser.parse_args()
    print(args)

    loop = asyncio.new_event_loop()
    loop.run_until_complete(
        async_publish_find( 
            server=server,
            generator=find_loop_generator(args)
        )
    )
    loop.close()
    