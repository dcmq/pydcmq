from pydcmq import *
from pydicom.dataset import Dataset
from datetime import datetime, timedelta

studyseries = {} #{studyuid: {study: studyds, series: {seriesid: seriesds, ..}}}
download_started = {} #{studyuid: Bool}

def check_series(series):
# check for completed study by assuring the newest series is at least 10 minutes old
# check for 1. MRI, 2. Head, 3. Neurorad
    if len(series) == 0:
        print("check_series: zero series in study")
        return False
    now = datetime.now()
    for seriesUID in series:
        ds = series[seriesUID]
        if not ds.Modality in ["CT"]:
            print(f"check_series: Modality {ds.Modality} not supported")
            return False
        if not ds.InstitutionName in ["www.neuroradiologie-mannheim.de"]:
            print(f"check_series: InstitutionName {ds.InstitutionName} not supported")
            return False
        if not ds.BodyPartExamined in ["HEAD", "BRAIN"]:
            print(f"check_series: BodyPartExamined {ds.BodyPartExamined} not supported")
            return False
        dstimestr = ds.SeriesDate + ds.SeriesTime
        dstimestr = dstimestr.split('.')[0]
        dstime = datetime.strptime(dstimestr, "%Y%m%d%H%M%S")
        if now - dstime < timedelta(minutes=10):
            print("check_series: series too fresh, there might be more coming?")
            return False
    return True

async def dcmhandler(channel, ds, uri, method):
    if not ds.StudyInstanceUID in studyseries.keys():
        studyseries[ds.StudyInstanceUID] = {'study': ds, 'series':{}}
    if not ds.StudyInstanceUID in download_started.keys():
        download_started[ds.StudyInstanceUID] = False
    if method == "found.series":
        if not ds.SeriesInstanceUID in studyseries[ds.StudyInstanceUID]['series'].keys():
            studyseries[ds.StudyInstanceUID]['series'][ds.SeriesInstanceUID] = ds
    elif method == "found.study.series":
        if not download_started[ds.StudyInstanceUID] and check_series(studyseries[ds.StudyInstanceUID]['series']):
            print(f"starting download for study {ds.StudyInstanceUID}")
            download_started[ds.StudyInstanceUID] = True
            await publish(channel, 'get.study', ds)

if __name__ == '__main__':
    subscriber_loop(
        server="amqp://guest:guest@127.0.0.1/",
        queue="",
        methods=[
            'found.series',
            'found.study.series',
        ],
        dcmhandler=dcmhandler
    )