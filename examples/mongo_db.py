from pydicom import dcmread, dcmwrite
from pydicom.errors import InvalidDicomError
from pydicom.misc import is_dicom
import os
import json
from pydicom.filebase import DicomBytesIO
import io
import pandas as pd
from pymongo import DESCENDING
import asyncio
from motor import motor_asyncio
import re
from pydcmq.util import datasetToJSON, datasetFromBinary, datasetToBinary, datasetFromJSON, writeFile, filterBinary, getFilename
from pydcmq import *

class MongoDicomDB(object):
    def __init__(self, loop):
        self.client = motor_asyncio.AsyncIOMotorClient(io_loop=loop)
        self.db = self.client.test_database
        self.datasets = self.db.datasets
        self.datasets.create_index([("SOPInstanceUID", DESCENDING)],
                                background=True,
                                unique=True)
        self.datasets.create_index([("SeriesInstanceUID", DESCENDING)],
                                background=True)
        self.datasets.create_index([("StudyInstanceUID", DESCENDING)],
                                background=True)

    async def addDataset(self, ds, data=None, uri=""):
        await self.datasets.update_one({
            'SOPInstanceUID': ds.SOPInstanceUID,
        },{
            '$set': {
                'json': datasetToJSON(ds),
                'binary': datasetToBinary(ds),
                'uri': uri
            },
            '$setOnInsert': {
                'SOPInstanceUID': ds.SOPInstanceUID,
                'SeriesInstanceUID': ds.SeriesInstanceUID,
                'StudyInstanceUID': ds.StudyInstanceUID,
                'SOPClassUID': ds.SOPClassUID
            }
        }, upsert=True)
        return True

    async def getInstance(self, ds):
        res = await self.datasets.find_one({'SOPInstanceUID': ds.SOPInstanceUID})
        if res:
            return res['binary'], res['uri']
        return None

    async def hasStudy(self, ds):
        res = await self.datasets.find_one({'StudyInstanceUID': ds.StudyInstanceUID})
        if res:
            return True
        return False

    async def getStudy(self, ds):
        async for res in self.datasets.find({'StudyInstanceUID': ds.StudyInstanceUID}):
            yield res['binary'], res['uri']

    async def getSeries(self, ds):
        async for res in self.datasets.find({
                'StudyInstanceUID': ds.StudyInstanceUID,
                'SeriesInstanceUID': ds.SeriesInstanceUID,
            }):
            yield res['binary'], res['uri']

    async def findInstances(self, ds, limit=0,):
        query = {}
        if 'StudyInstanceUID' in ds and len(ds.StudyInstanceUID)>0:
            query['StudyInstanceUID'] = ds.StudyInstanceUID
        if 'SeriesInstanceUID' in ds and len(ds.SeriesInstanceUID)>0:
            query['SeriesInstanceUID'] = ds.StudyInstanceUID
        if 'Modality' in ds and len(ds.Modality)>0:
            query['json.00080060.Value.0'] = ds.Modality
        if 'PatientName' in ds:
            query['json.00100010.Value.0.Alphabetic'] = {'$regex': '^' + re.escape(str(ds.PatientName))}
        if 'PatientID' in ds and len(ds.PatientID)>0:
            query['json.00100020.Value.0'] = ds.PatientID
        async for res in self.datasets.find(query):
            yield res['binary'], res['uri']

    async def findSeries(self, ds, limit=0,):
        query = {}
        if 'StudyInstanceUID' in ds and len(ds.StudyInstanceUID)>0:
            query['StudyInstanceUID'] = ds.StudyInstanceUID
        if 'Modality' in ds and len(ds.Modality)>0:
            query['json.00080060.Value.0'] = ds.Modality
        pipeline = [{
            "$match": query
        },{
            "$group": {
                "_id": "$SeriesInstanceUID", 
                "json": { "$first": "$json"}, 
                "binary": { "$first": "$binary"}, 
                "uri": { "$first": "$uri"}, 
                "SeriesInstanceUID": { "$first": "$SeriesInstanceUID"}, 
                "StudyInstanceUID": { "$first": "$StudyInstanceUID"}, 
                }
        }]
        results2 = self.datasets.aggregate(pipeline)
        async for res in results2:
            yield res['binary'], res['uri']
    
    async def findStudies(self, ds, limit=0):
        query = {}
        if 'StudyInstanceUID' in ds and ds.StudyInstanceUID != '':
            query['StudyInstanceUID'] = ds.StudyInstanceUID
        if 'PatientName' in ds:
            query['json.00100010.Value.0.Alphabetic'] = {'$regex': '^' + re.escape(str(ds.PatientName))}
        if 'Modality' in ds:
            query['json.00080060.Value.0'] = ds.Modality
        pipeline = [{
            "$match": query
        },{
            "$group": {
                "_id": "$StudyInstanceUID", 
                "json": { "$first": "$json"}, 
                "binary": { "$first": "$binary"}, 
                "uri": { "$first": "$uri"}, 
                "StudyInstanceUID": { "$first": "$StudyInstanceUID"}
                }
        }]
        results2 = self.datasets.aggregate(pipeline)
        async for res in results2:
            yield res['binary'], res['uri']
    

async def dcmhandler(channel, ds, uri, routing_key):
        method = routing_key
        if method == "get.reports":
            retlist = dicom_db.findInstances(ds)
            async for data, uri in retlist:
                await publish(channel, "got.report", datasetFromBinary(data), uri=uri, data=data)
        elif method == 'find.studies':
            retlist = dicom_db.findStudies(ds)
            async for data, uri in retlist:
                await publish(channel, "found.study", datasetFromBinary(data), uri=uri, data=data)
        elif method == 'find.series':
            retlist = dicom_db.findSeries(ds)
            async for data, uri in retlist:
                await publish(channel, "found.series", datasetFromBinary(data), uri=uri, data=data)
        elif method == 'find.instances':
            retlist = dicom_db.findInstances(ds)
            async for data, uri in retlist:
                await publish(channel, "found.instance", datasetFromBinary(data), uri=uri, data=data)
        elif method == 'get.study':
            retlist = dicom_db.getStudy(ds)
        elif method == 'get.instance':
            retlist = dicom_db.getInstance(ds)
        elif method == 'stored.instance':
            await dicom_db.addDataset(ds, uri=uri)
            return
        else:
            return

        
if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    dicom_db = MongoDicomDB(loop)
    subscriber_loop(
        server="amqp://guest:guest@127.0.0.1/",
        queue="",
        methods=[
            #'find.studies',
            #'find.series',
            #'get.*',
            'stored.instance',
            'get.reports'
        ],
        dcmhandler=dcmhandler,
        loop = loop
    )
    loop.run_forever()