from pydicom.multival import MultiValue
from pydicom.charset import convert_encodings
from pydicom import Dataset, DataElement, Sequence, dcmwrite, dcmread
from pydicom.tag import Tag
from pydicom.filebase import DicomBytesIO
from pydicom.datadict import tag_for_keyword, dictionary_VR
import os
from pathlib import Path
import errno
import json
import aiofiles
from pynetdicom import (
    PYNETDICOM_IMPLEMENTATION_UID,
    PYNETDICOM_IMPLEMENTATION_VERSION
)

def getFilename(ds):
    filename = str(Path.home()) + "/.dimseweb/master/" + ds.StudyInstanceUID
    if "SeriesInstanceUID" in ds and len(ds.SeriesInstanceUID)>0:
        filename += "/" + ds.SeriesInstanceUID
    if "SOPInstanceUID" in ds and len(ds.SOPInstanceUID)>0:
        filename += "/" + ds.SOPInstanceUID
    return filename

async def readFileBinary(filename, ds):
    async with aiofiles.open(filename, mode='rb') as f:
        return await f.read()
        
async def writeFile(filename, ds, data=None):
    if not os.path.exists(os.path.dirname(filename)):
        try:
            os.makedirs(os.path.dirname(filename))
        except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise
    async with aiofiles.open(filename, mode='wb') as f:
        if data != None:
            await f.write(data)
        else:
            await f.write(datasetToBinary(ds))

async def readFile(filename):
    with DicomBytesIO() as dcmfile:
        async with aiofiles.open(filename, mode='rb') as f:
            dcmfile.write(await f.read())
            dcmfile.seek(0)
        ds = dcmread(dcmfile)
    return ds

def filterBinary(data):
    ds = datasetFromBinary(data, skipPixel = True)
    if ds == None:
        return b''
    for key in list(ds.keys()):
        try:
            if ds[key].VR in ['OW', 'OB', 'OD', 'OF', 'OL', 'UN', 'OB or OW', 'US or OW', 'US or SS or OW']:
                del ds[key]
        except Exception as e: 
            print(e)
    smalldata = datasetToBinary(ds)
    return smalldata, ds

def dataElementToValue(dataElement, datasetencoding, subvalue=None):
    if subvalue != None:
        value = subvalue
    else:
        value = dataElement.value
    if value == '':
        return None
    if type(value) in [str, int, float, list]:
        return value
    if type(value) == bytes:
        pythonencoding = convert_encodings([datasetencoding])
        return value.decode(pythonencoding[0])
    if type(value) == MultiValue:
        return [dataElementToValue(dataElement, datasetencoding, subvalue) for subvalue in value]
    if dataElement.VR == 'PN':
        if len(str(value))>0:
            return {'Alphabetic': str(value)}
        return None
    if dataElement.VR == 'DA':
        return [value.isoformat()]
    if dataElement.VR == 'IS':
        return int(value)
    if dataElement.VR == 'UI' or dataElement.VR == 'AT':
        return str(value)
    if dataElement.VR == 'SQ':
        return [datasetToJSON(subvalue, datasetencoding) for subvalue in value]
    return value.original_string

def fix_meta_info(ds):
    try:
        ds.file_meta
    except:
        meta = Dataset()
        meta.ImplementationClassUID = PYNETDICOM_IMPLEMENTATION_UID
        meta.ImplementationVersionName = PYNETDICOM_IMPLEMENTATION_VERSION
        try:
            meta.MediaStorageSOPClassUID = ds.SOPClassUID
        except:
            meta.MediaStorageSOPClassUID = '1.2.276.0.7230010.3.1.0.1'
        try:
            meta.MediaStorageSOPInstanceUID = ds.SOPInstanceUID
        except:
            meta.MediaStorageSOPInstanceUID = '1.2.276.0.7230010.3.1.4.8323329.10856.1560082132.76381'
        meta.TransferSyntaxUID = "1.2.840.10008.1.2.1"
        ds.file_meta = meta
        ds.is_little_endian = True
        ds.is_implicit_VR = False
        ds.fix_meta_info()

def datasetToBinary(ds: Dataset):
    fix_meta_info(ds)

    with DicomBytesIO() as dcmfile:
        try:
            dcmwrite(dcmfile, ds, write_like_original=False)
        except Exception as e:
            print(e)
            #import IPython; IPython.embed()
            return None
        dcmfile.seek(0)
        return dcmfile.read()

def datasetFromBinary(data, specific_tags=None, skipPixel = False):
    try:
        return dcmread(DicomBytesIO(data), specific_tags=specific_tags, stop_before_pixels = skipPixel, force=True)
    except Exception as e:
        print(e)
        #import IPython; IPython.embed()
        return None

def datasetToJSON(ds : Dataset, datasetencoding = 'ISO_IR 6', filter_dataset = None, skipBinary = True, skipPixel = True):
    #return json.loads(ds.to_json())
    retval = {}
    if '00080005' in ds:
        datasetencoding = ds['00080005'].value
    if filter_dataset != None:
        keys = set(filter_dataset.keys()).intersection(set(ds.keys()))
    else:
        keys = ds.keys()
    for key in keys:
        keystr = "%08X" % key
        if skipPixel and keystr == '7FE00010': # skip pixel data
            retval[keystr] = {
                'vr': 'OW',
                'BulkDataURI': 'http://localhost:8088/rs/studies/'+ds.StudyInstanceUID+'/series/'+ds.SeriesInstanceUID+'/instances/'+ds.SOPInstanceUID
            }
            continue
        if skipBinary:
            try:
                if ds[key].VR in ['OW', 'OB', 'OD', 'OF', 'OL', 'UN', 'OB or OW', 'US or OW', 'US or SS or OW']: # skip vendor specific data
                    continue
            except Exception as e:
                print(e)
        retval[keystr] = {
            'vr': ds[key].VR,
        }
        value = dataElementToValue(ds[key], datasetencoding)
        if value != None:
            if type(value) != list:
                value = [value]
            retval[keystr]['Value'] = value
    return retval

def datasetFromJSON(data : dict):
    #return Dataset.from_json(data)
    ds = Dataset()
    for key in data.keys():
        tag = Tag(key)
        try:
            if 'Value' in data[key].keys():
                if data[key]['vr'] == 'SQ':
                    tempds = []
                    for subdata in data[key]['Value']:
                        tempds.append(datasetFromJSON(subdata))
                    seq = Sequence(tempds)
                    ds[key] = DataElement(tag, data[key]['vr'], seq)
                elif type(data[key]['Value'][0]) == dict and 'Alphabetic' in data[key]['Value'][0].keys():
                    ds[key] = DataElement(tag, data[key]['vr'], data[key]['Value'][0]['Alphabetic'])
                else:
                    if len(data[key]['Value'])>1:
                        ds[key] = DataElement(tag, data[key]['vr'], data[key]['Value'])
                    else:
                        ds[key] = DataElement(tag, data[key]['vr'], data[key]['Value'][0])
            else:
                ds[key] = DataElement(tag, data[key]['vr'], '')
        except:
            from IPython import embed; embed()
    return ds
