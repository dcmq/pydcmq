import asyncio
from pydicom import dcmread
from pydicom.tag import Tag
import os 
import pathlib
from pydcmq import consumer_loop, publish_nifti, publish_nifti_study, async_consumer

async def dcmhandler(channel, ds, uri):
    print(f"dicom2nii: converting {uri} ({ds.SeriesDescription})")
    outdir = f"{os.environ['HOME']}/.dimseweb/nii/{ds.StudyInstanceUID}"
    pathlib.Path(outdir).mkdir(parents=True, exist_ok=True)
    count = 0
    with os.scandir(uri) as it:
        for series in it: 
            if series.is_dir():
                with os.scandir(series) as it2: 
                    for instance in it2:
                        if instance.is_file():
                            dcmfilename = instance.name
                            break
                refds = dcmread(os.path.join(uri, series.name, dcmfilename))
                if not Tag("ImageType") in refds or not "PRIMARY" in refds.ImageType: #only convert primary data
                    print(f"dcm2niix: {os.path.join(uri, series.name)} ({refds.SeriesDescription}) is not a primary image")
                    continue
                outfile = os.path.join(outdir, refds.SeriesInstanceUID + ".nii")
                try:
                    indir = os.path.join(uri, series.name)
                    ret = os.system(f"dcm2niix -f %j -o {outdir} -b n {indir}")
                    if ret == 0:
                        await publish_nifti(channel, refds, outfile)
                        count += 1
                except Exception as e:
                    print(f"dcm2niix: error converting {series.name} ({refds.SeriesDescription}): {e}")
                    continue
    if count>0:
        await publish_nifti_study(channel, ds, outdir)
        
if __name__ == '__main__':
    consumer_loop(
        server="amqp://guest:guest@127.0.0.1/",
        queue="dicom2nii",
        methods=['stored.study'],
        dcmhandler=dcmhandler
    )