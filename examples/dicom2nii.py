import asyncio
import dicom2nifti
from pydicom import dcmread
from pydicom.tag import Tag
import os 
import pathlib
from pydcmq import *
import dicom2nifti.settings as settings

settings.disable_validate_orthogonal()
settings.disable_validate_slice_increment()
settings.enable_resampling()
settings.set_resample_spline_interpolation_order(1)
settings.set_resample_padding(-1024)

async def dcmhandler(channel, ds, uri, routing_key):
    print(f"dicom2nii: converting {uri} ({ds.SeriesDescription})")
    outdir = f"{os.environ['HOME']}/.dcmq/nii/{ds.StudyInstanceUID}"
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
                    print(f"dicom2nii: {os.path.join(uri, series.name)} ({refds.SeriesDescription}) is not a primary image")
                    continue
                outfile = os.path.join(outdir, refds.SeriesInstanceUID + ".nii")
                try:
                    indir = os.path.join(uri, series.name)
                    dicom2nifti.dicom_series_to_nifti(indir, outfile, reorient_nifti=True)
                    count += 1
                    await publish(channel, "stored.series.nii", refds, uri=outfile)
                except Exception as e:
                    print(f"dicom2nii: error converting {series.name} ({refds.SeriesDescription}): {e}")
                    continue
    if count>0:
        await publish(channel, "stored.study.nii", ds, uri=outdir)
        
if __name__ == '__main__':
    subscriber_loop(
        server="amqp://guest:guest@127.0.0.1/",
        queue="",
        methods=['stored.study'],
        dcmhandler=dcmhandler
    )