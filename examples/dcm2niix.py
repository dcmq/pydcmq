import asyncio
from pydicom import dcmread
from pydicom.tag import Tag
import dicom2nifti
import os 
import pathlib
from pydcmq import *
import dicom2nifti.settings as settings

async def dcmhandler(channel, ds, uri, routing_key):
    print(f"dcm2niix: converting {uri} ({ds.SeriesDescription})")
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
                    print(f"dcm2niix: {os.path.join(uri, series.name)} ({refds.SeriesDescription}) is not a primary image")
                    continue
                outfile = os.path.join(outdir, refds.SeriesInstanceUID + ".nii")
                try:
                    indir = os.path.join(uri, series.name)
                    cmd = f"dcm2niix -f %j -o {outdir} -b n -m y {indir}"
                    print(cmd)
                    ret = os.system(cmd)
                    if ret == 0:
                        if os.path.exists(os.path.join(outdir, refds.SeriesInstanceUID + "_Tilt_1.nii")):
                            outfile = os.path.join(outdir, refds.SeriesInstanceUID + "_Tilt_1.nii")
                        if os.path.exists(os.path.join(outdir, refds.SeriesInstanceUID + "_Tilt_Eq_1.nii")):
                            outfile = os.path.join(outdir, refds.SeriesInstanceUID + "_Tilt_Eq_1.nii")
                        if os.path.exists(outfile):
                            await publish_nifti(channel, refds, outfile)
                            count += 1
                        else:
                            print(f"dcm2niix: error converting {series.name} ({refds.SeriesDescription}): outfile not found")
                    else:
                        print(f"dcm2niix: error converting {series.name} ({refds.SeriesDescription}): exited with error code {ret}")
                        print(f"dcm2niix: retrying with dicom2nifti")
                        dicom2nifti.dicom_series_to_nifti(indir, outfile, reorient_nifti=True)
                        count += 1
                        await publish_nifti(channel, refds, outfile)
                except Exception as e:
                    print(f"dcm2niix: error converting {series.name} ({refds.SeriesDescription}): {e}")
                    continue
    if count>0:
        await publish(channel, "stored.series.nii", ds, uri=outdir)
        
if __name__ == '__main__':
    subscriber_loop(
        server="amqp://guest:guest@127.0.0.1/",
        queue="dicom2nii",
        methods=['stored.study'],
        dcmhandler=dcmhandler
    )