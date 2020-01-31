import asyncio
from pydicom import dcmread
import os, sys
from pathlib import Path
from pydcmq import publish_study_generator


path = ""

if os.path.exists(sys.argv[1]):
    path = os.path.abspath(sys.argv[1])
else:
    print("Cannot find " + sys.argv[1])
    exit()


def dcm_walk(dirpath):
    p = Path(dirpath)
    if not p.is_dir():
        return
    for dirName, subdirList, fileList in os.walk(p):
        for fname in fileList:
            fpath = Path(dirName) / fname
            try:
                ds = dcmread(str(fpath), stop_before_pixels=True)
                yield (ds, fpath)
            except Exception as e:
                print(e)
                print(f"failed reading {fpath}")

if __name__ == "__main__":
    publish_study_generator("amqp://guest:guest@127.0.0.1/", dcm_walk(path))