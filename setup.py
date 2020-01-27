import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pydcmq", # Replace with your own username
    version="0.0.1",
    author="Victor Saase",
    author_email="vsaase@gmail.com",
    description="Package to interface with the dcmq DICOM message queue",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/dcmq/pydcmq",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache License 2.0",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=[
        "pydicom",
        "pynetdicom",
        "aiofiles",
        "aio_pika"
    ]
)