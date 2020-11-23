# pydcmq
This is a python library and examples to build a distributed system for processing DICOM data. The typical use case would be a radiologist who knows Python and wants to automatically do some analysis or transformations of images without having to remember all the software that is used to process the images before being able to start to write the report.

Please also have a look at the [poster pdf](poster.pdf) which was presented at ASNR 2020.

External dependencies you need to install when using the examples:
* [RabbitMQ](https://www.rabbitmq.com/)
* [DCMTK](https://dicom.offis.de/dcmtk.php.de)
* [GDCM](https://github.com/malaterre/GDCM)
* [ANTs](https://github.com/ANTsX/ANTs)
