DL Weather Test

This is a demo Spark application for the Weather ETL data pipeline. The general workflow pattern is modelled around the lambda batch processing framework.
The workflow consists of four main stages, mainly consisting of file-based operations. I have tried to demonstrate various methods and techniques 
defined around data pre-processing, that can be scaled for operations in cloud-based storage systems such as data lakes/blobs etc. Apart from the I/O operations, Spark will 
act as the orchestrator for the pipeline build, in which I have included data quality and validation checks, as well as a unit testing component.
The ETL Workflow includes the following stages:

- Source-->Ingestion
1) Move files from source to ingestion storage/dir- 
The files at this stage will not be modified or transformed - so essentially a data dump. 
Typically, the files tend to land on a SFTP server, from which they are copied.
- Ingestion-->Raw
1) Move files from Ingestion to Raw- this is essentially a replica of the ingestion stage, 
however here I have tried to demonstrate with pysaprk how filepath partitioning can be provisioned 
in cloud storage systems such as S3/Azure data lake. A typical approach that's taken 
for file path partitioning is via date extraction from filename.
2) With the assumption that the weather file is a monthly import, I have used basic os 
functions/methods to extract the date from the filename to create directories in the 
raw folder (storage). The structure of the filepath is based 
on project--> module -->yyyy-->mm-->file format.
- Raw-->Clean
1) In the Clean stage, I will mostly be doing conducting data quality checks, as well as some data validation checks.Please note that some of these checks can be added to the unit tests, however, I have included them as a part of the workflow.
The purpose of the cleaning stage is to ensure that the data is of acceptable quality before loading it into the your chosen platform (i.e. DW, analytics engine). This stage is necessary in that if you were using external tables, the restrictions are quite rigid, with data types and nullable property etc.
The tests/checks have been wrapped in try catch blocks, to ensure all errors are captured. However, no logging has been defined. 
- Clean-->Load
Unlike the traditional loading into the DW table or DL, I have decided to load the data into a pandas dataframe, for querying purposes.

This project is for demonstration purposes only. The workflow includes multiple approaches, for multi-dimensional
processing and would require a strategic approach on standardizing the method implemented (i.e. filepath partitioning). 
Please note that, in local mode, the spark master and executors (slaves) run locally 
within a single JVM, thus while the local build is appropriate for a small workload, in real world scenarios, 
you would probably have to define runtime properties for a more scalable approach.

Assumptions/Justification

Based on the requirements, it was my understanding that there was a focus on the pre-processing of data rather than the substantial analysis. 
From an engineering perspective, my focus was building a data pipeline that replicated various stages of an actual ETL process, which can 
potentially be implemented/scaled to a cloud storage system. I have tried to demonstrate various methods and techniques for data extraction 
and processing using PySpark’s dataframe API, as well as conversion between both PySpark and pandas to complete various tasks.  
I had debated whether I should do exploratory analysis, more on the ML end, however, determined that it was outside of scope 
(you need some sort of a hypothesis to even begin). Additionally, I was considering look at ML engineering framework with pykafka 
but thought it too comprehensive for the tasks (required segmentation split into json).I have included a .ipynb notebook for visual output purposes.

Prerequisites

To deploy this project locally, you will need the following installed on your os.

- Setup environment
1) Install Spark --v > 1.2
3) Install Hadoop WinUtils --https://github.com/steveloughran/winutils
4) Install Scala Distribution --v2.12.4 (msi)
5) Install Python distribution --Native/Python 3.6.4 :: Anaconda, Inc.
6) Install Java 8 jdk
- add system variables for path of your executables

- IDE setup- add dependencies from spark python and python/bin

- set up virtual env
Create virtual environment (Use either virtualevn or virtualenvwrapper- depending on preference)

- pip install -r requirements.txt

- code changes
1) Define root/dir locations to store your output files- the program has been written
dynamically to handle all file processing through to the load stage with the usage of variables.
2) Change list option in the cleaning stage to test for different transformations etc.

- Other 
1) EDE: Pycharm/Ipython notebook
2) Time Taken: 2 hours approx.
3) Average Runtime: 58 secs

Running the tests

- The projects includes a basic test suite running a single test on a dataframe. The testing should
demonstrate the ability to switch between pyspark and pandas when using the standard unit test module.
- Logging has been enables so all output of the tests are saved in the home dir of the project under the logging folder.

Deployment

Not Applicable

Built With

- Apache Spark - MPP engine
- Hdfs - file system utilities
- Python packages PySpark- Python API for Spark
- Pandas- python package

Versioning

I used git for version control

Authors

Maleeha Tahir





