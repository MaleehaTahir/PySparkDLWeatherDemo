from ETLWorkflow.init import sc
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from datetime import datetime
import pandas as pd
import os
import zipfile

"""
This example uses relative paths to access directories in the project root folder named 'resources'.
This method is not recommended as paths should be saved under variable names to manage dependencies between
different environments. However, for demonstration purposes, I have included the resources folder, to reflect
partitioning in local file system 
"""

zipped_dir = '../resources/zippedDir/Data Engineer Test.zip'
source_dir = '../resources/sourceFiles'
base_dir = '../resources/baseDir/'
raw_dir = 'raw/weather/'
clean_dir = 'clean/weather/'
temp_dir = 'tempdir/'

tempFiles, extracted_date =[], []
df_dict = {}

# extracting data from the zipped folder into the ingestion directory
with zipfile.ZipFile(zipped_dir, "r") as zipObj:
    try:
        FileList = zipObj.namelist()
        for file in FileList:
            if file.startswith('weather'):
                zipObj.extract(file, source_dir)
    except OSError as o:
        print(str(o))
        raise o


# ok, lets read the csv files from source and put them in the ingestion folder
def split_file_date(sourcedir):
    """
    :param sourcedir: path to source dir
    :return: This function is specifically used to extract the date from the
    filenames and converting them to date format. The extracted
    dates are then appended to a list, which is then returned from the
    function.
    """
    import glob
    file_list = []
    try:
        for root, dirs, files in os.walk(sourcedir):
            file_list += glob.glob(os.path.relpath(os.path.join(root, '*.csv')))
            for i in file_list:
                get_date = i.split(".", 8)[-2]
                convert_to_date = datetime.strptime(get_date, '%Y%m%d').strftime('%Y/%m/%d').replace('/', '\\')
                extracted_date.append(convert_to_date.replace('\\', '/'))
    except OSError as e:
        print(str(e))
        raise e

    return file_list


# the for loop uses python's built-in enumerate function, with the udf defined above to create a dicitonary,
# which stores the name of the df associated with file that's being loaded into it.
for f, df in enumerate(split_file_date(source_dir)):
    df_dict['df_' + str(df).split(".", 8)[-2]] = sc.read.csv(df, header=True)

# another for-loop is then used to iterate over the dict, from which the df is extracted with it's associated name.
# Remember, the name of the df has been extracted from the date port of the filename in form of date so df_20160201
for i, j in df_dict.items():
    try:
        date_split = datetime.strptime(i.split("_")[1], '%Y%m%d').strftime('%Y/%m/%d')
        base_path = base_dir + raw_dir
        full_path = base_path + date_split + '/' + temp_dir
        # ok, so spark by its nature as an MPP engine will partition files (partitions are dependent on the number of
        # of executors defined- each executor is responsible for a task), which is usefui for a large workload
        # however since, I am only working with a small dataset, I will merge all partitions into a single output.
        j.repartition(1).write.mode("overwrite").option("header", "true").parquet(full_path)

    except SystemError as e:
        print(str(e))
        raise e

# list comprehension to save the raw_dir file paths to a list
file_process_path = [base_dir+raw_dir + x for x in extracted_date]


# please note that the function below is very similar to the one used to manage files in
# databricks, with Azure data lake
def delete_from_temp(fullpath):
    try:
        for l in fullpath:
            fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
            file = fs.globStatus(sc._jvm.Path(l + '/' + temp_dir + 'part*'))[0].getPath().getName()
            fs.rename(sc._jvm.Path(l + '/' + temp_dir + file),
                      sc._jvm.Path(l + '/' + l[-19:].replace('/', '') + '.parquet'))
            fs.delete(sc._jvm.Path(l + '/' + temp_dir), True)
            fs.delete(sc._jvm.Path(l + '/' + '.' + l[-19:].replace('/', '') + '.parquet.crc'), True)
    except IOError as e:
        print("failed to delete the temp folder" + str(e))
        raise e

    return fullpath


ingestion_to_raw = delete_from_temp(file_process_path)

# ok lets load data and clean
# ok, so I have explicitly defined both a spark and sql context, primarily for spark df to pandas df conversion.
sqlCtx = SQLContext(sparkContext=sc.sparkContext, sparkSession=sc)

clean_df = sqlCtx.read.option("inferSchema", "true").parquet(*file_process_path)

# check to ensure that source file is not empty
try:
    if len(clean_df.head(1)) != 0:
        print("The source file is not empty")
except IOError as e:
    raise e


# row_count comparison between source and target files, to ensure that we haven't lost any data
# between the file conversion from csv to parquet
def get_row_count(path, file_format, file_type, input):
    """
    :param path: path to read file
    :param file_format: file extension i.e. '.csv' '.parquet'
    :param file_type: pyspark args; type of file
    :param input: boolean True/False header property
    :return: row count is saved in a list of dicts, which is then
    used to create a pandas & spark df. The function returns the
    spark df
    """
    new_row = []
    try:
        for i in path:
            for root, dirs, files in os.walk(i):
                for file in files:
                    if file.endswith(file_format):
                        f = os.path.join(root, file)
                        row_count = sc.read.option("header", input).format(file_type).load(f).count()
                        new_row.append({'filename': file, 'RowCount': row_count})
        row_df = pd.DataFrame(new_row)
        sp_df = sqlCtx.createDataFrame(row_df)
    except TypeError as t:
        print(str(t))
        raise AssertionError(t)

    return sp_df


def create_row_output(original_file, modified_file, source_file = [], target_file = []):
    """
    :param original_file: df for original file
    :param modified_file: df for modified file
    :param source_file: list of paths for source files
    :param target_file: list of paths for target files
    :return: The funciton from above is used to create the multiple source/target
    df's, which are then merged for comparison purposes.
    """
    try:
        df_original = get_row_count(*source_file).withColumn("Type", F.format_string(original_file))
        df_modified = get_row_count(*target_file).withColumn("Type", F.format_string(modified_file))
        combined = df_original.union(df_modified)
        cols = list(combined.columns)
        cols = [cols[-1]] + cols[:-1]
        rowComparisonDf = combined[cols]
    except LookupError as l:
        print(str(l))
        raise l

    return rowComparisonDf


# the functions above can be used to specify various formats for comparison purposes
array = []
csv_file_rows = ['.csv', 'csv', 'true']  # use as source/target
parquet_file_rows = ['.parquet', 'parquet', 'false']  # use as source/target
landing_path = [source_dir]
# initialise function
create_row_output('SourceFile', 'TargetFile', [landing_path, *csv_file_rows], [file_process_path, *parquet_file_rows])


# Data Type Check
# ok, typically when you infer schema from a pyspark dataframe, spark automatically gives you the best-fit data types
# for the cols. However, if you want to custom specify a schema, you can use the approach below. A benefit for the below
# approach is that you might have, through your pre-processing data validation stage identified data types most suited
# to your dataset. Additionally, you can pass in the lists below from various sources including as parameters
# to your pipeline.
to_int_cols = ['WindSpeed', 'WindDirection', 'WindGust', 'Pressure', 'SignificantWeatherCode']
to_long_cols = ['ForecastSiteCode', 'Visibility']
to_date_cols = ['ObservationDate']
to_double_cols = ['ScreenTemperature', 'Latitude', 'Longitude']
# the assumption was that time fields in the weather datasets were of int type, and required formatting to
# a time format
clean_df = clean_df.withColumn('ObservationTime', F.lpad(clean_df['ObservationTime'], 4, '0').substr(3, 4))
clean_df = clean_df.withColumn('ObservationTime', F.rpad(clean_df['ObservationTime'], 6, '0')).\
    withColumn("ObservationTime", (F.regexp_replace('ObservationTime',"""(\d\d)""", "$1:")).substr(0,8))

# clean_df.select('ObservationTime').distinct().show()
# using a cast function from spark to modify the data types
for col in clean_df.columns:
    try:
        if col in to_int_cols:
            clean_df = clean_df.withColumn(col, F.col(col).cast('int'))
        elif col in to_long_cols:
            clean_df = clean_df.withColumn(col, F.col(col).cast('long'))
        elif col in to_date_cols:
            clean_df = clean_df.withColumn(col, F.col(col).cast('date'))
        elif col in to_double_cols:
            clean_df = clean_df.withColumn(col, F.col(col).cast('double'))
        else:
            pass
    except AttributeError as ae:
        print(str(ae))
        raise ae

# handling NULLS
# It's good practice to handle Nulls and blanks in your dataset.
# Below, using list comprehension all categorical and continious fields have been added to lists
all_str_cols = [item[0] for item in clean_df.dtypes if item[1].startswith('string')]
all_continuous_cols =[item[0] for item in clean_df.dtypes if item[1].startswith(('int', 'long', 'double'))]
try:
    clean_df = clean_df.na.fill("Unknown", all_str_cols)
    clean_df = clean_df.na.fill(0, all_continuous_cols)
    default_time = '00:00:00'
    clean_df = clean_df.fillna({'ObservationTime': default_time})
except Exception as e:
    print(str(e))
    raise e


# ok, I am going to revert to pandas for a bit
# The observation date is converted into datetime, so we can set an index on the pandas dataframe,
# the date column is then formatted to a date from datetime
pd_df = clean_df.toPandas()
pd_df['ObservationDate'] = pd.to_datetime(pd_df['ObservationDate'], format='%Y-%m-%d')
pd_df['ObservedDate'] = pd_df['ObservationDate'].dt.date


# could've also used the pyspark approach defined above.
def create_file_paritions(file_prefix='weather'):
    """
    :param file_prefix: fixed value, however can be left empty
    :return: This function is used to create daily file partitions
    from date taken from the filename. This is a pandas approach,
    where we group data from the date column in the dataframe. The
    for-loop is then to used to write the files to target dir based
    on the distinct yyyy/mm/dd specified in the date column. This
    could also be adjusted for a higher level categorization i.e. YYYY/mm
    """
    partitioned_files = pd_df.groupby([pd_df.ObservationDate.dt.date])
    processed_df_list = []
    try:
        for (ObservationDate), group in partitioned_files:
            fname = file_prefix + ObservationDate.strftime('%Y%m%d')
            clean_process_path = base_dir + clean_dir + str(ObservationDate).replace('-', '/')
            group.iloc[:, 0:].to_parquet(clean_process_path + '/' + f'{fname}.parquet')
            # the files are then read back into a pandas df and df's ar appended to a list
            processed_df_list.append(pd.read_parquet(clean_process_path))
    except RuntimeError as re:
        print(str(re))
        raise re

    return processed_df_list

# the pandas concat function is used to merge dfs in the list into a single df
processed_df = pd.concat(create_file_paritions(), ignore_index=True)
processed_df.drop('ObservationDate', axis=1, inplace=True)
processed_df.rename(columns={'ObservedDate': 'ObservationDate'}, inplace=True)


# grouped data by date
group_temp_by_day = processed_df.groupby(['ObservationDate'], sort=False)['ScreenTemperature'].max()

# DL Queries
# first get the max screentemp
maxTemp = processed_df['ScreenTemperature'].max()
# second map the maxtemp var to date column in df to extract the associated date
date_of_max_temp = processed_df[processed_df['ScreenTemperature'] == maxTemp]['ObservationDate'].values[0]
# third, map the maxtemp var to region column to extract the associated date
region_of_max_temp = processed_df[processed_df['ScreenTemperature'] == maxTemp]['Region'].values[0]
# combining the above three vars into a single statement
filtered_max_temp = processed_df[(processed_df['ObservationDate'] == date_of_max_temp) &
                                 (processed_df['Region'] == region_of_max_temp) &
                                 (processed_df['ScreenTemperature'] == maxTemp)]
output = filtered_max_temp[['ObservationDate', 'Region', 'ScreenTemperature']]
print('Maximum Temperature on date of occurrence' + " " + "(" + str(date_of_max_temp) + ")" + ':' + " " + str(maxTemp))
print('Maximum Temperature was recorded in the region' + " " + str(region_of_max_temp) + ':' + " " + str(maxTemp))


# revert pandas dataframe to spark dataframe
processed_spark = sqlCtx.createDataFrame(processed_df)
processed_spark.printSchema()

# The spark sql context can be used to register a temp table (in-memory) to make sql queries.
# The output of both the python and sql queries should be identical (and well it is!!)
processed_spark = sqlCtx.createDataFrame(processed_df)
processed_spark.printSchema()

try:
    processed_spark.registerTempTable("Weather")
    sql_max_temp = sqlCtx.sql("select MAX(ScreenTemperature) from Weather")
    sql_max_dtemp_date = sqlCtx.sql("SELECT ObservationDate FROM Weather WHERE ScreenTemperature = "
                                    "(SELECT MAX(ScreenTemperature) FROM Weather)")
    sql_max_region = sqlCtx.sql("SELECT Region FROM Weather WHERE ScreenTemperature = "
                                "(SELECT MAX(ScreenTemperature) FROM Weather)")
    sql_max_aggreagted = sqlCtx.sql("SELECT ObservationDate, Region, ScreenTemperature "
                                    "FROM Weather WHERE ScreenTemperature = "
                                    "(SELECT MAX(ScreenTemperature) FROM Weather)")
    sql_max_aggreagted.show()
    sqlCtx.sql("DROP TABLE Weather")
except Exception as e:
    print(e)
    raise e


