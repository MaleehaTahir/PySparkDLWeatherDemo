from tests.numeric_cals import time_col_max
from tests.test_config import PySparkTest
from pandas.testing import assert_frame_equal
import logging
import numpy as np
import unittest

# setting up logging for the test and defining an output path to save log files (as text files)
# the tests were run for both failure and success instances to output logs
logging.basicConfig(level=logging.DEBUG, filename='C:\weather\\logging\\weatherloggingsucsess.txt', filemode='w')
logger = logging.getLogger()


# This is a dataframe test, where the test case is essentially to check whether the max value in
# the ObservationTime col
class SparkDfTest(PySparkTest):

    def test_data_frame(self):
        import pandas as pd

        csv_file_list = ["C:\DLTestFiles\\weather.20160201.csv", "C:\DLTestFiles\\weather.20160301.csv"]

        df_list = []
        for file in csv_file_list:
            df_list.append(pd.read_csv(file)) # pandas df to read csv files
        merged_weather_data =  pd.concat(df_list)
        logger.debug('created a merged pandas dataframe from source file')
        # merge files in a single dataframe
        for col in merged_weather_data.columns:
            if ((merged_weather_data[col].dtypes != np.int64) &
                    (merged_weather_data[col].dtypes != np.float64)):
                merged_weather_data[col] = merged_weather_data[col].fillna('')
        # create a spark dataframe and apply test function
        spark_df = self.spark.createDataFrame(merged_weather_data)
        # specify the time_col_max
        output_spark = time_col_max(spark_df)
        # since spark has sorting issues, the dataframe is reverted back into a pandas df to assert whether
        # the expected output is same as actual
        results_pandas = output_spark.toPandas()
        # the expected results should be none, there should be no rows where the rows exceed the max 24 hour
        # clock value
        expected_results = results_pandas[0:0]
        # Assert that the 2 results are the same. We’ll cover this function in a bit
        logger.debug("checking if this works")
        assert_frame_equal(results_pandas, expected_results)


# run the test
if __name__ == '__main__':
    unittest.main()