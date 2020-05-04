from pyspark.sql import functions as F


# simple test function to filter the df col by max value
def time_col_max(df):
 return df.filter(F.col('ObservationTime') >= 24)

