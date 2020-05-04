import unittest
import logging
from pyspark.sql import SparkSession


# set up base class for spark session
class PySparkTest(unittest.TestCase):

    @classmethod
    def quiet_py4j_logging(cls):
        logger = logging.getLogger("py4j")
        logger.setLevel(logging.WARN)

    @classmethod
    def create_testing_pyspark_session(cls):
        return (SparkSession.builder
            .master("local[2]")
            .appName("SparkUnitTestDemo")
                .getOrCreate())

    @classmethod
    def setUpClass(cls):
        cls.quiet_py4j_logging()
        cls.spark = cls.create_testing_pyspark_session()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

