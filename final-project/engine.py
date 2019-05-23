import os
import logging
import pandas as pd

from pyspark.sql import Row
from pyspark.sql import types
from pyspark.sql.functions import explode
import pyspark.sql.functions as func
from sklearn.preprocessing import MinMaxScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import DoubleType
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
import plotly.plotly as py
import plotly.graph_objs as go
import plotly

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ClusteringEngine:
    """clustering engine
    """

    def __transform_model(self, spark_session):
        logger.info("Transforming model 1...")
        self.crimesdf1 = self.crimesdf1.withColumn("lat", self.crimesdf1["lat"].cast("double"))
        self.crimesdf1 = self.crimesdf1.withColumn("lon", self.crimesdf1["lon"].cast("double"))
        assembler = VectorAssembler(inputCols=["lat","lon"], outputCol='features')
        self.crimesdf1 = assembler.transform(self.crimesdf1)
        logger.info("Transformation model 1 done!")

        self.crimesdf2 = self.crimesdf2.withColumn("lat", self.crimesdf2["lat"].cast("double"))
        self.crimesdf2 = self.crimesdf2.withColumn("lon", self.crimesdf2["lon"].cast("double"))
        logger.info("Transforming model 2...")
        assembler = VectorAssembler(inputCols=["lat","lon"], outputCol='features')
        self.crimesdf2 = assembler.transform(self.crimesdf2)
        logger.info("Transformation model 2 done!")

        self.crimesdf3 = self.crimesdf3.withColumn("lat", self.crimesdf3["lat"].cast("double"))
        self.crimesdf3 = self.crimesdf3.withColumn("lon", self.crimesdf3["lon"].cast("double"))
        logger.info("Transforming model 3...")
        assembler = VectorAssembler(inputCols=["lat","lon"], outputCol='features')
        self.crimesdf3 = assembler.transform(self.crimesdf3)
        logger.info("Transformation model 3 done!")


    def __train_model(self):
        """Train the model with the current dataset
        """
        logger.info("Training model 1...")
        kmeans1 = KMeans().setK(7).setSeed(1)
        model1 = kmeans1.fit(self.crimesdf1)
        logger.info("Model 1 built!")
        logger.info("Evaluating the model 1...")
        self.centers1 = model1.clusterCenters()
        logger.info("Model 1 done!")

        logger.info("Training model 2...")
        kmeans2 = KMeans().setK(7).setSeed(1)
        model2 = kmeans2.fit(self.crimesdf2)
        logger.info("Model 2 built!")
        logger.info("Evaluating the model 2...")
        self.centers2 = model2.clusterCenters()
        logger.info("Model 2 done!")

        logger.info("Training model 3...")
        kmeans3 = KMeans().setK(7).setSeed(1)
        model3 = kmeans1.fit(self.crimesdf3)
        logger.info("Model 3 built!")
        logger.info("Evaluating the model 3...")
        self.centers3 = model3.clusterCenters()
        logger.info("Model 3 done!")

    def get_cluster1(self, c_id):
        return 1

    def __init__(self, spark_session, dataset_path):
        """Init the clustering engine given a Spark context and a dataset path
        """
        logger.info("Starting up the Clustering Engine: ")
        self.spark_session = spark_session
        # Load ratings data for later use
        logger.info("Loading crimes data...")

        file_name1 = 'model-1.txt'
        dataset_file_path1 = os.path.join(dataset_path, file_name1)
        exist = os.path.isfile(dataset_file_path1)
        if exist:
            self.crimesdf1 = spark_session.read.csv(dataset_file_path1, header=None, inferSchema=True)
            self.crimesdf1 = self.crimesdf1.selectExpr("_c0 as dr_number", "_c1 as lat", "_c2 as lon")

        file_name2 = 'model-2.txt'
        dataset_file_path2 = os.path.join(dataset_path, file_name2)
        exist = os.path.isfile(dataset_file_path2)
        if exist:
            self.crimesdf2 = spark_session.read.csv(dataset_file_path2, header=None, inferSchema=True)
            self.crimesdf2 = self.crimesdf2.selectExpr("_c0 as dr_number", "_c1 as lat", "_c2 as lon")

        file_name3 = 'model-3.txt'
        dataset_file_path3 = os.path.join(dataset_path, file_name3)
        exist = os.path.isfile(dataset_file_path3)
        if exist:
            self.crimesdf3 = spark_session.read.csv(dataset_file_path3, header=None, inferSchema=True)
            self.crimesdf3 = self.crimesdf3.selectExpr("_c0 as dr_number", "_c1 as lat", "_c2 as lon")
        
        # Transform the model
        self.__transform_model(spark_session)

        # Train the model
        self.__train_model()
