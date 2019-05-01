import os
import logging
import pandas as pd

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql import types
from pyspark.sql.functions import explode
import pyspark.sql.functions as func

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RecommendationEngine:
    """A movie recommendation engine
    """

    def __train_model(self):
        """Train the ALS model with the current dataset
        """
        logger.info("Training the ALS model...")
        self.als = ALS(userCol="userID", itemCol="movieID", ratingCol="rating",
                  coldStartStrategy="drop")
        self.model = self.als.fit(self.ratingsdf)
        logger.info("ALS model built!")

    def get_top_ratings(self, user_id, movies_count):
        """Recommends up to movies_count top unrated movies to user_id
        """
        users = self.ratingsdf.select(self.als.getUserCol())
        users = users.filter(users.userID == user_id)
        userSubsetRecs = self.model.recommendForUserSubset(users, movies_count)
        userSubsetRecs = userSubsetRecs.withColumn("recommendations", explode("recommendations"))
        userSubsetRecs = userSubsetRecs.select(func.col('userID'),
                                               func.col('recommendations')['movieID'].alias('movieID'),
                                               func.col('recommendations')['rating'].alias('rating')).\
                                                                                    drop('recommendations')
        userSubsetRecs = userSubsetRecs.drop('rating')
        userSubsetRecs = userSubsetRecs.join(self.moviesdf, ("movieID"), 'inner')
        # userSubsetRecs.show()
        # userSubsetRecs.printSchema()
        userSubsetRecs = userSubsetRecs.toPandas()
        userSubsetRecs = userSubsetRecs.to_json()
        return userSubsetRecs

    def get_top_recommendations(self, movie_id, user_count):
        """Recommends up to movies_count top unrated movies to user_id
        """
        movies = self.ratingsdf.select(self.als.getItemCol())
        movies = movies.filter(movies.movieID == movie_id)
        movieSubsetRecs = self.model.recommendForItemSubset(movies, user_count)
        movieSubsetRecs = movieSubsetRecs.withColumn("recommendations", explode("recommendations"))
        movieSubsetRecs = movieSubsetRecs.select(func.col('movieID'),
                                                 func.col('recommendations')['userID'].alias('userID'),
                                                 func.col('recommendations')['rating'].alias('rating')).\
                                                                                        drop('recommendations')
        movieSubsetRecs = movieSubsetRecs.drop('rating')
        movieSubsetRecs = movieSubsetRecs.join(self.moviesdf, ("movieID"), 'inner')
        # userSubsetRecs.show()
        # userSubsetRecs.printSchema()
        movieSubsetRecs = movieSubsetRecs.toPandas()
        movieSubsetRecs = movieSubsetRecs.to_json()
        return movieSubsetRecs

    def get_ratings_for_movie_ids(self, user_id, movie_id):
        """Given a user_id and a list of movie_ids, predict ratings for them
        """
        request = self.spark_session.createDataFrame([(user_id, movie_id)], ["userID", "movieID"])
        ratings = self.model.transform(request).collect()
        return ratings

    def get_user_ratings_history(self, user_id):
        """Get rating history for a user
        """
        self.ratingsdf.createOrReplaceTempView("ratings_data")
        user_ratings_history = self.spark_session.sql('SELECT userID, movieID, rating from ratings_data where userID = "%s"' %user_id)
        user_ratings_history = user_ratings_history.join(self.moviesdf, ("movieID"), 'inner')
        user_ratings_history = user_ratings_history.toPandas()
        user_ratings_history = user_ratings_history.to_json()
        return user_ratings_history

    def get_movie_ratings_history(self, movie_id):
        """Get rating history for a movie
        """
        self.ratingsdf.createOrReplaceTempView("ratings_data")
        movie_ratings_history = self.spark_session.sql('SELECT userID, movieID, rating from ratings_data where movieID = "%s"' %movie_id)
        movie_ratings_history = movie_ratings_history.join(self.moviesdf, ("movieID"), 'inner')
        movie_ratings_history = movie_ratings_history.toPandas()
        movie_ratings_history = movie_ratings_history.to_json()
        return movie_ratings_history

    def __init__(self, spark_session, dataset_path):
        """Init the recommendation engine given a Spark context and a dataset path
        """
        logger.info("Starting up the Recommendation Engine: ")
        self.spark_session = spark_session
        # Load ratings data for later use
        logger.info("Loading Ratings data...")
        ratings_file_path = os.path.join(dataset_path, 'user_ratedmovies-timestamps.dat')
        self.ratingsdf = spark_session.read.option("header", "true").option("delimiter", "\t").option("inferSchema", "true").csv(ratings_file_path)
        self.ratingsdf = self.ratingsdf.drop("timestamp")
        # Load movies data for later use
        logger.info("Loading Movies data...")
        movies_file_path = os.path.join(dataset_path, 'movies.dat')
        self.moviesdf = spark_session.read.option("header", "true").option("delimiter", "\t").option("inferSchema", "true").csv(movies_file_path)
        self.moviesdf = self.moviesdf.withColumnRenamed("id", "movieID")
        self.moviesdf = self.moviesdf.drop("imdbID")
        self.moviesdf = self.moviesdf.drop("spanishTitle")
        self.moviesdf = self.moviesdf.drop("imdbPictureURL")
        self.moviesdf = self.moviesdf.drop("year")
        self.moviesdf = self.moviesdf.drop("rtID")
        self.moviesdf = self.moviesdf.drop("rtAllCriticsRating")
        self.moviesdf = self.moviesdf.drop("rtAllCriticsNumReviews")
        self.moviesdf = self.moviesdf.drop("rtAllCriticsNumFresh")
        self.moviesdf = self.moviesdf.drop("rtAllCriticsNumRotten")
        self.moviesdf = self.moviesdf.drop("rtAllCriticsScore")
        self.moviesdf = self.moviesdf.drop("rtTopCriticsRating")
        self.moviesdf = self.moviesdf.drop("rtTopCriticsNumReviews")
        self.moviesdf = self.moviesdf.drop("rtTopCriticsNumFresh")
        self.moviesdf = self.moviesdf.drop("rtTopCriticsNumRotten")
        self.moviesdf = self.moviesdf.drop("rtTopCriticsScore")
        self.moviesdf = self.moviesdf.drop("rtAudienceRating")
        self.moviesdf = self.moviesdf.drop("rtAudienceNumRatings")
        self.moviesdf = self.moviesdf.drop("rtAudienceScore")
        self.moviesdf = self.moviesdf.drop("rtPictureURL")
        # Train the model
        self.__train_model()
