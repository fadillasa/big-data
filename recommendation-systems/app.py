from flask import Blueprint

main = Blueprint('main', __name__)

import json
from engine import RecommendationEngine

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from flask import Flask, request


@main.route("/<int:user_id>/ratings/top/<int:count>", methods=["GET"])
def top_ratings(user_id, count):
    logger.debug("User %s TOP ratings requested", user_id)
    top_ratings = recommendation_engine.get_top_ratings(user_id, count)
    return json.dumps(top_ratings)

@main.route("/movies/<int:movie_id>/recommendations/<int:count>", methods=["GET"])
def top_recommendations(movie_id, count):
    logger.debug("MovieID %s TOP recommendations requested", movie_id)
    top_recommendations = recommendation_engine.get_top_recommendations(movie_id, count)
    return json.dumps(top_recommendations)

@main.route("/<int:user_id>/ratings/<int:movie_id>", methods=["GET"])
def movie_ratings(user_id, movie_id):
    logger.debug("User %s rating requested for movie %s", user_id, movie_id)
    ratings = recommendation_engine.get_ratings_for_movie_ids(user_id, movie_id)
    return json.dumps(ratings)

@main.route("/<int:user_id>/ratings", methods=["GET"])
def user_ratings_history(user_id):
    logger.debug("User %s ratings requested", user_id)
    user_ratings_history = recommendation_engine.get_user_ratings_history(user_id)
    return json.dumps(user_ratings_history)

@main.route("/movies/<int:movie_id>/ratings", methods=["GET"])
def movie_ratings_history(movie_id):
    logger.debug("MovieID %s ratings requested", movie_id)
    movie_ratings_history = recommendation_engine.get_movie_ratings_history(movie_id)
    return json.dumps(movie_ratings_history)


def create_app(spark_session, dataset_path):
    global recommendation_engine

    recommendation_engine = RecommendationEngine(spark_session, dataset_path)

    app = Flask(__name__)
    app.register_blueprint(main)
    return app
