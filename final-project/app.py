from flask import Blueprint

main = Blueprint('main', __name__)

import json
from engine import ClusteringEngine

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from flask import Flask, request


@main.route("/model1/<int:c_id>/cluster", methods=["GET"])
def get_cluster1(c_id):
    logger.debug("Crime %s requested", c_id)
    cluster = clustering_engine.get_cluster1(c_id)
    return json.dumps(cluster)

@main.route("/model2/<int:c_id>/cluster", methods=["GET"])
def get_cluster2(c_id):
    logger.debug("Crime %s requested", c_id)
    cluster = clustering_engine.get_cluster2(c_id)
    return json.dumps(cluster)

@main.route("/model3/<int:c_id>/cluster", methods=["GET"])
def get_cluster3(c_id):
    logger.debug("Crime %s requested", c_id)
    cluster = clustering_engine.get_cluster3(c_id)
    return json.dumps(cluster)

def create_app(spark_session, dataset_path):
    global clustering_engine

    clustering_engine = ClusteringEngine(spark_session, dataset_path)

    app = Flask(__name__)
    app.register_blueprint(main)
    return app
