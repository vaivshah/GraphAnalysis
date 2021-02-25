import sys
import time
import json
from pprint import pprint

import pandas as pd

# from neo4j import GraphDatabase
# from neo4j.exceptions import ClientError
from dask import delayed, compute
import multiprocessing.popen_spawn_posix
from dask.distributed import Client

import helpers.graph_services as gs
import helpers.helper as helper

def project_Graph_in_memory(GRAPH_NAME):
    neo4j_connection = gs.graph_driver(uri_scheme='bolt', host='localhost', port='8687', username='neo4j', password='vaibhav123')
    existing_graphs = neo4j_connection.check_if_graph_exists(GRAPH_NAME)
    if not existing_graphs[0]['exists']:
        r = neo4j_connection.create_graph(GRAPH_NAME)
    else:
        r = neo4j_connection.get_graph_information(GRAPH_NAME)
    res = dict(r[0].items())

def run_algorithms(graph_name, algorithm_class, algorithm, properties):
    neo4j_connection = gs.graph_driver(uri_scheme='bolt', host='localhost', port='8687', username='neo4j', password='vaibhav123')
    query_time = time.time()
    query = neo4j_connection.get_gds_query(graph_name=graph_name, algorithm_name=algorithm, execution_mode="write", properties=properties, estimate=False)
    res=neo4j_connection.run_gds_algorithm(query)
    r=[]
    for rs in res:
        r.append(dict(rs.items()))
    return {'results':r, 'time':time.time()-query_time, 'query':query, 
            'algorithm':algorithm, 'algorithm_class': algorithm_class, 'properties':properties}

def estimate_algorithms(graph_name, algorithm_class, algorithm, properties):
    neo4j_connection = gs.graph_driver(uri_scheme='bolt', host='localhost', port='8687', username='neo4j', password='vaibhav123')
    query_time = time.time()
    query = str(
    query = neo4j_connection.get_gds_query(graph_name=graph_name, algorithm_name=algorithm, execution_mode="write", properties=properties, estimate=True))
    r=dict(neo4j_connection.run_gds_algorithm(query)[0].items())
    return {'results':r, 'time':time.time()-query_time, 'query':query, 
            'algorithm':algorithm, 'algorithm_class': algorithm_class, 'properties':properties}

def main():
    client = Client(n_workers=6, threads_per_worker=6)
    
    GRAPH_NAME="twitch-graph"
    project_Graph_in_memory(GRAPH_NAME)
    list_of_algorithms = helper.get_list_of_algorithms()
    res = []
    for class_of_algorithm in list_of_algorithms:
        for algorithm in list_of_algorithms[class_of_algorithm]:
            props=list_of_algorithms[class_of_algorithm][algorithm]
#             lazy_result = delayed(estimate_algorithms)(GRAPH_NAME, class_of_algorithm, algorithm, props)
            lazy_result = delayed(run_algorithms)(GRAPH_NAME, class_of_algorithm, algorithm, props, )
            res.append(lazy_result)
    actual_res = compute(res)
    
if __name__ == "__main__":
    main()
