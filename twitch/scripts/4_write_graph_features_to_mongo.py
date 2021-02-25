import sys
import time
import json
from pprint import pprint

import pandas as pd

from neo4j import GraphDatabase
from neo4j.exceptions import ClientError
from dask import delayed, compute
import multiprocessing.popen_spawn_posix
from dask.distributed import Client

import helpers.graph_services as gs
import helpers.mongo_services as ms
import helpers.helper as helper

def read_batch_from_neo4j(driver, writeProps, start_index=0, batch_size=1, ):
    query="""
    MATCH (n)
    RETURN {props} 
    SKIP {start_index}
    LIMIT {batch_size}
    """.format(start_index=start_index, batch_size=batch_size, props=" ".join(writeProps)[:-1])
    raw_res=driver.run_single_query(query)
    intermediate_res=driver.format_raw_res(raw_res)
    return format_intermediate_address(intermediate_res)

def format_intermediate_address(intermediate_res):
    writeProps=get_algos(helper.get_list_of_algorithms())
    DEFAULT='non-graph'
    formatted_results=[]
    for rs in intermediate_res:
        res_dict={DEFAULT:{}}
        for propName in writeProps.keys():
            res_dict[propName]={}
            
        rs=dict(rs)
        for key, value in rs.items():
            for propName in writeProps.keys():
                if key in writeProps[propName]:
                    res_dict[propName][key]=value
                    break
            else:
                res_dict[DEFAULT][key]=value
        formatted_results.append(res_dict)
    return formatted_results

def get_mongo_driver(db="DEFAULT_DB", collection="DEFAULT_COLLECTION"):
    conf = {
        'MONGODB_HOST': 'localhost',
        'MONGODB_PORT': '27817',
        'LOG_FILE': './twitch/mongo/logs/db.log'
    }
    return ms.get_MongoDB_driver(conf, db, collection)

def write_batch_to_mongo(driver, data):
    return driver.collection.insert_many(data)
    
def run_single_batch(writeProps, start_index, step):
    neo4j_connection=gs.graph_driver(uri_scheme='bolt', host='localhost', port='8687', username='neo4j', password='vaibhav123')
    neo4j_data=read_batch_from_neo4j(neo4j_connection, writeProps, start_index, step)
    
    mongo_driver=get_mongo_driver(db="twitch_users", collection="graph_features")
    mongo_res=None
    if mongo_driver:
        mongo_res=write_batch_to_mongo(mongo_driver, neo4j_data)
    
    return mongo_res

def get_number_of_nodes():
    neo4j_connection=gs.graph_driver(uri_scheme='bolt', host='localhost', port='8687', username='neo4j', password='vaibhav123')
    neo4j_data=neo4j_connection.test_connection()
    return dict(neo4j_data[0])['nodes']

def get_algos(list_of_algorithms):
    writeProps={}
    for class_of_algorithm in list_of_algorithms:
        writeProps[class_of_algorithm]=[]
        for algorithm in list_of_algorithms[class_of_algorithm]:
            writeProps[class_of_algorithm].append(list_of_algorithms[class_of_algorithm][algorithm]['writeProperty'])
    return writeProps

def main():
    client = Client(n_workers=6, threads_per_worker=6)
    writeProps=get_algos(helper.get_list_of_algorithms())    
    writeProps=[prop for class_of_algorithm in writeProps for prop in writeProps[class_of_algorithm] ]
#     writeProps+=['language', 'mature', 'partner', 'views', 'days', 'node_id']
    writeProps+=['node_id', 'language']
    writeProps=["n."+prop+" AS "+prop+"," for prop in writeProps]
    
    STEP=6000
    NUMBER_OF_NODES=get_number_of_nodes()

    res = []
    for index in range(0, NUMBER_OF_NODES, STEP):
        lazy_result = delayed(run_single_batch)(writeProps, index, STEP)
        res.append(lazy_result)
        
    actual_res = compute(res)

if __name__ == "__main__":
    main()