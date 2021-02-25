import sys
import time

from tqdm import tqdm
from pymongo import UpdateOne, InsertOne
import dask
from dask.distributed import Client, progress
# from dask.diagnostics import ProgressBar
import multiprocessing.popen_spawn_posix

import helpers.mongo_services as ms
import helpers.helper as helper

def get_mongo_driver(db="DEFAULT_DB", collection="DEFAULT_COLLECTION"):
    conf = {
        'MONGODB_HOST': 'localhost',
        'MONGODB_PORT': '27817',
        'LOG_FILE': './twitch/mongo/logs/db.log'
    }
    return ms.get_MongoDB_driver(conf, db, collection)

def write_data(language_code, edges, features, targets):
    mongo_driver=get_mongo_driver(db="twitch_users", collection="relational_features")
    
    targets_to_push=[]
    for target in tqdm(targets):
        targets_to_push.append(
            InsertOne(
                {
                    'language_code':language_code,
                    'mature': target['mature']=="True",
                    'partner': target['partner']=="True",
                    'days': int(target['days']),
                    'views': int(target['views']),
                    'new_id': int(target['new_id']),
                    'id': int(target['id']),
                },
            )
        )
    
    mongo_driver.perform_bulk_operations(targets_to_push)
#     mongo_driver.collection.insert_many(targets)
    
#     edges_to_push=[]
#     for edge in tqdm(edges):
#         edges_to_push.append(
#             UpdateOne(
#                 filter={'new_id': int(edge['from']), 'language_code':language_code},  # Find application if it exists in the collection.
#                 update={'$push': {'to': edge['to']}}, # Using '$push', means push to list if does not exist in list already.
#             )
#         )
#     mongo_driver.perform_bulk_operations(edges_to_push)
    
#     features_to_push=[]
#     for key in tqdm(features):
#         features_to_push.append(
#             UpdateOne(
#                 filter={'new_id': key, 'language_code':language_code},  # Find application if it exists in the collection.
#                 update={'$addToSet': { 'features' :{'$each': features[key]}}}, # Using '$push', means push to list if does not exist in list already.
#             )
#         )
#     mongo_driver.perform_bulk_operations(features_to_push)
    return 
    
    
def read_data(language_code):
    edges_filename = './twitch/data/0_raw_data/{}/musae_{}_edges.csv'.format(language_code, language_code)
    features_filename = './twitch/data/0_raw_data/{}/musae_{}_features.json'.format(language_code, language_code)
    target_filename = './twitch/data/0_raw_data/{}/musae_{}_target.csv'.format(language_code, language_code)
    
    edges_start_time = time.time()
    edges = helper.read_csv_as_dict(edges_filename)
    edges_reading_time = time.time()-edges_start_time
    
    features_start_time = time.time()
    features = helper.read_json(features_filename)
    features_reading_time = time.time()-features_start_time
    
    target_start_time = time.time()
    targets = helper.read_csv_as_dict(target_filename)
    target_reading_time = time.time()-target_start_time
    
    results = {
        "edges": {"length":len(edges), "time":edges_reading_time}, 
        "features": {"length":len(features), "time":features_reading_time}, 
        "targets": {"length":len(targets), "time":target_reading_time},
    }
    return edges, features, targets, results

def process_language_files(language_code):
    reading_start_time = time.time()
    edges, features, targets, reading_results = read_data(language_code)
    reading_time = time.time()-reading_start_time
    
    
    adding_start_time = time.time()
    writing_results = write_data(language_code, edges, features, targets)
    adding_time = time.time()-adding_start_time
    
    return {"reading_results": reading_results, "writing_results": writing_results}

def main():
    client = Client(n_workers=6, threads_per_worker=6)
    language_codes = ['DE', 'ENGB', 'ES', 'FR', 'PTBR', 'RU']
    res=[]
    for index, language_code in enumerate(language_codes):
        processing_start_time = time.time()

        lazy_result = dask.delayed(process_language_files)(language_code)
        res.append(lazy_result)

        processing_time =time.time()-processing_start_time

    results = dask.persist(res)  # start computation in the background
    progress(results)      # watch progress
    

if __name__ == "__main__":
    main()