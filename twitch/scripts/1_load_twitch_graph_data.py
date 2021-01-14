import sys
import time

import dask
from dask.distributed import Client

import helpers.graph_services as gs
import helpers.helper as helper

def add_nodes(nodes_list, language_code):
    list_of_create_strings = []
    create_string_template = """
        CREATE (:TWITCH_USER {{
            node_id:{node_id}, 
            days:{days}, 
            mature:{mature}, 
            views:{views}, 
            partner:{partner},
            language:'{language_code}'
            }}
            )"""
    for index, node in nodes_list.iterrows():
        create_string = create_string_template.format(
            node_id=node['new_id'],
            days=node['days'],
            mature=node['mature'],
            views=node['views'],
            partner=node['partner'],
            language_code=str(language_code),
        )

        list_of_create_strings.append(create_string)
    return list_of_create_strings
        
def add_relations(relations_list, language_code):
    list_of_create_strings = []
    create_string_template = """
        MATCH (a:TWITCH_USER),(b:TWITCH_USER)
        WHERE a.node_id = {source} AND b.node_id = {target} AND a.language = '{language_code}' AND b.language = '{language_code}'
        MERGE (a)-[r:KNOWS {{ relationship_id:{edge_id}, language:'{language_code}' }}]-(b)
        """
    for index, relations in relations_list.iterrows():
        create_string = create_string_template.format(
            source=relations['from'],
            target=relations['to'],
            edge_id=''+str(relations['from'])+'-'+str(relations['to']),
            language_code=str(language_code),
        )

        list_of_create_strings.append(create_string)
    return list_of_create_strings

def add_data(language_code, edges, features, targets, neo4j_connection):
    add_nodes_start_time = time.time()
    add_nodes_cypher = add_nodes(targets, language_code)
    node_results = neo4j_connection.run_bulk_query(add_nodes_cypher)
    add_nodes_time = time.time()-add_nodes_start_time
    print("--------Finished writing {num} nodes in {time}s.".format(num=len(targets), time=str(add_nodes_time)))
 
    add_relations_start_time = time.time()
    add_relations_cypher = add_relations(edges, language_code)
    relations_results = neo4j_connection.run_bulk_query(add_relations_cypher)
    add_relations_time = time.time()-add_relations_start_time
    print("--------Finished writing {num} relations in {time}s.".format(num=len(edges), time=str(add_relations_time)))
    
    
    results = {
        "nodes": {"results": node_results, "length":len(add_nodes_cypher), "time":add_nodes_time}, 
        "relations": {"results": relations_results, "length":len(add_relations_cypher), "time":add_relations_time}, 
    }
    return results

def load_data(language_code):
    edges_filename = './data/0_raw_data/{}/musae_{}_edges.csv'.format(language_code, language_code)
    features_filename = './data/0_raw_data/{}/musae_{}_features.json'.format(language_code, language_code)
    target_filename = './data/0_raw_data/{}/musae_{}_target.csv'.format(language_code, language_code)
    
    edges_start_time = time.time()
    edges = helper.read_csv(edges_filename)
    edges_reading_time = time.time()-edges_start_time
    print("--------Finished reading {num} edges in {time}s.".format(num=len(edges), time=str(edges_reading_time)))
    
    features_start_time = time.time()
    features = helper.read_json(features_filename)
    features_reading_time = time.time()-features_start_time
    print("--------Finished reading {num} features in {time}s.".format(num=len(features), time=str(features_reading_time)))
    
    target_start_time = time.time()
    targets = helper.read_csv(target_filename)
    target_reading_time = time.time()-target_start_time
    print("--------Finished reading {num} target in {time}s.".format(num=len(targets), time=str(target_reading_time)))
    
    results = {
        "edges": {"length":len(edges), "time":edges_reading_time}, 
        "features": {"length":len(features), "time":features_reading_time}, 
        "targets": {"length":len(targets), "time":target_reading_time},
    }
    return edges, features, targets, results

def process_language_files(language_code):
    print("----Started reading raw data: {language}".format(language=language_code))
    reading_start_time = time.time()
    edges, features, targets, reading_results = load_data(language_code)
    reading_time = time.time()-reading_start_time
    print("----Finshed reading raw data: {language} in {time}".format(language=language_code, time=str(reading_time)))
    
    
    print("----Started writing graph data: {language}".format(language=language_code))
    adding_start_time = time.time()
    neo4j_connection = gs.graph_driver(uri_scheme='bolt', host='localhost', port='8687', username='neo4j', password='vaibhav123')
    writing_results = add_data(language_code, edges, features, targets, neo4j_connection)
    adding_time = time.time()-adding_start_time
    print("----Finshed writing graph data: {language} in {time}".format(language=language_code, time=str(adding_time)))
    
    return {"reading_results": reading_results, "writing_results": writing_results}

def main():
    client = Client(n_workers=6, threads_per_worker=6)
    language_codes = ['DE', 'ENGB', 'ES', 'FR', 'PTBR', 'RU']
    res = []
    results= {}
    for index, language_code in enumerate(language_codes):
        print("="*25)

        print("Started processing data: {language}".format(language=language_code))   
        processing_start_time = time.time()
        lazy_result = dask.delayed(process_language_files)(language_code)
        res.append(lazy_result)
#         node_results, relations_results = process_language_files(language_code)
#         results[language_code] = {"node_results": node_results, "relations_results": relations_results}
        processing_time =time.time()-processing_start_time
        print("Finished processing data: {language} in {time}".format(language=language_code, time=processing_time))   

        print("="*25)
        print("\n")
    results = dask.compute(res)
#     print(results)
    
if __name__ == "__main__":
    main()