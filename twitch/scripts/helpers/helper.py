import json
import csv
import pandas as pd

def read_csv(filename):
    with open(filename, 'r') as file:
        data = pd.read_csv(file)
        
    return data

def read_csv_as_dict(filename):
    data=[]
    file = csv.DictReader(open(filename, 'r'))
    for row in file:
        data.append(row)
        
    return data

def read_json(filename):
    with open(filename, 'r') as file:
        data = json.load(file)
        
    return data

def convert_dict_to_string(dic={}):
    dict_pieces=[]
    print(dic)
    for key, value in dic.items():
        if type(value) == str:
            dict_pieces.append(f'{key}:\'{value}\'')
        else:
            dict_pieces.append(f'{key}:{value}')
            
    return ", ".join(dict_pieces) 

def get_list_of_centrality_algorithms():
    pageRank={'writeProperty':'pageRank'}
    betweenness={'writeProperty':'betweenness'}
    articleRank={'writeProperty':'articleRank'}
    closeness={'writeProperty':'closeness'}
    closeness_harmonic={'writeProperty':'closeness_harmonic'}
    degree={'writeProperty':'degree'}
    eigenvector={'writeProperty':'eigenvector'}

    centrality_algorithms = {
        'pageRank':pageRank, 
        'betweenness':betweenness, 
        'alpha.articleRank':articleRank,
        'alpha.closeness':closeness, 
        'alpha.closeness.harmonic':closeness_harmonic, 
        'alpha.degree':degree,
        'alpha.eigenvector':eigenvector
    }
    return centrality_algorithms

def get_list_of_community_detection_algorithms():
    louvain={'writeProperty':'louvain'}
    labelPropagation={'writeProperty':'labelPropagation'}
    wcc={'writeProperty':'wcc'}
    triangleCount={'writeProperty':'triangleCount'}
    localClusteringCoefficient={'writeProperty':'localClusteringCoefficient'}
    k1coloring={'writeProperty':'k1coloring'}
    modularityOptimization={'writeProperty':'modularityOptimization'}
    scc={'writeProperty':'scc'}

    community_detection_algorithms = {
        'louvain':louvain, 
        'labelPropagation':labelPropagation, 
        'wcc':wcc,
        'triangleCount':triangleCount, 
        'localClusteringCoefficient':localClusteringCoefficient, 
        'beta.k1coloring':k1coloring,
        'beta.modularityOptimization':modularityOptimization,
        'alpha.scc':scc,
    }
    return community_detection_algorithms

def get_list_of_node_embedding_algorithms():
    fastRP={'writeProperty':'fastRP', 'embeddingDimension':4}
    graphSagetrain={'modelName': 'graphSage', 'degreeAsProperty': True, 'projectedFeatureDimension': 4}
    graphSage={'writeProperty':'graphSage', 'modelName': 'graphSage'}
    node2vec={'writeProperty':'node2vec', 'embeddingDimension':4}

    node_embedding_algorithms = {
        'fastRP':fastRP, 
    #     'beta.graphSage.train':graphSagetrain,
    #     'beta.graphSage':graphSage,
        'alpha.node2vec':node2vec,
    }
    return node_embedding_algorithms

def get_list_of_algorithms():
    centrality_algorithms=get_list_of_centrality_algorithms()
    community_detection_algorithms=get_list_of_community_detection_algorithms()
    node_embedding_algorithms=get_list_of_node_embedding_algorithms()
    list_of_algorithms = {
        'centrality': centrality_algorithms,
        'community_detection': community_detection_algorithms,
        'node_embedding': node_embedding_algorithms
    }
    return list_of_algorithms