import json
import pandas as pd

def read_csv(filename):
    with open(filename, 'r') as file:
        data = pd.read_csv(file)
        
    return data

def read_json(filename):
    with open(filename, 'r') as file:
        data = json.load(file)
        
    return data