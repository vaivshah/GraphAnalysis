from tqdm import tqdm

from neo4j import GraphDatabase
from neo4j.exceptions import ClientError

from . import helper as helpers

class graph_driver():
    def __init__(self, uri_scheme='bolt', host='localhost', port='7687', username='neo4j', password=''):
        self.uri_scheme = uri_scheme
        self.host = host
        self.port = port
        
        self.username = username
        self.password = password
        
        self.connection_uri = "{uri_scheme}://{host}:{port}".format(uri_scheme=self.uri_scheme, host=self.host, port=self.port)
        self.auth = (self.username, self.password)
        self.driver = GraphDatabase.driver(self.connection_uri, auth=self.auth)
        
    def __del__(self):
        self._close_driver()
    
    def _close_driver(self):
        if self.driver:
            self.driver.close()
    
    def run_single_query(self, query):
        res = None
        with self.driver.session() as session:
            raw_res = session.run(query)
            res = self.format_raw_res(raw_res)
        return res
    
    def run_bulk_query(self, query_list):
        results = []
        with self.driver.session() as session:
            for query in tqdm(query_list):
                raw_res = session.run(query)
                res = self.format_raw_res(raw_res)
                results.append({'query':query, 'result':res})
        return results
    
    def check_if_graph_exists(self, graph_name):
        query="""CALL gds.graph.exists('{graph_name}')""".format(graph_name=graph_name)
        res=self.run_single_query(query)
        return res

    def get_graph_information(self, graph_name):
        query="""CALL gds.graph.list('{graph_name}')""".format(graph_name=graph_name)
        res=self.run_single_query(query)
        return res

    def create_graph(self, graph_name):
        query = """
        CALL gds.graph.create(
          '{graph_name}', 
          {{
            TWITCH_USER: {{
                label:'TWITCH_USER', 
                properties: ['days', 'views']
                }}
          }}, 
          {{
            KNOWS: {{
                type:'KNOWS', 
                orientation: 'UNDIRECTED',
                properties: ['days', 'views']
                }}
          }} 
        )
        """.format(graph_name=graph_name)
        res=self.run_single_query(query)
        return res

    @staticmethod
    def get_gds_query(graph_name, algorithm_name, execution_mode="stream", properties={}, estimate=False):
        properties_as_string = helpers.convert_dict_to_string(properties)
        if estimate:
            query="""CALL gds.{algorithm_name}.{execution_mode}.estimate('{graph_name}', {{ {properties} }})""".format(
                algorithm_name=algorithm_name,
                execution_mode=execution_mode,
                graph_name=graph_name,
                properties=properties_as_string)
        else:
            query="""CALL gds.{algorithm_name}.{execution_mode}('{graph_name}', {{ {properties} }})""".format(
                algorithm_name=algorithm_name,
                execution_mode=execution_mode,
                graph_name=graph_name,
                properties=properties_as_string)
        return query

    def run_gds_algorithm(self, query):
        try:
            res=self.run_single_query(query)
        except ClientError as e:
            print(e)
            res=[{'error':e}]
        return res
    
    def reset_graph(self, db=None):
        return self.run_single_query("MATCH (n) DETACH DELETE n")
    
    def test_connection(self):
        return self.run_single_query("MATCH (n) RETURN COUNT(n) as nodes, sum(size((n)-[]->())) as relations")
    
    @staticmethod
    def format_raw_res(raw_res):
        res = []
        for r in raw_res:
            res.append(r)
        return res

        
def main():
    driver = graph_driver()
    res = driver.test_connection()
    print(res)
    
if __name__ == "__main__":
    main()