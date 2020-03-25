from neo4j import GraphDatabase


class Neo4jDriver:
    """
    The driver used to connect to the neo4j server.
    """
    def __init__(self, bolt: str, username: str, password: str):
        self.driver = GraphDatabase.driver(uri=bolt, auth=(username, password), encrypted=False)
