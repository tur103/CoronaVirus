from typing import List

from neo4j_runner.neo4j_query_runner import Neo4jQueryRunner


class Neo4jInsertionManager:
    """
    Manager that handles the insertion to the graph.
    """
    def __init__(self, neo4j_query_runner: Neo4jQueryRunner):
        self.neo4j_query_runner = neo4j_query_runner

    def manage_insertion(self, insertion_queries: List[str]):
        """
        Manage the insertion to the graph.
        """
        for query in insertion_queries:
            self.neo4j_query_runner.run_query(query=query)
