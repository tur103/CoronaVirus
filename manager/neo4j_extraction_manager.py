from pandas import DataFrame

from neo4j_runner.neo4j_query_runner import Neo4jQueryRunner


class Neo4jExtractionManager:
    """
    Manager that handles the extraction from the graph.
    """
    def __init__(self, neo4j_query_runner: Neo4jQueryRunner):
        self.neo4j_query_runner = neo4j_query_runner

    def manage_extraction(self, extraction_query: str) -> DataFrame:
        """
        Manage the extraction from the graph.
        """
        result = self.neo4j_query_runner.run_query(query=extraction_query)
        dataframe = DataFrame([record.values() for record in result], columns=result.keys())
        return dataframe
