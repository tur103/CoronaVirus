from neo4j import BoltStatementResult

from neo4j_runner.neo4j_driver import Neo4jDriver


class Neo4jQueryRunner:
    """
    Run cypher queries on neo4j server.
    """
    def __init__(self, neo4j_driver: Neo4jDriver):
        self.neo4j_driver = neo4j_driver

    def run_query(self, query: str) -> BoltStatementResult:
        """
        Run the given cypher query in the neo4j server.
        """
        with self.neo4j_driver.driver.session() as session:
            return session.run(query)
