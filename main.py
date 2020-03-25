import constants
from neo4j_runner.neo4j_driver import Neo4jDriver
from neo4j_runner.neo4j_query_runner import Neo4jQueryRunner
from manager.neo4j_insertion_manager import Neo4jInsertionManager
from manager.neo4j_extraction_manager import Neo4jExtractionManager
from visualizer.line_progression_visualizer import LineProgressionVisualizer
from cypher_query.insertion import LOAD_UPDATES, LOAD_COUNTRIES, LOAD_DATES, LOAD_COUNTRY_CASES,\
    LOAD_GLOBAL_CASES, LOAD_PROGRESSION
from cypher_query.extraction import FIND_PROGRESSION


def main():
    neo4j_driver = Neo4jDriver(bolt=constants.BOLT, username=constants.USERNAME, password=constants.PASSWORD)
    neo4j_query_runner = Neo4jQueryRunner(neo4j_driver=neo4j_driver)

    insertion_queries = [LOAD_UPDATES, LOAD_COUNTRIES, LOAD_DATES, LOAD_COUNTRY_CASES, LOAD_GLOBAL_CASES,
                         LOAD_PROGRESSION]
    neo4j_insertion_manager = Neo4jInsertionManager(neo4j_query_runner=neo4j_query_runner)
    neo4j_insertion_manager.manage_insertion(insertion_queries=insertion_queries)

    country_region = input("Enter which country do you want to investigate: ")
    extraction_query = FIND_PROGRESSION.format(country_region=country_region)
    neo4j_extraction_manager = Neo4jExtractionManager(neo4j_query_runner=neo4j_query_runner)
    progression = neo4j_extraction_manager.manage_extraction(extraction_query=extraction_query)

    LineProgressionVisualizer.visualize_progression(progression=progression, country_region=country_region)


if __name__ == "__main__":
    main()
