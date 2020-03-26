import constants
from neo4j_runner.neo4j_driver import Neo4jDriver
from neo4j_runner.neo4j_query_runner import Neo4jQueryRunner
from manager.neo4j_insertion_manager import Neo4jInsertionManager
from manager.neo4j_extraction_manager import Neo4jExtractionManager
from visualizer.line_progression_visualizer import LineProgressionVisualizer
from cypher_query.insertion import LOAD_UPDATES, LOAD_COUNTRIES, LOAD_DATES, LOAD_COUNTRY_CASES,\
    LOAD_GLOBAL_RECENT_UPDATES, LOAD_PROGRESSION, LOAD_GLOBAL_CASE_TYPES, LOAD_GLOBAL_PROGRESSION,\
    LOAD_MOST_CASES_FOR_DATE, LOAD_PEOPLE
from cypher_query.extraction import FIND_PROGRESSION, FIND_WORLD_PROGRESSION


def main():
    neo4j_driver = Neo4jDriver(bolt=constants.BOLT, username=constants.USERNAME, password=constants.PASSWORD)
    neo4j_query_runner = Neo4jQueryRunner(neo4j_driver=neo4j_driver)

    insertion_queries = [LOAD_UPDATES, LOAD_COUNTRIES, LOAD_DATES, LOAD_COUNTRY_CASES, LOAD_GLOBAL_RECENT_UPDATES,
                         LOAD_PROGRESSION, LOAD_GLOBAL_CASE_TYPES, LOAD_GLOBAL_PROGRESSION, LOAD_MOST_CASES_FOR_DATE,
                         LOAD_PEOPLE]
    neo4j_insertion_manager = Neo4jInsertionManager(neo4j_query_runner=neo4j_query_runner)
    neo4j_insertion_manager.manage_insertion(insertion_queries=insertion_queries)

    country_region = input("Enter which country do you want to investigate: ")
    if country_region == "World":
        province_state = ""
        extraction_query = FIND_WORLD_PROGRESSION
    else:
        province_state = input("Enter which province do you want to investigate (optional): ")
        checked_province_state = province_state if province_state else "N/A"
        extraction_query = FIND_PROGRESSION.format(country_region=country_region,
                                                   province_state=checked_province_state)

    neo4j_extraction_manager = Neo4jExtractionManager(neo4j_query_runner=neo4j_query_runner)
    progression = neo4j_extraction_manager.manage_extraction(extraction_query=extraction_query)

    LineProgressionVisualizer.visualize_progression(progression=progression, country_region=country_region,
                                                    province_state=province_state)


if __name__ == "__main__":
    main()
