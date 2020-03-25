FIND_PROGRESSION = """
MATCH (country:Country {{Country_Region: '{country_region}'}})
MATCH (case_type:CaseType)-[:IN_COUNTRY]->(country)
MATCH (update:Update)-[:IN_COUNTRY_CASE]->(case_type)
WITH country, case_type, update
ORDER BY update.Date
RETURN toString(update.Date) AS Date, update.Cases AS Cases, update.Case_Type AS Case_Type
"""
