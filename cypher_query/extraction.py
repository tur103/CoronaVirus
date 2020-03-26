FIND_PROGRESSION = """
MATCH (country:Country {{Country_Region: '{country_region}', Province_State: '{province_state}'}})
MATCH (case_type:CaseType)-[:IN_COUNTRY]->(country)
MATCH (update:Update)-[:IN_COUNTRY_CASE]->(case_type)
WITH country, case_type, update
ORDER BY update.Date
RETURN toString(update.Date.day + '/' + update.Date.month) AS Date, update.Cases AS Cases, update.Case_Type AS Case_Type
"""

FIND_WORLD_PROGRESSION = """
MATCH (global_case_type:GlobalCaseType)
MATCH (global_update:GlobalUpdate)-[:IN_GLOBAL_CASE]->(global_case_type)
WITH global_case_type, global_update
ORDER BY global_update.Date
RETURN toString(global_update.Date.day + '/' + global_update.Date.month) AS Date, global_update.Cases AS Cases,
       global_update.Case_Type AS Case_Type
"""