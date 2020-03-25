DELETE_ALL = """
MATCH (n)
DETACH DELETE n
"""

LOAD_UPDATES = """
LOAD CSV WITH HEADERS FROM "file:///COVID-19Cases.csv" AS line

MERGE (update:Update {Country_Region: line.Country_Region, Case_Type: line.Case_Type, Date: line.Date})
SET update += line
WITH line, update, SPLIT(update.Date, "/") AS datetime
SET update.Date = DATE({month: toInteger(datetime[0]),
                        day: toInteger(datetime[1]),
                        year: toInteger(datetime[2])})

WITH line, update
CALL apoc.create.addLabels([update], [line.Case_Type])
YIELD node
RETURN DISTINCT 1
"""

LOAD_COUNTRIES = """
MATCH (update:Update)
WHERE NOT (update)-[:IN_COUNTRY_CASE]->(:CaseType)

MERGE (country:Country {Country_Region: update.Country_Region})
MERGE (case_type: CaseType {Case_Type: update.Case_Type, Country_Region: country.Country_Region})
MERGE (case_type)-[case_rel:IN_COUNTRY]->(country)
MERGE (update)-[:IN_COUNTRY_CASE {Date: update.Date, Cases: update.Cases}]->(case_type)

WITH COLLECT(case_type) as case_types
UNWIND case_types AS case_type
CALL apoc.create.addLabels([case_type], [case_type.Case_Type])
YIELD node 
RETURN DISTINCT 1
"""

LOAD_DATES = """
MATCH (update:Update)
MERGE (date:Date {Date: update.Date})
MERGE (update)-[:IN_DATE {Date: update.Date}]->(date)
"""

LOAD_COUNTRY_CASES = """
OPTIONAL MATCH (update:Update:MostRecent)
REMOVE update:MostRecent
WITH COLLECT(update) AS most_recent_updates

MATCH (country:Country)
MATCH (case_type:CaseType)-[in_country:IN_COUNTRY]->(country)
MATCH (update:Update)-[:IN_COUNTRY_CASE]->(case_type)

WITH country, case_type, in_country, update
ORDER BY update.Date DESC
WITH country, case_type, in_country, COLLECT(update) AS updates
WITH country, case_type, in_country, updates[0] AS latest_update

SET latest_update:MostRecent
SET case_type.Last_Update = latest_update.Date, case_type.Cases = latest_update.Cases,
    in_country.Last_Update = latest_update.Date, in_country.Cases = latest_update.Cases
    
WITH country, case_type, latest_update
CALL apoc.do.when(case_type:Deaths,
                  'SET country.Last_Death_Update = latest_update.Date,
                       country.Death_Cases = latest_update.Cases',
                  'SET country.Last_Confirmed_Update = latest_update.Date,
                       country.Confirmed_Cases = latest_update.Cases',
                  {case_type:case_type, country:country, latest_update:latest_update})
YIELD value
RETURN DISTINCT 1
"""

LOAD_GLOBAL_CASES = """
MERGE (global_deaths_case_type:GlobalCaseType:Deaths {Case_Type: "Deaths"})

WITH global_deaths_case_type
OPTIONAL MATCH (global_deaths_case_type)<-[rel:IN_GLOBAL_CASE]-(old_deaths_update:Update)
WHERE NOT old_deaths_update:MostRecent
DELETE rel

WITH global_deaths_case_type, COLLECT(old_deaths_update) AS old_deaths_updates
MATCH (new_deaths_update:Update:MostRecent:Deaths)

MERGE (new_deaths_update)-[:IN_GLOBAL_CASE {Date: new_deaths_update.Date,
                                            Cases: new_deaths_update.Cases}]->(global_deaths_case_type)

WITH global_deaths_case_type, COLLECT(new_deaths_update) AS new_deaths_updates
MATCH (global_deaths_case_type)<-[rel:IN_GLOBAL_CASE]-(:Update)
WITH global_deaths_case_type, SUM(toInteger(rel.Cases)) AS global_cases
SET global_deaths_case_type.Cases = global_cases

MERGE (global_confirmed_case_type:GlobalCaseType:Confirmed {Case_Type: "Confirmed"})

WITH global_confirmed_case_type
OPTIONAL MATCH (global_confirmed_case_type)<-[rel:IN_GLOBAL_CASE]-(old_confirmed_update:Update)
WHERE NOT old_confirmed_update:MostRecent
DELETE rel

WITH global_confirmed_case_type, COLLECT(old_confirmed_update) AS old_confirmed_updates
MATCH (new_confirmed_update:Update:MostRecent:Confirmed)

MERGE (new_confirmed_update)-[:IN_GLOBAL_CASE {Date: new_confirmed_update.Date,
                                               Cases: new_confirmed_update.Cases}]->(global_confirmed_case_type)

WITH global_confirmed_case_type, COLLECT(new_confirmed_update) AS new_confirmed_updates
MATCH (global_confirmed_case_type)<-[rel:IN_GLOBAL_CASE]-(:Update)
WITH global_confirmed_case_type, SUM(toInteger(rel.Cases)) AS global_cases
SET global_confirmed_case_type.Cases = global_cases
"""

LOAD_PROGRESSION = """
MATCH (country:Country)<-[:IN_COUNTRY]-(case_type:CaseType)<-[:IN_COUNTRY_CASE]-(update:Update)

WITH country, case_type, update
ORDER BY update.Date
WITH country, case_type, COLLECT(update) AS updates

UNWIND updates AS update
WITH updates, update
WHERE NOT update:MostRecent
WITH update, updates[apoc.coll.indexOf(updates, update) + 1] AS next_update
MERGE (update)-[:NEXT_UPDATE {Cases_Difference: next_update.Difference,
                              Days_Difference: duration.inDays(update.Date, next_update.Date).Days}]->(next_update)
"""
