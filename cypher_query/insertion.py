LOAD_UPDATES = """
LOAD CSV WITH HEADERS FROM "file:///COVID-19Cases.csv" AS line

WITH line, SPLIT(line.Date, "/") AS split_date
WITH apoc.map.removeKey(line, 'Date') AS line, DATE({month: toInteger(split_date[0]),
                                                     day: toInteger(split_date[1]),
                                                     year: toInteger(split_date[2])}) AS date

MERGE (update:Update {Country_Region: line.Country_Region, Case_Type: line.Case_Type, Date: date,
                      Province_State: line.Province_State})
SET update += line

WITH line, update
CALL apoc.create.addLabels([update], [line.Case_Type])
YIELD node
RETURN DISTINCT 1
"""

LOAD_COUNTRIES = """
MATCH (update:Update)
WHERE NOT (update)-[:IN_COUNTRY_CASE]->(:CaseType)

MERGE (country:Country {Country_Region: update.Country_Region, Province_State: update.Province_State})
ON CREATE SET country.Lat = update.Lat, country.Long = update.Long
MERGE (case_type: CaseType {Case_Type: update.Case_Type, Country_Region: country.Country_Region,
       Province_State: country.Province_State})
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

LOAD_GLOBAL_RECENT_UPDATES = """
MERGE (global_deaths_recent_update:GlobalRecentUpdate:Deaths {Case_Type: "Deaths"})

WITH global_deaths_recent_update
OPTIONAL MATCH (global_deaths_recent_update)<-[rel:IN_GLOBAL_RECENT_UPDATE]-(old_deaths_update:Update)
WHERE NOT old_deaths_update:MostRecent
DELETE rel

WITH global_deaths_recent_update, COLLECT(old_deaths_update) AS old_deaths_updates
MATCH (new_deaths_update:Update:MostRecent:Deaths)

MERGE (new_deaths_update)-[:IN_GLOBAL_RECENT_UPDATE {Date: new_deaths_update.Date,
                                            Cases: new_deaths_update.Cases}]->(global_deaths_recent_update)

WITH global_deaths_recent_update, COLLECT(new_deaths_update) AS new_deaths_updates
MATCH (global_deaths_recent_update)<-[rel:IN_GLOBAL_RECENT_UPDATE]-(:Update)
WITH global_deaths_recent_update, SUM(toInteger(rel.Cases)) AS global_cases
SET global_deaths_recent_update.Cases = global_cases

MERGE (global_confirmed_recent_update:GlobalRecentUpdate:Confirmed {Case_Type: "Confirmed"})

WITH global_confirmed_recent_update
OPTIONAL MATCH (global_confirmed_recent_update)<-[rel:IN_GLOBAL_RECENT_UPDATE]-(old_confirmed_update:Update)
WHERE NOT old_confirmed_update:MostRecent
DELETE rel

WITH global_confirmed_recent_update, COLLECT(old_confirmed_update) AS old_confirmed_updates
MATCH (new_confirmed_update:Update:MostRecent:Confirmed)

MERGE (new_confirmed_update)-[:IN_GLOBAL_RECENT_UPDATE {Date: new_confirmed_update.Date,
                                               Cases: new_confirmed_update.Cases}]->(global_confirmed_recent_update)

WITH global_confirmed_recent_update, COLLECT(new_confirmed_update) AS new_confirmed_updates
MATCH (global_confirmed_recent_update)<-[rel:IN_GLOBAL_RECENT_UPDATE]-(:Update)
WITH global_confirmed_recent_update, SUM(toInteger(rel.Cases)) AS global_cases
SET global_confirmed_recent_update.Cases = global_cases
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

LOAD_GLOBAL_CASE_TYPES = """
MERGE (global_deaths_case_type:GlobalCaseType:Deaths {Case_Type: "Deaths"})

WITh global_deaths_case_type
MATCH (update:Update:Deaths)-[:IN_DATE]->(date:Date)

WITH global_deaths_case_type, date, SUM(toInteger(update.Cases)) AS updates_cases
MERGE (global_update:GlobalUpdate:Deaths {Date: date.Date, Case_Type: "Deaths"})
SET global_update.Cases = updates_cases

MERGE (global_update)-[:IN_GLOBAL_CASE {Date: date.Date, Cases: updates_cases}]->(global_deaths_case_type)
MERGE (global_update)-[:IN_DATE {Date: date.Date}]->(date)
WITH COLLECT(global_update) AS global_updates

MERGE (global_confirmed_case_type:GlobalCaseType:Confirmed {Case_Type: "Confirmed"})

WITh global_confirmed_case_type
MATCH (update:Update:Confirmed)-[:IN_DATE]->(date:Date)

WITH global_confirmed_case_type, date, SUM(toInteger(update.Cases)) AS updates_cases
MERGE (global_update:GlobalUpdate:Confirmed {Date: date.Date, Case_Type: "Confirmed"})
SET global_update.Cases = updates_cases

MERGE (global_update)-[:IN_GLOBAL_CASE {Date: date.Date, Cases: updates_cases}]->(global_confirmed_case_type)
MERGE (global_update)-[:IN_DATE {Date: date.Date}]->(date)
"""

LOAD_GLOBAL_PROGRESSION = """
MATCH (global_case_type:GlobalCaseType)<-[:IN_GLOBAL_CASE]-(global_update:GlobalUpdate)

WITH global_case_type, global_update
ORDER BY global_update.Date DESC
WITH global_case_type, COLLECT(global_update) AS global_updates
WITH global_case_type, global_updates[0] AS most_recent_global_update
SET most_recent_global_update:MostRecent
SET global_case_type.Last_Update = most_recent_global_update.Date, global_case_type.Cases = most_recent_global_update.Cases

WITh global_case_type
MATCH (global_case_type)<-[:IN_GLOBAL_CASE]-(global_update:GlobalUpdate)

WITH global_case_type, global_update
ORDER BY global_update.Date
WITH global_case_type, COLLECT(global_update) AS global_updates

UNWIND global_updates AS global_update
WITH global_updates, global_update
WHERE NOT global_update:MostRecent
WITH global_update, global_updates[apoc.coll.indexOf(global_updates, global_update) + 1] AS next_global_update
WITH global_update, next_global_update, toInteger(next_global_update.Cases) - toInteger(global_update.Cases) AS difference
SET next_global_update.Difference = difference
MERGE (global_update)-[:NEXT_UPDATE {Cases_Difference: next_global_update.Difference,
                                     Days_Difference: duration.inDays(global_update.Date, next_global_update.Date).Days}]
                     ->(next_global_update)
"""

LOAD_MOST_CASES_FOR_DATE = """
OPTIONAL MATCH (update:Update)
WHERE update:MostDeathsForDate OR update:MostConfirmedForDate
REMOVE update:MostDeathsForDate, update:MostConfirmedForDate
WITH COLLECT(update) AS updates

MATCH (date:Date)

MATCH (date)<-[:IN_DATE]-(update:Update:Deaths)

WITH date, update
ORDER BY update.Cases DESC
WITH date, COLLECT(update) AS updates
WITh date, updates[0] AS most_deaths_update

SET most_deaths_update:MostDeathsForDate

WITH date
MATCH (date)<-[:IN_DATE]-(update:Update:Confirmed)

WITH date, update
ORDER BY update.Cases DESC
WITH date, COLLECT(update) AS updates
WITh date, updates[0] AS most_confirmed_update

SET most_confirmed_update:MostConfirmedForDate
"""

LOAD_PEOPLE = """
MATCH (update:Update)

MERGE (people:People)-[:FROM_UPDATE]->(update)
ON CREATE SET people.Cases = 0

WITH update, people
WHERE toInteger(people.Cases) < toInteger(update.Difference)

UNWIND RANGE(toInteger(people.Cases), toInteger(update.Difference) - 1) AS case_number
CREATE (person:Person {Lat: update.Lat, Long: update.Long})
CREATE (person)-[:GROUP_BY]->(people)

WITH update, people, COLLECT(person) AS persons
SET people.Cases = update.Difference
"""
