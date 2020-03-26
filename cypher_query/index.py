LOAD_UPDATE_INDEX = """
CREATE INDEX FOR (update:Update) ON (update.Country_Region, update.Case_Type, update.Date, update.Province_State)
"""

LOAD_COUNTRY_INDEX = """
CREATE INDEX FOR (country:Country) ON (country.Country_Region, country.Province_State)
"""

LOAD_CASE_TYPE_INDEX = """
CREATE INDEX FOR (case_type:CaseType) ON (case_type.Case_Type, case_type.Country_Region, case_type.Province_State)
"""

LOAD_DATE_INDEX = """
CREATE INDEX FOR (date:Date) ON (date.Date)
"""

LOAD_GLOBAL_UPDATE_INDEX = """
CREATE INDEX FOR (global_update:GlobalUpdate) ON (global_update.Date, global_update.Case_Type)
"""
