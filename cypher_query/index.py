LOAD_UPDATE_INDEX = """
CREATE INDEX FOR (update:Update) ON (update.Country_Region, update.Case_Type, update.Date)
"""

LOAD_COUNTRY_INDEX = """
CREATE INDEX FOR (country:Country) ON (country.Country_Region)
"""

LOAD_CASE_TYPE_INDEX = """
CREATE INDEX FOR (case_type:CaseType) ON (case_type.Case_Type, case_type.Country_Region)
"""

LOAD_DATE_INDEX = """
CREATE INDEX FOR (date:Date) ON (date.Date)
"""
