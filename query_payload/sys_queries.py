
query = """BUSINESS_QUERY"""

query2 =  """BUSINESS_QUERY"""

search_case = """BUSINESS_QUERY"""

get_last_cases = """SELECT (COLUMNS))          
           FROM (DATABASE) p
           LEFT JOIN (SELECT row_number() over (PARTITION BY X ORDER BY r.Y desc) as rownum, r.Z,r.C,r.B,r.A, r.E FROM (DATABASE) r) r
           ON p.event_id = r.event_id and r.rownum = 1
           WHERE p.COLUMN = 'N'
           AND r.COLUMN is null
           AND p.COLUMN is null
           AND p.COLUMN_DATE >= ?"""
