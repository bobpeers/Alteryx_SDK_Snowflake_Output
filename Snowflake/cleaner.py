# list all Snowflake reserved words (https://docs.snowflake.com/en/sql-reference/reserved-keywords.html)
# if field has leading/trailing spaces remove
# if field has spaces then quote
# if field is reserved then quote
import re

def sanitise_inputs(data: str):
    if data:
        data = None if data.strip() == '' else data
    return data

def reserved_words(field: str, case_sensitive: bool) -> str:
    reserved_list: list = ['ACCOUNT',
                    'ALL',
                    'ALTER',
                    'AND',
                    'ANY',
                    'AS',
                    'BETWEEN',
                    'BY',
                    'CASE',
                    'CAST',
                    'CHECK',
                    'COLUMN',
                    'CONNECT',
                    'CONNECTION',
                    'CONSTRAINT',
                    'CREATE',
                    'CROSS',
                    'CURRENT',
                    'CURRENT_DATE',
                    'CURRENT_TIME',
                    'CURRENT_TIMESTAMP',
                    'CURRENT_USER',
                    'DATABASE',
                    'DELETE',
                    'DISTINCT',
                    'DROP',
                    'ELSE',
                    'EXISTS',
                    'FALSE',
                    'FOLLOWING',
                    'FOR',
                    'FROM',
                    'FULL',
                    'GRANT',
                    'GROUP',
                    'GSCLUSTER',
                    'HAVING',
                    'ILIKE',
                    'IN',
                    'INCREMENT',
                    'INNER',
                    'INSERT',
                    'INTERSECT',
                    'INTO',
                    'IS',
                    'ISSUE',
                    'JOIN',
                    'LATERAL',
                    'LEFT',
                    'LIKE',
                    'LOCALTIME',
                    'LOCALTIMESTAMP',
                    'MINUS',
                    'NATURAL',
                    'NOT',
                    'NULL',
                    'OF',
                    'ON',
                    'OR',
                    'ORDER',
                    'ORGANIZATION',
                    'QUALIFY',
                    'REGEXP',
                    'REVOKE',
                    'RIGHT',
                    'RLIKE',
                    'ROW',
                    'ROWS',
                    'SAMPLE',
                    'SCHEMA',
                    'SELECT',
                    'SET',
                    'SOME',
                    'START',
                    'TABLE',
                    'TABLESAMPLE',
                    'THEN',
                    'TO',
                    'TRIGGER',
                    'TRUE',
                    'TRY_CAST',
                    'UNION',
                    'UNIQUE',
                    'UPDATE',
                    'USING',
                    'VALUES',
                    'VIEW',
                    'WHEN',
                    'WHENEVER',
                    'WHERE',
                    'WITH']

    # regex for valid object name
    p = re.compile('^[A-Za-z_][A-Za-z0-9_$]{1,254}$')
    # limit to 255 chars
    field = field.strip()[:255]

    if case_sensitive == True:
        return f'"{field}"'
    elif not p.match(field):
        return f'"{field}"'
    elif field.upper() in reserved_list:
        return f'"{field.upper()}"'
    else:
        return field