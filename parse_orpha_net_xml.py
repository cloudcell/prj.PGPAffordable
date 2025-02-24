import xml.etree.ElementTree as ET

import duckdb
from tqdm import tqdm


XML_FILE = './data_tmp/en_product9_prev.xml'
DB_FILE = 'orpha.net.duck.db'
TBL_NAME = 'tbl_orphaNetDisorderList'

conn = duckdb.connect(DB_FILE)

conn.execute(f'DROP TABLE IF EXISTS {TBL_NAME}')
conn.execute(f"""
CREATE TABLE IF NOT EXISTS {TBL_NAME} (
    OrphaCode STRING,
    ExpertLink STRING,
    Name STRING,
    DisorderType STRING,
    DisorderGroup STRING,
    Source STRING,
    PrevalenceType STRING,
    PrevalenceQualification STRING,
    PrevalenceClass STRING,
    ValMoy STRING,
    PrevalenceGeographic STRING,
    PrevalenceValidationStatus STRING,
);
""")

tree = ET.parse(XML_FILE)
root = tree.getroot()
disorder_list = root[1]
assert disorder_list.tag == 'DisorderList'

for disorder in tqdm(disorder_list):
    row0 = {}
    prevalence_list = []
    for field in disorder:
        if field.tag in ('DisorderType', 'DisorderGroup'):
            if len(field):
                row0[field.tag] = field[0].text
            else:
                row0[field.tag] = ''
        elif field.tag in ('OrphaCode', 'ExpertLink', 'Name'):
            row0[field.tag] = field.text
        elif field.tag == 'PrevalenceList':
            prevalence_list = field
        else:
            print(f'Unknown tag: {field.tag} (Disorder {disorder.attrib})')
            exit(1)

    for prevalence in prevalence_list:
        row1 = row0.copy()
        for field in prevalence:
            if field.tag in ('Source', 'ValMoy'):
                row1[field.tag] = field.text
            elif field.tag in ('PrevalenceType', 'PrevalenceQualification', 'PrevalenceClass',
                            'PrevalenceGeographic', 'PrevalenceValidationStatus'):
                if len(field):
                    row1[field.tag] = field[0].text
                else:
                    row1[field.tag] = ''
            else:
                print(f'Unknown tag: {field.tag} (Disorder {disorder.attrib}, Prevalence {prevalence.attrib})')
                exit(1)

        row1 = [
            row1['OrphaCode'],
            row1['ExpertLink'],
            row1['Name'],
            row1['DisorderType'],
            row1['DisorderGroup'],
            row1['Source'],
            row1['PrevalenceType'],
            row1['PrevalenceQualification'],
            row1['PrevalenceClass'],
            row1['ValMoy'],
            row1['PrevalenceGeographic'],
            row1['PrevalenceValidationStatus'],
        ]

        conn.execute(f"INSERT INTO {TBL_NAME} VALUES ({','.join(['?']*len(row1))})", row1)


conn.sql(f"SELECT * FROM {TBL_NAME} LIMIT 10").show()
print(conn.execute(f"SELECT count(*) FROM {TBL_NAME}").fetchone()[0], 'rows')

conn.close()

print(f"✅ Data successfully written to DuckDB ({DB_FILE})")
