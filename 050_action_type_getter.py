"""
This script is used to get the unique types of actions from the generated intermediary database.
"""

import duckdb


ACTION_TYPES = {
    'ACTIVATOR': 0,
    'AGONIST': 0,
    'ALLOSTERIC ANTAGONIST': 0,
    'ANTAGONIST': 0,
    'ANTISENSE INHIBITOR': 0,
    'BINDING AGENT': 0,
    'BLOCKER': 0,
    'CROSS-LINKING AGENT': 0,
    'DEGRADER': 0,
    'DISRUPTING AGENT': 0,
    'EXOGENOUS GENE': 0,
    'EXOGENOUS PROTEIN': 0,
    'HYDROLYTIC ENZYME': 0,
    'INHIBITOR': 0,
    'INVERSE AGONIST': 0,
    'MODULATOR': 0,
    'NEGATIVE ALLOSTERIC MODULATOR': 0,
    'NEGATIVE MODULATOR': 0,
    'OPENER': 0,
    'OTHER': 0,
    'PARTIAL AGONIST': 0,
    'POSITIVE ALLOSTERIC MODULATOR': 0,
    'POSITIVE MODULATOR': 0,
    'PROTEOLYTIC ENZYME': 0,
    'RELEASING AGENT': 0,
    'RNAI INHIBITOR': 0,
    'STABILISER': 0,
    'SUBSTRATE': 0,
    'VACCINE ANTIGEN': 0,
}

db_path = "bio_data.duck.db"
con = duckdb.connect(db_path)

action_type_list = [row[0] for row in con.execute('SELECT DISTINCT actionType FROM actions').fetchall()]

for actionType in sorted(action_type_list):
    print(actionType)
    v = ACTION_TYPES.get(actionType)
    params = {'actionType': actionType, 'value': v}
    con.execute('INSERT INTO action_types VALUES ($actionType, $value) ON CONFLICT DO UPDATE SET value = EXCLUDED.value;', params)

con.sql('SELECT * FROM action_types').show()

con.close()
