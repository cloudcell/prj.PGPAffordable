"""
This script is used to get the unique types of actions from the generated intermediary database.
"""

import duckdb


ACTION_TYPES = {
    'ACTIVATOR': 1,
    'AGONIST': 1,
    'ALLOSTERIC ANTAGONIST': -1,
    'ANTAGONIST': -1,
    'ANTISENSE INHIBITOR': -1,
    'BINDING AGENT': 1,
    'BLOCKER': -1,
    'CROSS-LINKING AGENT': 1,
    'DEGRADER': -1,
    'DISRUPTING AGENT': -1,
    'EXOGENOUS GENE': 1,
    'EXOGENOUS PROTEIN': 1,
    'HYDROLYTIC ENZYME': 1,
    'INHIBITOR': -1,
    'INVERSE AGONIST': -1,
    'MODULATOR': 1,
    'NEGATIVE ALLOSTERIC MODULATOR': -1,
    'NEGATIVE MODULATOR': -1,
    'OPENER': 1,
    'OTHER': 0.00001,
    'PARTIAL AGONIST': 1,
    'POSITIVE ALLOSTERIC MODULATOR': 1,
    'POSITIVE MODULATOR': 1,
    'PROTEOLYTIC ENZYME': 1,
    'RELEASING AGENT': 1,
    'RNAI INHIBITOR': -1,
    'STABILISER': 1,
    'SUBSTRATE': 1,
    'VACCINE ANTIGEN': -1,
}

db_path = "bio_data.duck.db"
con = duckdb.connect(db_path)

action_type_list = [row[0] for row in con.execute('SELECT DISTINCT actionType FROM tbl_actions').fetchall()]

for actionType in sorted(action_type_list):
    print(actionType)
    v = ACTION_TYPES.get(actionType)
    params = {'actionType': actionType, 'value': v}
    con.execute('INSERT OR REPLACE INTO tbl_action_types VALUES ($actionType, $value);', params)

con.sql('SELECT * FROM tbl_action_types').show()

con.close()
