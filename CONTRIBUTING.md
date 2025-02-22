(draft)


Database Naming Conventions
===========================

* All table names shall have a prefix "tbl_"
* Fields will have a prefix "fld_" only to avoid ambiguity with table names (for example the data from opentargets.org's table knownDrugsAggregated if stored as json in another table will reside in a field named fld_knownDrugsAggregated, to avoid ambiguity).

