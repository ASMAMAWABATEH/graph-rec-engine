// Load CO_OCCURS pre-aggregated from session membership.

MATCH ()-[r:CO_OCCURS]->()
CALL {
  WITH r
  DELETE r
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM 'file:///cooccurs_typed.csv' AS row
FIELDTERMINATOR '\t'
CALL {
  WITH row
  MATCH (src:Item {item_id: toInteger(row.src_item_id)})
  MATCH (dst:Item {item_id: toInteger(row.dst_item_id)})
  CREATE (src)-[:CO_OCCURS {
    weight: toInteger(row.weight),
    updated_at: timestamp()
}]->(dst)
} IN TRANSACTIONS OF 2000 ROWS;
