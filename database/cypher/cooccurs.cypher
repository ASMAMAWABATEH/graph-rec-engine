// ======================================  
// CO_OCCURS - Item Co-occurrence Engine (Batch Version)
// ======================================

// Process sessions in batches to avoid memory overload
// Adjust batch size depending on your RAM (start with 10_000)

:param batch_size => 10000;  // Number of sessions per batch
:param skip_sessions => 0;    // Initial offset, increase per batch

MATCH (s:Session)
WITH s
ORDER BY id(s)
SKIP $skip_sessions
LIMIT $batch_size

// Generate all unique Item pairs within each session
MATCH (s)-[:CONTAINS]->(i1:Item)
MATCH (s)-[:CONTAINS]->(i2:Item)
WHERE id(i1) < id(i2)  // Prevents duplicate pairs (A→B and B→A)

// Merge or update the co-occurrence relationship
WITH i1, i2
MERGE (i1)-[r:CO_OCCURS]->(i2)
  ON CREATE SET 
    r.weight = 1,
    r.created_at = timestamp()
  ON MATCH SET 
    r.weight = r.weight + 1;
