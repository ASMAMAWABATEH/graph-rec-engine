// ---------------------------
// Node uniqueness constraints
// ---------------------------

CREATE CONSTRAINT item_id_unique IF NOT EXISTS
FOR (i:Item)
REQUIRE i.item_id IS UNIQUE;

CREATE CONSTRAINT session_id_unique IF NOT EXISTS
FOR (s:Session)
REQUIRE s.session_id IS UNIQUE;

// ---------------------------
// Node indexes for fast lookup
// ---------------------------

CREATE INDEX item_node_idx IF NOT EXISTS
FOR (i:Item)
ON (i.item_id);

CREATE INDEX session_node_idx IF NOT EXISTS
FOR (s:Session)
ON (s.session_id);

// ---------------------------
// Relationship indexes
// ---------------------------

// CO_OCCURS relationship for RIC co-occurrence queries
CREATE INDEX co_occurs_weight_idx IF NOT EXISTS
FOR ()-[c:CO_OCCURS]-()
ON (c.weight);

// NEXT relationship for HSP sequential traversal (optional)
CREATE INDEX next_weight_idx IF NOT EXISTS
FOR ()-[n:NEXT]->()
ON (n.weight);
