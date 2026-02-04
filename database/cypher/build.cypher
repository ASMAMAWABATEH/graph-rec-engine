UNWIND $batch AS row

// 1️⃣ ALWAYS CREATE ITEM
MERGE (i:Item {item_id: row.item_id})

// 2️⃣ CONDITIONAL NEXT  
FOREACH (_ IN CASE WHEN row.next_item_id IS NOT NULL THEN [1] ELSE [] END |
    MERGE (next_i:Item {item_id: row.next_item_id})
    MERGE (i)-[r:NEXT]->(next_i)
    ON CREATE SET
        r.weight = 1,
        r.sessions = CASE WHEN row.session_id IS NOT NULL THEN [row.session_id] ELSE [] END,
        r.created_at = timestamp()
    ON MATCH SET
        r.weight = r.weight + 1,
        r.sessions = CASE
            WHEN row.session_id IS NULL OR row.session_id IN r.sessions
            THEN r.sessions
            ELSE r.sessions + [row.session_id]
        END
)

// 3️⃣ CONDITIONAL SESSION GRAPH
FOREACH (_ IN CASE WHEN row.session_id IS NOT NULL THEN [1] ELSE [] END |
    MERGE (s:Session {session_id: row.session_id})
    MERGE (s)-[:CONTAINS]->(i)
)
