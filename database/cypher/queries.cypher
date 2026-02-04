// --------------------------------------
// Graph Preflight / Analytics Queries
// --------------------------------------

// 1️⃣ Count total items
MATCH (i:Item)
RETURN count(i) AS total_items;

// 2️⃣ Count total sessions
MATCH (s:Session)
RETURN count(s) AS total_sessions;

// 3️⃣ Count total CONTAINS relationships (session -> item)
MATCH (:Session)-[c:CONTAINS]->(:Item)
RETURN count(c) AS total_contains_edges;

// 4️⃣ Count total CO_OCCURS relationships
MATCH ()-[co:CO_OCCURS]-()
RETURN count(co) AS total_co_occurs_edges,
       sum(co.weight) AS sum_co_occurs_weight;

// 5️⃣ Count total NEXT relationships
MATCH ()-[n:NEXT]->()
RETURN count(n) AS total_next_edges,
       sum(n.weight) AS sum_next_weight;

// 6️⃣ Optional: Top 10 items by popularity (via CO_OCCURS or CONTAINS)
MATCH (i:Item)<-[c:CONTAINS]-()
RETURN i.item_id, count(c) AS popularity
ORDER BY popularity DESC
LIMIT 10;

// 7️⃣ Optional: Top 10 co-occurring item pairs by weight
MATCH (i1:Item)-[co:CO_OCCURS]-(i2:Item)
RETURN i1.item_id AS item_1, i2.item_id AS item_2, co.weight AS weight
ORDER BY weight DESC
LIMIT 10;

// 8️⃣ Optional: Top 10 sequential pairs by NEXT weight
MATCH (i1:Item)-[n:NEXT]->(i2:Item)
RETURN i1.item_id AS prev_item, i2.item_id AS next_item, n.weight AS weight
ORDER BY n.weight DESC
LIMIT 10;

