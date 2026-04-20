# Player Review Notes

This document describes how to handle the `player_review` table — the manual resolution queue for players that scored 60–84 during entity matching.

## When does a player land here?

A player goes into `player_review` when the best candidate match in `dim_player` scores between 60 and 84 (out of 100). Common causes:

- **Name spelling / transliteration differences** — e.g. "Mohammed Salah" vs "Mohamed Salah"
- **Missing birth date** — Understat and WhoScored often omit birth dates, which costs 35 potential points
- **Common surnames** — "David Silva" may match multiple candidates with moderate confidence

## How to review

```sql
-- View unresolved cases, ordered by highest similarity first
SELECT
    pr.id,
    pr.source_name         AS scraped_name,
    pr.source_system,
    pr.source_id,
    pr.similarity_score,
    dp.canonical_name      AS suggested_match,
    dp.birth_date,
    dp.nationality,
    dp.position
FROM player_review pr
LEFT JOIN dim_player dp ON pr.suggested_canonical_id = dp.canonical_id
WHERE pr.resolved = FALSE
ORDER BY pr.similarity_score DESC;
```

## Confirming a match

```sql
-- Step 1: mark as resolved and record the confirmed canonical ID
UPDATE player_review
SET resolved = TRUE,
    canonical_id_assigned = <confirmed_canonical_id>
WHERE id = <player_review_id>;

-- Step 2: update the source ID column on dim_player
-- (replace id_understat with the appropriate column for the source)
UPDATE dim_player
SET id_understat = <source_id>
WHERE canonical_id = <confirmed_canonical_id>;
```

## Rejecting a suggested match (player is actually new)

If the suggested canonical match is wrong, insert a new `dim_player` row and mark the review resolved:

```sql
-- Insert new canonical player
INSERT INTO dim_player (canonical_name, birth_date, nationality, position, id_understat)
VALUES ('<name>', '<dob>', '<nationality>', '<position>', <source_id>);

-- Mark review resolved with the new canonical_id
UPDATE player_review
SET resolved = TRUE,
    canonical_id_assigned = LAST_INSERT_ID()
WHERE id = <player_review_id>;
```

## Known ambiguous cases

*(Populate this section after running the scrapers against real data.)*

| Review ID | Scraped name | Source | Score | Decision |
|---|---|---|---|---|
| — | — | — | — | — |
