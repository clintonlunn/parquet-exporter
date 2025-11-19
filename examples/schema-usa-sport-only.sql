-- USA Sport Climbs Only
-- Filtered to just sport routes in the United States

SELECT
    uuid AS climb_id,
    name AS climb_name,
    grades.yds AS grade,

    list_element(pathTokens, 2) AS state,
    list_element(pathTokens, 3) AS region,
    list_element(pathTokens, 4) AS area,
    list_element(pathTokens, 5) AS crag,

    metadata.lat AS latitude,
    metadata.lng AS longitude,

    length AS length_meters,
    boltsCount AS bolts_count,
    safety

FROM climbs
WHERE list_element(pathTokens, 1) = 'USA'
  AND type.sport = true
  AND metadata.lat IS NOT NULL
  AND metadata.lng IS NOT NULL
