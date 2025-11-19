-- Minimal Schema - Just the essentials for a watch/GPS device
-- Smallest file size, fastest to load

SELECT
    uuid AS climb_id,
    name AS climb_name,
    COALESCE(grades.yds, grades.vscale, '') AS grade,
    COALESCE(list_element(pathTokens, 1), '') AS country,
    COALESCE(list_element(pathTokens, 2), '') AS state,
    COALESCE(metadata.lat, 0.0) AS latitude,
    COALESCE(metadata.lng, 0.0) AS longitude

FROM climbs
WHERE metadata.lat IS NOT NULL
  AND metadata.lng IS NOT NULL
