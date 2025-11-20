-- OpenBeta Climb Export Schema
-- This SQL query transforms the raw GraphQL data into a flat table
-- Feel free to customize by adding/removing columns!

SELECT
    -- Identifiers
    uuid AS climb_id,
    name AS climb_name,

    -- Grades (multiple systems available)
    COALESCE(grades.yds, '') AS grade_yds,
    COALESCE(grades.vscale, '') AS grade_vscale,
    COALESCE(grades.french, '') AS grade_french,

    -- Climbing type (boolean flags)
    COALESCE(type.sport, false) AS is_sport,
    COALESCE(type.trad, false) AS is_trad,
    COALESCE(type.bouldering, false) AS is_boulder,
    COALESCE(type.alpine, false) AS is_alpine,
    COALESCE(type.tr, false) AS is_top_rope,

    -- Location hierarchy (from pathTokens array)
    -- Adjust indices based on your needs:
    -- [0] = Country, [1] = State/Province, [2] = Region, [3] = Area, [4] = Crag
    COALESCE(list_element(pathTokens, 1), '') AS country,
    COALESCE(list_element(pathTokens, 2), '') AS state_province,
    COALESCE(list_element(pathTokens, 3), '') AS region,
    COALESCE(list_element(pathTokens, 4), '') AS area,
    COALESCE(list_element(pathTokens, 5), '') AS crag,

    -- Full path as array (for advanced use)
    -- pathTokens AS location_path,  -- Commented out: array types not supported by many visualization tools

    -- Coordinates
    COALESCE(metadata.lat, 0.0) AS latitude,
    COALESCE(metadata.lng, 0.0) AS longitude,

    -- Route metadata
    COALESCE(length, 0) AS length_meters,
    COALESCE(boltsCount, 0) AS bolts_count,
    COALESCE(fa, '') AS first_ascent,

    -- Safety rating (if available)
    COALESCE(safety, '') AS safety,

    -- Description (comment out if not needed - makes file larger)
    COALESCE(content.description, '') AS description

FROM climbs

-- Optional filters (uncomment to use):
-- WHERE list_element(pathTokens, 1) IN ('USA', 'Canada')
-- AND type.sport = true
-- AND grades.yds >= '5.10a'
