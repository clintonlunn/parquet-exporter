-- OpenBeta Climb Export Schema
-- This SQL query transforms the raw GraphQL data into a flat table
-- Feel free to customize by adding/removing columns!

SELECT
    -- Identifiers
    CAST(uuid AS VARCHAR) AS climb_id,
    CAST(name AS VARCHAR) AS climb_name,

    -- Grades (multiple systems available)
    CAST(COALESCE(grades.yds, '') AS VARCHAR) AS grade_yds,
    CAST(COALESCE(grades.vscale, '') AS VARCHAR) AS grade_vscale,
    CAST(COALESCE(grades.french, '') AS VARCHAR) AS grade_french,

    -- Climbing type (boolean flags)
    CAST(COALESCE(type.sport, false) AS BOOLEAN) AS is_sport,
    CAST(COALESCE(type.trad, false) AS BOOLEAN) AS is_trad,
    CAST(COALESCE(type.bouldering, false) AS BOOLEAN) AS is_boulder,
    CAST(COALESCE(type.alpine, false) AS BOOLEAN) AS is_alpine,
    CAST(COALESCE(type.tr, false) AS BOOLEAN) AS is_top_rope,

    -- Location hierarchy (from pathTokens array)
    -- Adjust indices based on your needs:
    -- [0] = Country, [1] = State/Province, [2] = Region, [3] = Area, [4] = Crag
    CAST(COALESCE(list_element(pathTokens, 1), '') AS VARCHAR) AS country,
    CAST(COALESCE(list_element(pathTokens, 2), '') AS VARCHAR) AS state_province,
    CAST(COALESCE(list_element(pathTokens, 3), '') AS VARCHAR) AS region,
    CAST(COALESCE(list_element(pathTokens, 4), '') AS VARCHAR) AS area,
    CAST(COALESCE(list_element(pathTokens, 5), '') AS VARCHAR) AS crag,

    -- Full path as array (for advanced use)
    -- pathTokens AS location_path,  -- Commented out: array types not supported by many visualization tools

    -- Coordinates
    CAST(COALESCE(metadata.lat, 0.0) AS DOUBLE) AS latitude,
    CAST(COALESCE(metadata.lng, 0.0) AS DOUBLE) AS longitude,

    -- Route metadata
    CAST(COALESCE(length, 0) AS INTEGER) AS length_meters,
    CAST(COALESCE(boltsCount, 0) AS INTEGER) AS bolts_count,
    CAST(COALESCE(fa, '') AS VARCHAR) AS first_ascent,

    -- Safety rating (if available)
    CAST(COALESCE(safety, '') AS VARCHAR) AS safety,

    -- Description (comment out if not needed - makes file larger)
    CAST(COALESCE(content.description, '') AS VARCHAR) AS description

FROM climbs

-- Optional filters (uncomment to use):
-- WHERE list_element(pathTokens, 1) IN ('USA', 'Canada')
-- AND type.sport = true
-- AND grades.yds >= '5.10a'
