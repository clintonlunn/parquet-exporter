-- Extended Schema - All available climb metadata
-- Larger file, but includes everything

SELECT
    -- Identifiers
    uuid AS climb_id,
    name AS climb_name,

    -- All grade systems (cast from JSON to VARCHAR)
    COALESCE(CAST(grades.yds AS VARCHAR), '') AS grade_yds,
    COALESCE(CAST(grades.vscale AS VARCHAR), '') AS grade_vscale,
    COALESCE(CAST(grades.french AS VARCHAR), '') AS grade_french,
    COALESCE(CAST(grades.ewbank AS VARCHAR), '') AS grade_ewbank,
    COALESCE(CAST(grades.uiaa AS VARCHAR), '') AS grade_uiaa,
    COALESCE(CAST(grades.za AS VARCHAR), '') AS grade_za,
    COALESCE(CAST(grades.british AS VARCHAR), '') AS grade_british,

    -- Type flags
    COALESCE(type.sport, false) AS is_sport,
    COALESCE(type.trad, false) AS is_trad,
    COALESCE(type.bouldering, false) AS is_boulder,
    COALESCE(type.alpine, false) AS is_alpine,
    COALESCE(type.tr, false) AS is_top_rope,
    COALESCE(type.mixed, false) AS is_mixed,
    COALESCE(type.ice, false) AS is_ice,
    COALESCE(type.snow, false) AS is_snow,
    COALESCE(type.aid, false) AS is_aid,

    -- Full location hierarchy
    COALESCE(list_element(pathTokens, 1), '') AS country,
    COALESCE(list_element(pathTokens, 2), '') AS state_province,
    COALESCE(list_element(pathTokens, 3), '') AS region,
    COALESCE(list_element(pathTokens, 4), '') AS area,
    COALESCE(list_element(pathTokens, 5), '') AS crag,
    COALESCE(list_element(pathTokens, 6), '') AS sub_area,
    pathTokens AS full_location_path,

    -- Coordinates
    COALESCE(metadata.lat, 0.0) AS latitude,
    COALESCE(metadata.lng, 0.0) AS longitude,

    -- Route details
    COALESCE(length, 0) AS length_meters,
    COALESCE(boltsCount, 0) AS bolts_count,
    COALESCE(fa, '') AS first_ascent,
    COALESCE(CAST(safety AS VARCHAR), '') AS safety,

    -- Content
    COALESCE(content.description, '') AS description,
    COALESCE(content.location, '') AS location_description,
    COALESCE(content.protection, '') AS protection

FROM climbs
