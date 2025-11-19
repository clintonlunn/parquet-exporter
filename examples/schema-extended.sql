-- Extended Schema - All available climb metadata
-- Larger file, but includes everything

SELECT
    -- Identifiers
    uuid AS climb_id,
    name AS climb_name,

    -- All grade systems
    COALESCE(grades.yds, '') AS grade_yds,
    COALESCE(grades.vscale, '') AS grade_vscale,
    COALESCE(grades.french, '') AS grade_french,
    COALESCE(grades.ewbank, '') AS grade_ewbank,
    COALESCE(grades.uiaa, '') AS grade_uiaa,
    COALESCE(grades.za, '') AS grade_za,
    COALESCE(grades.british, '') AS grade_british,

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
    COALESCE(safety, '') AS safety,

    -- Content
    COALESCE(content.description, '') AS description,
    COALESCE(content.location, '') AS location_description,
    COALESCE(content.protection, '') AS protection

FROM climbs
