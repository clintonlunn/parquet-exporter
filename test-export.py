#!/usr/bin/env python3
"""
Quick test of the export functionality with limited data
"""

import json
import requests
import duckdb

# Test with just one area
TEST_QUERY = """
query {
  area(uuid: "0f1eddf1-5a79-556e-92f6-0d91627e1f2f") {
    uuid
    area_name
    pathTokens
    metadata { lat lng }
    climbs {
      uuid
      name
      fa
      length
      boltsCount
      grades { yds vscale french }
      type { sport trad bouldering alpine tr }
      safety
      metadata { lat lng }
      content { description }
      pathTokens
    }
  }
}
"""

print("Testing OpenBeta Parquet Exporter...")
print("=" * 60)

# Fetch test data
print("\n1. Fetching test data from GraphQL API...")
response = requests.post(
    "https://api.openbeta.io/graphql",
    json={"query": TEST_QUERY},
    headers={"Content-Type": "application/json"}
)

data = response.json()
if "errors" in data:
    print(f"ERROR: {data['errors']}")
    exit(1)

area = data["data"]["area"]
climbs = area.get("climbs", [])

# Add pathTokens from area if climbs don't have them
for climb in climbs:
    if not climb.get("pathTokens"):
        climb["pathTokens"] = area.get("pathTokens", [])
    if not climb.get("metadata", {}).get("lat"):
        climb.setdefault("metadata", {})
        climb["metadata"]["lat"] = area["metadata"]["lat"]
        climb["metadata"]["lng"] = area["metadata"]["lng"]

print(f"✓ Fetched {len(climbs)} climbs from {area['area_name']}")

# Test DuckDB transformation
print("\n2. Testing DuckDB transformation...")
con = duckdb.connect(database=":memory:")
con.execute("CREATE TABLE climbs AS SELECT * FROM read_json_auto(?)", [json.dumps(climbs)])

test_schema = """
SELECT
    uuid AS climb_id,
    name AS climb_name,
    COALESCE(grades.yds, '') AS grade_yds,
    COALESCE(grades.vscale, '') AS grade_vscale,
    COALESCE(type.sport, false) AS is_sport,
    COALESCE(type.trad, false) AS is_trad,
    COALESCE(type.bouldering, false) AS is_boulder,
    COALESCE(list_element(pathTokens, 1), '') AS country,
    COALESCE(list_element(pathTokens, 2), '') AS state,
    COALESCE(metadata.lat, 0.0) AS latitude,
    COALESCE(metadata.lng, 0.0) AS longitude,
    COALESCE(length, 0) AS length_meters
FROM climbs
LIMIT 10
"""

result = con.execute(test_schema)
rows = result.fetchall()
print(f"✓ Transformed {len(rows)} climbs")
print("\nSample data:")
for row in rows[:5]:
    print(f"  {row[1]} | {row[2]} | {row[7]} | {row[8]} | {row[9]:.4f} | {row[10]:.4f}")

# Test Parquet export
print("\n3. Testing Parquet export...")
con.execute(f"COPY ({test_schema}) TO 'test-output.parquet' (FORMAT PARQUET, COMPRESSION 'snappy')")

import os
size = os.path.getsize('test-output.parquet')
print(f"✓ Created test-output.parquet ({size:,} bytes)")

# Read it back
print("\n4. Verifying Parquet file...")
verify = con.execute("SELECT COUNT(*) FROM 'test-output.parquet'").fetchone()[0]
print(f"✓ Successfully read back {verify} rows from Parquet")

print("\n" + "=" * 60)
print("✓ All tests passed!")
print("\nReady to run full export with: python export.py")
