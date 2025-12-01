#!/usr/bin/env python3
"""Convert parquet to JSON or GeoJSON."""

import json
import sys
from uuid import UUID
import duckdb

class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, UUID):
            return str(o)
        return super().default(o)

def main():
    if len(sys.argv) < 2:
        print("Usage: python parquet2json.py <output.json|output.geojson> [input.parquet]")
        sys.exit(1)

    output = sys.argv[1]
    input_file = sys.argv[2] if len(sys.argv) > 2 else "openbeta-climbs.parquet"

    if output.endswith(".geojson"):
        rows = duckdb.execute(f"SELECT * FROM '{input_file}' WHERE latitude IS NOT NULL").fetchall()
        cols = [d[0] for d in duckdb.execute(f"DESCRIBE SELECT * FROM '{input_file}'").fetchall()]

        features = []
        for row in rows:
            props = dict(zip(cols, row))
            lat, lng = props.pop("latitude"), props.pop("longitude")
            features.append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lng, lat]},
                "properties": props
            })

        with open(output, "w") as f:
            json.dump({"type": "FeatureCollection", "features": features}, f, cls=JSONEncoder)
    else:
        duckdb.execute(f"COPY (SELECT * FROM '{input_file}') TO '{output}'")

    print(f"Wrote {output}")

if __name__ == "__main__":
    main()
