#!/bin/sh
# Sourced by setup.sh; expects METABASE_URL, SESSION_ID and DB_ID.
set -o pipefail

api() {
  curl -sSf -X "$1" "$METABASE_URL$2" \
    -H "Content-Type: application/json" \
    -H "X-Metabase-Session: $SESSION_ID" \
    -d @-
}

# create_card <name> <display> <sql> [visualization_settings]
create_card() {
  viz=${4:-'{}'}
  jq -n \
    --arg name "$1" \
    --arg display "$2" \
    --arg sql "$3" \
    --argjson viz "$viz" \
    --argjson db "$DB_ID" \
    '{
      name: $name,
      display: $display,
      visualization_settings: $viz,
      dataset_query: {type: "native", database: $db, native: {query: $sql}}
    }' | api POST /api/card | jq -r '.id'
}

LATEST_BIKES="WITH latest AS (
    SELECT DISTINCT ON (bike_point_id) *
    FROM gold.fct_bikes_availability_change
    ORDER BY bike_point_id, updated_at DESC
)"

LATEST_CONNECTORS="WITH latest AS (
    SELECT DISTINCT ON (connector_id) *
    FROM gold.fct_connector_availability_change
    ORDER BY connector_id, updated_at DESC
)"

ACTIVE_SEGMENTS="FROM gold.fct_disrupted_segment f
JOIN gold.dim_disruption d USING (disruption_id)
JOIN gold.dim_street_segment s USING (disrupted_segment_id)
WHERE now() BETWEEN f.start_date_time AND f.end_date_time"

BIKE_TOTALS="$LATEST_BIKES
SELECT
    sum(nb_standard_bikes) AS standard_bikes,
    sum(nb_e_bikes) AS e_bikes,
    sum(nb_empty_docks) AS empty_docks,
    count(*) FILTER (WHERE nb_standard_bikes + nb_e_bikes = 0) AS empty_stations
FROM latest"

echo "Creating dashboard cards..."

STANDARD_BIKES=$(create_card "Standard bikes available" scalar "$BIKE_TOTALS" '{"scalar.field": "standard_bikes"}')
E_BIKES=$(create_card "E-bikes available" scalar "$BIKE_TOTALS" '{"scalar.field": "e_bikes"}')
EMPTY_DOCKS=$(create_card "Empty docks" scalar "$BIKE_TOTALS" '{"scalar.field": "empty_docks"}')
EMPTY_STATIONS=$(create_card "Stations with no bikes" scalar "$BIKE_TOTALS" '{"scalar.field": "empty_stations"}')

STATION_STATUS=$(create_card "Docking stations by status" row "$LATEST_BIKES
SELECT
    CASE
        WHEN nb_standard_bikes + nb_e_bikes = 0 THEN 'No bikes'
        WHEN nb_empty_docks = 0 THEN 'No free docks'
        ELSE 'Available'
    END AS status,
    count(*) AS stations
FROM latest
GROUP BY 1
ORDER BY 2 DESC" '{"graph.dimensions": ["status"], "graph.metrics": ["stations"]}')

BIKES_MAP=$(create_card "Stations with bikes" map "$LATEST_BIKES
SELECT b.common_name, b.lat, b.lon, l.nb_standard_bikes AS standard_bikes, l.nb_e_bikes AS e_bikes
FROM latest l
JOIN gold.dim_bike_point b USING (bike_point_id)
WHERE l.nb_standard_bikes + l.nb_e_bikes > 0" \
  '{"map.type": "pin", "map.latitude_column": "lat", "map.longitude_column": "lon"}')

CONNECTOR_STATUS=$(create_card "EV connectors by status" row "$LATEST_CONNECTORS
SELECT status, count(*) AS connectors
FROM latest
GROUP BY 1
ORDER BY 2 DESC" '{"graph.dimensions": ["status"], "graph.metrics": ["connectors"]}')

CHARGER_MAP=$(create_card "Charging stations with free connectors" map "$LATEST_CONNECTORS
SELECT
    s.station_name,
    s.lat,
    s.lon,
    count(*) AS connectors,
    count(*) FILTER (WHERE l.status = 'Available') AS free_connectors
FROM latest l
JOIN gold.dim_charging_station s USING (charging_station_id)
GROUP BY s.charging_station_id, s.station_name, s.lat, s.lon
HAVING count(*) FILTER (WHERE l.status = 'Available') > 0" \
  '{"map.type": "pin", "map.latitude_column": "lat", "map.longitude_column": "lon"}')

OUT_OF_SERVICE=$(create_card "Out-of-service EV connectors" table "$LATEST_CONNECTORS
SELECT s.station_name, c.connector_type, c.power_kw, l.updated_at AS last_updated
FROM latest l
JOIN gold.dim_connector c USING (connector_id)
JOIN gold.dim_charging_station s USING (charging_station_id)
WHERE l.status = 'OutOfService'
ORDER BY l.updated_at")

DISRUPTIONS=$(create_card "Active road disruptions" bar "SELECT d.severity, d.category, count(DISTINCT f.disruption_id) AS disruptions
$ACTIVE_SEGMENTS
GROUP BY 1, 2" \
  '{"graph.dimensions": ["severity", "category"], "graph.metrics": ["disruptions"], "stackable.stack_type": "stacked"}')

DISRUPTION_MAP=$(create_card "Disrupted street segments" map "SELECT s.street_name, d.severity, d.category, s.start_lat AS lat, s.start_lon AS lon
$ACTIVE_SEGMENTS" \
  '{"map.type": "pin", "map.latitude_column": "lat", "map.longitude_column": "lon"}')

REJECTS=$(create_card "Rejected records" table "SELECT source, stage, error, count(*) AS records, max(rejected_at) AS last_seen
FROM silver.int_rejected_records
GROUP BY 1, 2, 3
ORDER BY records DESC")

echo "Creating dashboard..."

DASHBOARD_ID=$(jq -n '{name: "London Transport"}' | api POST /api/dashboard | jq -r '.id')

# card_id row col size_x size_y on Metabase's 24-column grid
echo "$STANDARD_BIKES 0 0 6 3
$E_BIKES 0 6 6 3
$EMPTY_DOCKS 0 12 6 3
$EMPTY_STATIONS 0 18 6 3
$STATION_STATUS 3 0 8 7
$BIKES_MAP 3 8 16 7
$CONNECTOR_STATUS 10 0 8 7
$CHARGER_MAP 10 8 16 7
$OUT_OF_SERVICE 17 0 24 5
$DISRUPTIONS 22 0 8 7
$DISRUPTION_MAP 22 8 16 7
$REJECTS 29 0 24 5" | jq -R -s '{
  dashcards: [
    split("\n")[] | select(length > 0) | split(" ") | map(tonumber)
    | {card_id: .[0], row: .[1], col: .[2], size_x: .[3], size_y: .[4]}
  ] | to_entries | map(.value + {id: -(.key + 1)})
}' | api PUT "/api/dashboard/$DASHBOARD_ID" >/dev/null

jq -n --argjson id "$DASHBOARD_ID" '{"custom-homepage": true, "custom-homepage-dashboard": $id}' \
  | api PUT /api/setting >/dev/null

echo "Dashboard created."
