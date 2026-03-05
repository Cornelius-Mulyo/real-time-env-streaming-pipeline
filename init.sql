CREATE TABLE IF NOT EXISTS sensor_readings (
  event_time TIMESTAMPTZ NOT NULL,
  sensor_id TEXT NOT NULL,
  location TEXT NOT NULL,
  ph DOUBLE PRECISION,
  turbidity DOUBLE PRECISION,
  dissolved_oxygen DOUBLE PRECISION,
  temperature_c DOUBLE PRECISION,
  conductivity DOUBLE PRECISION,
  ingestion_ts TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sensor_time
ON sensor_readings(sensor_id, event_time);

CREATE TABLE IF NOT EXISTS sensor_alerts (
  alert_time TIMESTAMPTZ NOT NULL,
  sensor_id TEXT NOT NULL,
  location TEXT NOT NULL,
  alert_type TEXT NOT NULL,
  metric DOUBLE PRECISION,
  threshold DOUBLE PRECISION,
  message TEXT
);