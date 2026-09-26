-- Natural keys keep this small mart easy to follow; no surrogate IDs are needed.
CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS mart.dim_property (
    property_id TEXT PRIMARY KEY CHECK (property_id <> ''),
    property_name TEXT NOT NULL CHECK (property_name <> ''),
    city TEXT NOT NULL CHECK (city <> '')
);

CREATE TABLE IF NOT EXISTS mart.dim_month (
    month DATE PRIMARY KEY CHECK (EXTRACT(DAY FROM month) = 1)
);

CREATE TABLE IF NOT EXISTS mart.fact_property_month (
    property_id TEXT REFERENCES mart.dim_property(property_id) NOT NULL,
    month DATE REFERENCES mart.dim_month(month) NOT NULL,
    total_units INTEGER NOT NULL CHECK (total_units >= 0),
    occupied_units INTEGER NOT NULL CHECK (occupied_units BETWEEN 0 AND total_units),
    earned_monthly_rent_usd NUMERIC(18,2) NOT NULL CHECK (earned_monthly_rent_usd >= 0),
    resolved_orders INTEGER NOT NULL CHECK (resolved_orders >= 0),
    sla_eligible_orders INTEGER NOT NULL CHECK (sla_eligible_orders = resolved_orders),
    sla_compliant_orders INTEGER NOT NULL CHECK (sla_compliant_orders BETWEEN 0 AND sla_eligible_orders),
    sla_noncompliant_orders INTEGER NOT NULL CHECK (sla_noncompliant_orders = resolved_orders - sla_compliant_orders),
    missing_response_orders INTEGER NOT NULL CHECK (missing_response_orders BETWEEN 0 AND sla_noncompliant_orders),
    open_orders INTEGER NOT NULL CHECK (open_orders >= 0),
    energy_kwh NUMERIC(18,2) NOT NULL CHECK (energy_kwh >= 0),
    floor_area_sqm NUMERIC(18,2) CHECK (floor_area_sqm >= 0),
    -- Generated rates always agree with their counts. NULLIF prevents division by zero.
    occupancy_rate NUMERIC GENERATED ALWAYS AS
        (occupied_units::NUMERIC / NULLIF(total_units, 0)) STORED,
    sla_compliance_rate NUMERIC GENERATED ALWAYS AS
        (sla_compliant_orders::NUMERIC / NULLIF(sla_eligible_orders, 0)) STORED,
    energy_intensity_kwh_sqm NUMERIC GENERATED ALWAYS AS
        (energy_kwh / NULLIF(floor_area_sqm, 0)) STORED,
    PRIMARY KEY (property_id, month)
);

-- The primary key already indexes property then month. This index helps month-only queries.
CREATE INDEX IF NOT EXISTS fact_month_idx ON mart.fact_property_month(month);
CREATE INDEX IF NOT EXISTS property_city_idx ON mart.dim_property(city);

-- Portfolio rates divide totals, rather than averaging property percentages.
CREATE OR REPLACE VIEW mart.portfolio_dashboard AS
SELECT month,
       SUM(total_units) AS total_units,
       SUM(occupied_units) AS occupied_units,
       SUM(occupied_units)::NUMERIC / NULLIF(SUM(total_units), 0) AS occupancy_rate,
       SUM(earned_monthly_rent_usd) AS earned_monthly_rent_usd,
       SUM(sla_eligible_orders) AS sla_eligible_orders,
       SUM(sla_compliant_orders) AS sla_compliant_orders,
       SUM(open_orders) AS open_orders,
       SUM(sla_compliant_orders)::NUMERIC / NULLIF(SUM(sla_eligible_orders), 0) AS sla_compliance_rate,
       SUM(energy_kwh) AS energy_kwh,
       SUM(floor_area_sqm) AS floor_area_sqm,
       SUM(energy_kwh) / NULLIF(SUM(floor_area_sqm), 0) AS energy_intensity_kwh_sqm
FROM mart.fact_property_month
GROUP BY month;
