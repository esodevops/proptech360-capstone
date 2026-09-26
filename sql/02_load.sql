-- stage_property_month is a temporary table filled by parameterized Python inserts.
-- One transaction loads dimensions first, then updates or inserts the fact rows.
INSERT INTO mart.fact_property_month (
    property_id, month, total_units, occupied_units, earned_monthly_rent_usd,
    resolved_orders, sla_eligible_orders, sla_compliant_orders, sla_noncompliant_orders,
    missing_response_orders, open_orders, energy_kwh, floor_area_sqm
)
SELECT property_id, month, total_units, occupied_units, earned_monthly_rent_usd,
       resolved_orders, sla_eligible_orders, sla_compliant_orders, sla_noncompliant_orders,
       missing_response_orders, open_orders, energy_kwh, floor_area_sqm
FROM stage_property_month
ON CONFLICT (property_id, month) DO UPDATE SET
    total_units = EXCLUDED.total_units,
    occupied_units = EXCLUDED.occupied_units,
    earned_monthly_rent_usd = EXCLUDED.earned_monthly_rent_usd,
    resolved_orders = EXCLUDED.resolved_orders,
    sla_eligible_orders = EXCLUDED.sla_eligible_orders,
    sla_compliant_orders = EXCLUDED.sla_compliant_orders,
    sla_noncompliant_orders = EXCLUDED.sla_noncompliant_orders,
    missing_response_orders = EXCLUDED.missing_response_orders,
    open_orders = EXCLUDED.open_orders,
    energy_kwh = EXCLUDED.energy_kwh,
    floor_area_sqm = EXCLUDED.floor_area_sqm;
