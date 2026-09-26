-- 1. Rank average monthly occupancy, include numerator and denominator context.
SELECT p.property_id, p.property_name,
       DENSE_RANK() OVER (ORDER BY AVG(f.occupancy_rate) DESC NULLS LAST) AS occupancy_rank,
       AVG(f.occupancy_rate) AS average_monthly_occupancy,
       SUM(f.occupied_units) AS occupied_unit_months,
       SUM(f.total_units) AS available_unit_months,
       COUNT(f.occupancy_rate) AS months_with_defined_rate
FROM mart.fact_property_month f JOIN mart.dim_property p USING (property_id)
WHERE month BETWEEN DATE '2026-01-01' AND DATE '2026-06-01'
GROUP BY p.property_id, p.property_name
ORDER BY occupancy_rank, p.property_id;

-- 2. Earned contractual rent by city, not payments collected.
SELECT f.month, p.city, SUM(f.earned_monthly_rent_usd) AS earned_monthly_rent_usd
FROM mart.fact_property_month f JOIN mart.dim_property p USING (property_id)
WHERE month BETWEEN DATE '2026-01-01' AND DATE '2026-06-01'
GROUP BY f.month, p.city
ORDER BY f.month, p.city;

-- 3. Missing responses stay in the denominator and count as noncompliant.
SELECT property_id, SUM(resolved_orders) AS resolved_orders,
       SUM(sla_eligible_orders) AS eligible_resolved_orders,
       SUM(sla_compliant_orders) AS compliant_orders,
       SUM(sla_noncompliant_orders) AS noncompliant_orders,
       SUM(open_orders) AS excluded_unresolved_orders,
       SUM(missing_response_orders) AS included_missing_response_orders,
       0 AS excluded_missing_response_orders,
       SUM(sla_compliant_orders)::NUMERIC / NULLIF(SUM(sla_eligible_orders), 0) AS sla_compliance_rate
FROM mart.fact_property_month
WHERE month BETWEEN DATE '2026-01-01' AND DATE '2026-06-01'
GROUP BY property_id
ORDER BY property_id;

-- 4. Portfolio average is area-weighted: total kWh / total accepted unit area.
SELECT f.property_id, f.month, f.energy_kwh, f.energy_intensity_kwh_sqm,
       p.energy_intensity_kwh_sqm AS portfolio_energy_intensity
FROM mart.fact_property_month f JOIN mart.portfolio_dashboard p USING (month)
WHERE f.energy_intensity_kwh_sqm > p.energy_intensity_kwh_sqm
  AND f.month BETWEEN DATE '2026-01-01' AND DATE '2026-06-01'
ORDER BY f.month, f.property_id;

-- 5. Undefined rates remain NULL rather than implying zero performance.
SELECT * FROM mart.portfolio_dashboard
WHERE month BETWEEN DATE '2026-01-01' AND DATE '2026-06-01'
ORDER BY month;
