-- Opt-in live check against the real UN APIs (make live-check). Not part of make check.
-- About 15 small requests; population.data is skipped (needs a token).
SELECT count(*) AS sdg_goals FROM un.sdg.goals;
SELECT year, value FROM un.sdg.data('SI_POV_DAY1')
WHERE country_iso3 = 'IND' AND year = 2022 AND sex = 'BOTHSEX' AND age = 'ALLAGE' AND location = 'ALLAREA';
SELECT count(*) AS uis_indicators FROM un.uis.indicators;
SELECT geo_unit, year, value FROM un.uis.data(indicator := 'CR.1') WHERE country_iso3 = 'BRA' AND year BETWEEN 2018 AND 2020 ORDER BY year;
SELECT name, year_inscribed, in_danger FROM un.heritage.sites(country := 'SYR') ORDER BY year_inscribed;
SELECT count(*) AS wpp_locations FROM un.population.locations;
SELECT value FROM un.refugees.populations WHERE origin_iso3 = 'SYR' AND asylum_iso3 = 'DEU' AND year = 2023 AND measure = 'refugees';
SELECT year, sex, value FROM un.health.data('WHOSIS_000001') WHERE country_iso3 = 'IND' AND year = 2019 ORDER BY sex;
