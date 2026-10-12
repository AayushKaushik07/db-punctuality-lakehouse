# Reference data

## db_official_punctuality.csv

Deutsche Bahn's published **operational punctuality** ("betriebliche Pünktlichkeit") in percent,
used by `sql/gold/08_agg_official_comparison.sql` to sanity-check the pipeline.

| Column | Meaning |
|---|---|
| `period` | label: a month (`2025-07`) or a half-year (`2025-H1`) |
| `first_month`, `last_month` | months covered by the period (inclusive) |
| `fernverkehr_pct` | DB long-distance traffic (ICE/IC/EC) |
| `regio_pct` | DB regional traffic ("Nahverkehr" on the monthly page, "DB Regional" in the report) |
| `personenverkehr_gesamt_pct` | DB passenger traffic overall |
| `source` | where the row comes from (see below) |

A period is only compared when **all** of its months were processed, so a half-year is
never compared against a partial half-year.

### Sources (retrieved 2026-09-29)

- `db_punctuality_page`: monthly values from
  https://www.deutschebahn.com/de/konzern/konzernprofil/zahlen_fakten/puenktlichkeitswerte-6878476
  (the page shows a rolling window of about 14 months; at retrieval it covered 2025-07 to 2026-08).
- `db_interim_report_2025`: half-year H1 2025 values from DB's 2025 interim report,
  https://zbir.deutschebahn.com/2025/en/interim-group-management-report-unaudited/quality-and-security/punctuality/

Months before July 2025 are no longer on DB's monthly page, and this project does not fill them
from secondary sources such as news articles. November and December 2024 therefore have no
official comparison value.

Definition used by DB: a stop is punctual if the train arrives **less than 6 minutes** late;
cancelled stops are not part of the punctuality rate.

To add newer months, copy the values from the page above into a new row, with
`first_month` = `last_month` = the month.
