# DataForge SFT v3 Failure Postmortem

- Failure samples analyzed: `25`
- Dataset counts: `beers`=8, `flights`=8, `hospital`=9
- Failure taxonomy: `missed_repair`=116, `overrepair`=32, `schema_case_error`=30, `wrong_value`=19, `wrong_cell`=1

## Findings

- Schema/case mistakes such as Index, Id, and Abv remain frequent.
- Wrong-cell index/address/provider repairs show weak row-id discipline.
- Beer samples overrepair style or preserve percent/unit text instead of normalizing.
- Flights samples invent, copy, or date-prefix times instead of abstaining.

## Top Predicted Columns

- `act_arr_time`: 22
- `Index`: 19
- `abv`: 15
- `style`: 5
- `Beer-Name`: 4
- `Abv`: 4
- `Address1`: 3
- `Src`: 3
- `Address2`: 2
- `sched_arr_time`: 2
- `ProviderNumber`: 1
- `Tuple ID`: 1
