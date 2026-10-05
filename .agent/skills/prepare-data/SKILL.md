---
name: prepare-data
description: Run and validate the Stop & Save weekly BigQuery preparation workflow, including the weekly pChurn source, daily GA platform catch-up, ordered P1/P2 materializations, revenue horizons, and usage features. Use for full weekly refreshes or individual preparation stages.
---

# Prepare Stop & Save Data

Run preparation from the repository root through `src/run_prepare_data.py`. This one entry point applies environment mapping, production safeguards, stage validation, and execution order to both partitioned and self-materializing queries.

## Full weekly workflow

The workflow runs these stages in order and stops after any failure or assessment alert:

1. Write the weekly Sunday partition of `stop_save_test_Bart`.
2. Catch up daily `ss_test_ga4_platform` partitions through that Sunday.
3. Run `ss_test_result_P1_gcp_events.sql`.
4. Run `ss_test_result_P2_gcp_sourced.sql`.
5. Run `ss_test_result_P2_revenue.sql` for original/restart 30-, 60-, 90-day, and to-date revenue.
6. Run `ss_test_result_P2_gcp_sourced_add_feas.sql`.

Run it in staging:

```bash
uv run python src/run_prepare_data.py --run-date YYYY-MM-DD
```

`--run-date` may be any date in the target week and resolves to that week's Sunday. `--ga-end-date` defaults to the same Sunday. The inclusive revenue cutoff defaults to `--ga-end-date`; override it with `--revenue-as-of-date` only when the revenue observation date intentionally differs. If the GA table has no baseline, provide an inclusive `--ga-start-date`; never use a date before `2025-12-29`.

The runner checks the intervention input, source partitions, 90-day GA coverage, nonempty outputs, requested-week rows, filtered versus unfiltered counts, revenue key uniqueness, and the revenue cutoff. Staging runs also report row-count and schema differences from production. Do not continue manually after a failure.

## Environments and authorization

The default destination is `gannett-datascience.stop_save_refactor_staging`. Use it for implementation tests and result comparisons.

Production writes require explicit user approval and both flags:

```bash
uv run python src/run_prepare_data.py \
  --run-date YYYY-MM-DD \
  --environment production \
  --confirm-production
```

Never infer production approval from a request to inspect, test, validate, or prepare a plan. Report staging results and request explicit production direction.

## Individual stages

Use `--stage source`, `--stage ga`, `--stage p1`, `--stage p2`, `--stage revenue`, or `--stage features`:

```bash
uv run python src/run_prepare_data.py \
  --run-date YYYY-MM-DD \
  --stage source
```

For GA catch-up, set `--ga-end-date` only when it should extend beyond the resolved Sunday:

```bash
uv run python src/run_prepare_data.py \
  --run-date YYYY-MM-DD \
  --stage ga \
  --ga-end-date YYYY-MM-DD
```

GA catch-up fills dates after the current maximum without rewriting a current final partition. It does not repair older internal gaps. An individual stage assumes its upstream tables are ready. Production stage runs still require `--environment production --confirm-production` after explicit approval.

Run revenue independently only after P2 is current:

```bash
uv run python src/run_prepare_data.py \
  --run-date YYYY-MM-DD \
  --stage revenue \
  --revenue-as-of-date YYYY-MM-DD
```

The detail table grain is original subscription × revenue component (`original` or `restart`) × horizon. Original windows start after both contact and pricing effectiveness; restart windows start on the original subscription's effective permanent-stop date. The summary also includes `all_components` rows. Fixed horizons include only fully observed origins.

## Failure rules

- Missing guardrail data, a missing intervention week, an unavailable GA baseline, a query error, or a validation failure stops the workflow.
- Treat a `⚠️ ALERT` from assessment as a failure and present it before downstream work.
- Require the success summary and stage validations; a completed shell command alone is insufficient.
- Do not execute the SQL files directly with the `bq` CLI or helper modules. The consolidated runner provides the required mapping and safeguards.
