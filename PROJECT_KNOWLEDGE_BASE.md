# pChurn-Assisted Stop & Save Project Knowledge Base

Last reviewed against the repository implementation: 2026-11-24.

This document is the project-level reference for future analysis. It describes what the current SQL and notebooks actually implement. When comments, saved notebook outputs, and executable code disagree, treat the current SQL and executable notebook cells as the source of truth, then document any intentional change here.

## 1. Experiment overview

### What is the pChurn-assisted Stop & Save test?

This is a retention-pricing experiment for digital subscriptions receiving a price-change notice. Before a customer contacts the call center or enters the online cancellation flow, the customer is assigned a stop-save strategy. The test asks whether using predicted churn risk to choose a retention offer produces a better retention and revenue outcome than simpler offer rules.

The repository combines the externally produced experiment assignments with downstream behavior:

- the pricing notice and effective date;
- call-center and online-cancel contacts;
- permanent stops and whether they are voluntary;
- restarts after a stop;
- paid invoices after contact and the price change;
- pChurn, RFV, engagement, platform, tenure, cost, and market attributes.

The pChurn inference population is generated every Sunday, and the corresponding pricing-email batch is sent every Friday. For example, the Sunday 2026-03-29 pChurn `inference_date` corresponds to the Friday 2026-04-03 pricing-email batch. The event query uses 2026-04-03 as its launch date. GA history begins no earlier than 2025-12-29 to support a 90-day pre-inference engagement window.

### How is the experiment configured?

The assignment itself is created outside this repository and read from `gannett-datascience.test_results_zone.stop_save_test_applied_Bart`. This repository does not contain the assignment generator, the exact offer-menu definitions, or proof of randomization. It contains the following configuration comments and assignment fields:

| Configuration | Implemented interpretation |
| --- | --- |
| Eligible products | Monthly subscriptions (Quarterly and annual subscriptions are excluded). |
| Balancing | Assignments are balanced by site and starting-price level. |
| Two-Offer Cohort | `modeltype != 'PCHURN'`, labeled `Two-Offer Cohort`; Control and Midpoint are assigned 1:1. |
| Three-Offer Cohort | `modeltype = 'PCHURN'`, labeled `Three-Offer Cohort`; Control, Midpoint, and Tiered are assigned 1:1:1. The Tiered arm preserves the pChurn risk-tier 1-5 distribution. |
| Treatment labels | `CONTROL` becomes `Control`, `MIDPOINT` becomes `Midpoint`, and every other group type becomes `Tiered`. |
| Weekly assignment date | The external file date is normally truncated to Sunday. Five known file dates are explicitly remapped in `src/workflow_config.py`. |
| Email date | Defined in P2 as `inference_date + 5 days`, normally Friday. |
| Price fields | `currentrate` is the starting monthly price, `newrate` is the step-up/target price, and `stopsave` is the offered stop-save price. |
| Result primary key | `billing_account` and `id_subscrip` have a one-to-one relationship. This account/subscription identity is the unique identifier and primary key of the resulting data. |

`Control`, `Midpoint`, and `Tiered` are reliable data labels, but their precise customer-facing scripts or offer menus are not defined in this repository. Do not add those details without an external experiment-design source.

### What are we trying to learn?

The primary business question is whether the retention strategies—especially the pChurn-based Tiered strategy—improve retained-customer value relative to Control and Midpoint without creating excessive offer-seeking behavior or repeat-stop/restart loopholes.

The repository approaches that question through four related result families:

1. **Contact and retention:** What share contacts after the price notice, and among contacted users what share is Saved versus voluntarily Stopped?
2. **Revenue efficiency:** How much standardized post-contact revenue is generated per contacted subscription (CNRC), by cohort, treatment, starting price, risk tier, market, and behavior type?
3. **Offer-seeking and restart behavior:** Which users repeatedly contact through multiple channels, pay below the intended stop-save rate, stop and restart, or restart on a different contract/rate?
4. **Customer behavior and timing:** How do engagement profiles differ across populations, and how is first-contact timing associated with the Saved rate and CNRC?

The implemented analyses are primarily descriptive. Treatment assignment may support causal comparisons if the external design confirms randomization and balance, but this repository does not itself establish those assumptions. Contact timing, contact channel, Gamer status, and payment behavior are post-assignment outcomes and must not be interpreted as randomized treatments.

### Common analysis definitions

| Term | Current code definition |
| --- | --- |
| Contacted | `contact_channels != 'No Action yet'`. Contact may be a call, an online-cancel event, or both. |
| First contact | The earliest call or online-cancel timestamp after the pricing notice, stored as `earliest_contact_date`. |
| Saved | A contacted subscription with `churn_code = 0`, meaning no permanent stop was found. This is a retention-based proxy, not a recorded offer-acceptance event. |
| Voluntary Stopped | `churn_code = 1`, derived from a voluntary permanent-stop event. |
| Involuntary/other Stopped | `churn_code = 2`; should be excluded from all analysis. |
| No Action | No contact; if there is also no stop, `churn_code` is set to null rather than Saved. |
| Repeat stop-save | No rejoin plus contact through both channels or at least two contact points; P2 further labels whether the user paid below the stop-save price. |
| Repeat restarter | A voluntary stopper who saw an offer within 90 days of notice, stopped within 90 days of notice, and started a new subscription within 90 days of that stop. The restart is labeled intro or winback. |
| Gamer | Analytical umbrella for Repeat stop-save or Repeat restarter behavior. `Not Gamer`/`Regular User` means neither behavior. |
| Mature | `is_two_payment_cycle_ago = 1`. This is a **global email-date cutoff**: an assignment is mature when its email date is no later than the latest email date among subscriptions observed with at least two distinct paid payment dates. It does not mean each included subscription paid twice. |
| `revenue_raw` | Sum of qualifying paid invoice amounts after first contact and on/after the price-effective date, ending before a permanent stop where applicable. |
| `revenue_single_saved` | The same qualifying revenue, but each invoice below the assigned stop-save price is raised to the stop-save price. It is a standardized “lower-rate gaming prevented” measure, not strictly actual collected revenue. |
| CNRC | In implemented notebooks, `SUM(revenue_single_saved) / contacted subscriptions`. In the contact-timing notebook all rows are contacted, so the denominator is distinct subscriptions in the timing cell. |
| Starting-price group | Derived once in P2 from numeric `start_price`: `<=4.99`, `<=9.99`, `<=14.99`, `<=16.99`, `<=19.99`, and `>19.99 or null`. All downstream analyses consume this `start_price_group` field. |

### How to examine the experiment result

Use the analyses in this order:

1. Refresh and validate the preparation pipeline in staging, compare it with production, and promote only with explicit production approval.
2. Run `examine_result.ipynb` for the top-level treatment, cohort, price-point, Gamer, and market scorecard.
3. Use `EDA_complete_v3.ipynb` to understand population mix and behavioral differences that may explain aggregate performance.
4. Use `contact_timing_acceptance_analysis.ipynb` to describe the association between first-contact timing, the Saved proxy, and CNRC.
5. Read results with denominators and population filters visible. Start with cohort-by-treatment comparisons, then examine price, pChurn risk tier, market, contact channel/timing, and Gamer segments.
6. Treat saved CSVs, charts, and notebook cell outputs as snapshots. Re-run the relevant notebook after a production data refresh before quoting a result.

All three notebooks currently hard-code production result tables. A staging preparation run is suitable for validation, but it does not change notebook inputs unless the notebook queries are deliberately redirected for a staging comparison.

## 2. Prepare-data skill and data lineage

### What the skill does

The repository-local skill at `.agent/skills/prepare-data/SKILL.md` is an operating procedure for an agent; the actual work is performed by `src/run_prepare_data.py`. The skill ensures that preparation uses one guarded entry point, runs stages in dependency order, defaults to an isolated staging dataset, stops after a validation failure, and requires explicit approval plus `--environment production --confirm-production` for production writes.

Run a full staging refresh with any date in the target week:

```bash
uv run python src/run_prepare_data.py --run-date YYYY-MM-DD
```

The runner resolves the date to that week's Sunday. A single stage can be run with `--stage source`, `ga`, `p1`, `p2`, `revenue`, or `features`, but an individual stage assumes all upstream tables are already current.

### What data are prepared, and through which queries?

```text
weekly pChurn population ─┐
external assignments ─────┼─> P2 experiment result ─> repeat-restart revenue
consumer events ─> P1 ────┤                         └> enriched usage features
invoices/features ─────────┘                              ↑
daily GA platform sessions ───────────────────────────────┘
```

| Stage | Query | Main output in production | Grain and purpose |
| --- | --- | --- | --- |
| 1. Weekly source | `src/sql/stop_save_source.sql` | `test_activation_zone.stop_save_test_Bart` | Sunday-partitioned pChurn population: inference date, billing account, subscription, risk tier, and publication/market for the participating-market allowlist. |
| 2. GA platform | `src/sql/ss_test_P1_raw_ga_platform.sql` | `test_activation_zone.ss_test_P1_raw_ga_platform` | Daily engaged-session counts by event date, anonymous ID, site code, and platform. Used for the trailing 90-day platform/session features. |
| 3. P1 events | `src/sql/ss_test_P1_gcp_events.sql` | `test_results_zone.ss_test_P1_gcp_events` | One experiment event record per targeted subscription/account after consolidating pricing targets, calls, online-cancel contacts, permanent stops, and starts. Derives contact channel/count/timing, Saved/Stopped status, conflicts, repeat stop-saves, and repeat restarts. |
| 4. P2 combined result | `src/sql/ss_test_P2_combined_results.sql` | `ss_test_P2_combined_results_unfiltered` and filtered `ss_test_P2_combined_results` | Joins the external assignments to P1, weekly pChurn and source features, plus paid invoices. Adds cohort/treatment, prices and the canonical starting-price group, risk tier, frequency/breadth/tenure/cost, actual and standardized revenue, payment behavior, Gamer labels, and the mature cutoff flag. |
| 5. Repeat-restart revenue | `src/sql/ss_test_P3_revenue_analysis.sql` | `test_results_zone.ss_test_P3_revenue_analysis` | One row per original Repeat restarter. Compares actual paid restart-invoice value with a fixed-original-stop-save-price scenario and labels first service period plus whether a restart stop-save rate was applied. This is a scenario analysis, not a causal estimate. |
| 6. Usage features | `src/sql/ss_test_P3_usages_features.sql` | `test_results_zone.ss_test_P3_usages_features` | P2 result enriched with market metadata, RFV model score/bin, favorite platform(s), and average engaged sessions per day in the trailing 90 days. This is the main input to all three requested notebooks. |

In staging, all mutable outputs above are redirected to `gannett-datascience.stop_save_refactor_staging`. The external intervention table and selected-market reference remain read-only production inputs in both environments.

Important P2 filtering rules:

- The unfiltered table is retained for reconciliation.
- The main filtered table removes subscriptions that contacted but subsequently paid the full target/step-up price (`paid_target = 1`).
- It removes contact/stop conflicts and records not covered by the consumer-events result.
- It does **not** filter to mature assignments; maturity is applied by the notebooks or detail SQL.

Important workflow guardrails:

- pChurn source data and the requested intervention week must exist before dependent work runs;
- the GA table must contain every date needed for the 90-day window;
- P1 must be nonempty, and the P2 and P3 usage-feature outputs must include the requested week;
- filtered P2 rows cannot exceed unfiltered rows;
- repeat-restart revenue must have exactly one complete row for every eligible origin and no extra origins;
- the weekly source row count and average null percentage are compared with history, with a greater-than-10% deviation treated as a failure;
- staging row counts and schemas are compared with production after a full run.

GA catch-up appends dates after the current maximum through the requested end date. It does not repair an older internal gap automatically. Repeat-restarter revenue is prepared by `src/sql/ss_test_P3_revenue_analysis.sql`.

## 3. `examine_result.ipynb`

### Data and population

The notebook executes `src/sql/price_point_detail_data.sql`, which reads `test_results_zone.ss_test_P3_usages_features` and returns one row per mature subscription. It includes both No Action and Contacted subscriptions and retains null, Saved, and voluntary-Stop outcomes, while excluding `churn_code = 2`.

The detail query supplies assignment dates, cohort/treatment, start/step-up/stop-save prices, the P2-defined `start_price_group`, pChurn risk tier, market attributes, contact status/timing, Gamer type, standardized revenue, and payment count.

### What it does

Using `src/examine_result_helpers.py`, it calculates:

- overall contact, save, voluntary-churn, Gamer, revenue, and CNRC metrics;
- the same metrics by cohort and treatment;
- Three-Offer results by starting-price group and treatment;
- contacted-user results by Gamer type and treatment;
- market results for markets with at least 200 subscriptions, ordered by contact rate.

It displays bar charts for contact rate by cohort/treatment, Three-Offer churn rate by start-price/treatment, and CNRC by Gamer type/treatment. It writes the qualifying market summary to `src/notebooks/tables/examine_result/market_summary.csv`.

### Primary question

**Where do the experiment cohorts and treatments differ in contact behavior, retention, and standardized revenue efficiency, and in which price points, Gamer types, or markets are those differences concentrated?**

This is a scorecard and segmentation notebook. It does not calculate confidence intervals, balance diagnostics, or a formal treatment-effect estimate.

## 4. `EDA_complete_v3.ipynb`

### Data and population

The notebook reads mature rows from `test_results_zone.ss_test_P3_usages_features` and left-joins `test_results_zone.ss_test_P3_revenue_analysis` by original subscription. It analyzes all mature accounts first, then defines:

- No Action versus Contacted;
- Contacted Gamers versus Contacted Not Gamers/Regular Users;
- Repeat stop-saves versus Repeat restarters;
- Repeat stop-saves who paid lower versus did not pay lower;
- Repeat restarters on longer contracts, monthly contracts, or a restart stop-save rate;
- Contacted Not Gamers who were Saved versus voluntarily Stopped.

The five primary behavior metrics are `frequency`, `breadth`, `tenure`, `tt_cost`, and RFV `modelscore`. RFV bin, favorite platform, selected-market flags, and market are supporting categorical dimensions.

### What it does

The notebook:

- saves population counts and distribution/quality summaries;
- compares behavior metrics across each population branch using raw, 1st/99th-percentile-clipped, and median/IQR-standardized boxplots;
- drills into one Three-Offer starting-price segment and compares No Action with Contacted Not Gamers, then compares treatments within Contacted Not Gamers;
- fits a common behavioral reference on all Contacted Not Gamers;
- profiles Saved and voluntarily Stopped Not Gamers within contact-channel, contact-timing, cohort, and risk-tier segments;
- ranks Saved-minus-Stopped contrasts and provides descriptive treatment drill-downs for supported segments.

The detailed profiles require at least 20 accounts per outcome/segment. Treatment drill-downs require at least five accounts per outcome/treatment cell.

### Primary question

**What behavioral patterns distinguish No Action, regular contacted users, and the two Gamer branches—and, among regular contacted users, which engagement patterns most distinguish Saved from voluntarily Stopped subscriptions?**

The notebook is intended to explain population composition and behavior, not to estimate treatment effectiveness or a causal effect of behavior.

## 5. `contact_timing_acceptance_analysis.ipynb`

### Data and population

The notebook reads the production P3 usage table and keeps only:

- mature subscriptions;
- Contacted users;
- Regular/Not Gamer users—neither Repeat restarters nor Repeat stop-saves;
- `churn_code` 0 or 1, so every row is classified as Saved or voluntarily Stopped.

It defines `accepted = 1` when `churn_code = 0`. This is explicitly a retention proxy, not a recorded acceptance event. Timing is the day difference between first contact and both the email date and pricing-effective date.

### What it does

The active notebook cells:

- summarize population size, Saved proxy, standardized revenue, CNRC, and timing distribution by treatment and before/on-after-pricing timing;
- construct weekly, 30-day monthly, and business-oriented timing bands spanning email delivery through contacts after pricing;
- calculate acceptance rate with 95% Wilson intervals, revenue, and CNRC in each timing cell;
- plot Two-Offer treatments separately from Three-Offer treatments;
- repeat monthly timing views within cohort/start-price group and cohort/source-risk-tier combinations;
- save low-volume points but mark cells with fewer than 30 users as hollow/unstable;
- retain a broad before-pricing versus on/after-pricing summary.

### Primary question

**Among mature contacted Regular Users, how is the timing of first contact relative to the pricing email and effective date associated with the Saved proxy and CNRC, and does that observed pattern vary by treatment, cohort, start price, or pChurn risk tier?**

Contact timing is chosen by the user after assignment, so the notebook supports descriptive association only. Timing may proxy for urgency, channel, price sensitivity, or other unmeasured customer differences.

### Active versus archived modeling

Sections 4-6 contain commented-out code for point-biserial correlations, bootstrap intervals, spline logistic models, a treatment-by-timing likelihood-ratio test, adjusted weekly odds ratios, and a headline model table. Their old cell outputs and CSV files can remain visible even though the code is currently archived. Do not treat those modeled results as refreshed by a current notebook run unless the code is deliberately re-enabled, reviewed, and executed.

## 6. Working rules for future analysis

- Inspect the upstream SQL population and filters before changing a notebook result.
- Use `src/run_prepare_data.py`; do not execute the six workflow SQL files directly for a refresh.
- Default to staging. Production writes require explicit approval and the production confirmation flags.
- Treat `revenue_single_saved` as a standardized scenario metric and `revenue_raw`/P3 paid-invoice value as closer to observed paid value; name the chosen measure in every result.
- Always state whether the denominator is all assigned subscriptions, Contacted subscriptions, or Contacted Regular Users.
- Always state whether the population is mature and remember that maturity is a cohort cutoff, not an individual two-payment condition.
- Keep treatment comparisons separate by cohort unless there is a justified pooling strategy; Tiered does not exist in the Two-Offer Cohort.
- Do not interpret Gamer, channel, or contact-timing comparisons causally.
- Regenerate tables and charts after the relevant production refresh; existing notebook outputs are evidence of the last execution, not a live result.
- If experiment assignment mechanics, offer definitions, or randomization details are needed, obtain the external design artifact rather than inferring them from treatment labels.
