# pChurn assistend Stop & Save 

**pChurn assistend Stop & Save** is a predicted churn risk assisted retention business use case framework. It is designed to applying different stop-save strategies based on predicted churn risk of customers and evaluate the effectiveness of retention strategies. The project provides a AI assisted comprehensive pipeline for data preprocessing, data quanty assessment, and strategy evaluation.

## 🚀 Features

- **Collect Data** 
    - **Churn Predictions**: predicted churn risk from a existing churn prediction model.
    - **Intervention Data(out of this workflow)**: which stop save strategy was applied to customer.
    - **Call Center Data(out of this workflow)**: whether and when customer called to call center. Whether the customer was saved or not.
    - **Online Cancell Data**: whether and when customer cancelled the service via online.
    - **Monitor performance Data**: Combine the above pieces and calculate performance metrics, such as churn, CNRC
- **Data Quanty Assessment**: Evaluates the availability and the quality of each data source.
- **Strategy Evaluation**: Robust data cleaning and feature engineering pipeline to evaluate the effectiveness of each stop save strategy.
    - **Evaluate metrics**: 
        - **Churn rate**: assess the churn rate of each strategy.
        - **CNRC(Cumulative Net Revenue per Caller)**: Total revenue of retained customers ÷ total callers per strategy in 30/60/90 days.
- **Visualization**: Interactive plots for strategies performance and intervention analysis.

## 🛠️ Installation

1.  **Clone the repository**:
    ```bash
    git clone <repository-url>
    cd stop_save_pChurn
    ```

2.  **Install dependencies**:
    This project uses `uv` for dependency management.
    ```bash
    uv sync
    ```

## 📂 Project Structure

```
stop_save_pChurn/
├── .agent/skills                   # Specialized AI agent skills
│   └── prepare-data                # Skill for data ingestion and quality assessment
│       └── SKILL.md                # Procedural instructions for the agent
│   └── pipeline_history.jsonl      # Skill execution log
├── data/                           # Local cache of BigQuery query results (Parquet)
├── notebooks/                      # Jupyter notebooks for analysis and experimentation
├── src/
│   ├── sql/                        # BQ scripts
│   │   └── ss_test_P3_revenue_analysis.sql  # Repeat-restarter revenue comparison
│   ├── data_processing.py          # SQL execution
│   ├── data_assessment.py          # Data quality assessment and logging
│   ├── run_prepare_data.py         # Ordered, staging-first weekly workflow
│   └── workflow_config.py          # Staging and production table mappings
├── pyproject.toml            # Project configuration and dependencies
├── prepare-data.skill        # Packaged agent skill for distribution
└── README.md                 # Project documentation
```

## 🏃 Usage

### Data Preparation
The preparation workflow runs six stages in order:

1. Write the weekly Sunday partition from `stop_save_source.sql`.
2. Catch up daily GA platform partitions from `ss_test_P1_raw_ga_platform.sql`.
3. Refresh P1 GCP event results from `ss_test_P1_gcp_events.sql`.
4. Refresh P2 unfiltered and filtered results from `ss_test_P2_combined_results.sql`.
5. Refresh the P3 repeat-restarter revenue comparison from `ss_test_P3_revenue_analysis.sql`.
6. Refresh the P3 usage features from `ss_test_P3_usages_features.sql`.

The full runner defaults to the isolated
`gannett-datascience.stop_save_refactor_staging` dataset:

```bash
uv run python src/run_prepare_data.py --run-date 2026-09-30
```

The run date may be any day in the target week and resolves to that week's Sunday.
Use `--ga-end-date` when GA should be caught up beyond that Sunday. The runner validates
the intervention input, source partitions, GA coverage, row counts, requested-week
outputs, and complete one-row-per-origin revenue coverage, and stops immediately after a
failed stage. A staging run also reports row-count and schema differences from production
so intentional query changes can be reviewed.

Production execution is opt-in and should follow a successful staging comparison:

```bash
uv run python src/run_prepare_data.py \
  --run-date 2026-09-30 \
  --environment production \
  --confirm-production
```

Run one stage through the same guarded entry point with `--stage source`, `--stage ga`,
`--stage p1`, `--stage p2`, `--stage revenue`, or `--stage features`. Individual stages
assume their upstream tables are ready. All preparation commands use
`src/run_prepare_data.py`.

The two `dev_prorated_revenue_*` SQL files are retained as references and are not executed
by the preparation workflow.

The packaged agent instructions are in `prepare-data.skill`. After installing or updating
the package, reload skills before invoking the workflow through an agent.

## 📊 Analysis Overview

The project performs the following key analyses:...

### 

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 📧 Contact

For questions or support, please contact Ying Kang.

---

**Built with ❤️ for churn prediction and retention analysis**
