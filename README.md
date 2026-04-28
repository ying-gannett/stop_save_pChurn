# pChurn assistend Stop & Save 

**pChurn assistend Stop & Save** is a predicted churn risk assisted retention business use case framework. It is designed to applying different stop-save strategies based on predicted churn risk of customers and evaluate the effectiveness of retention strategies. The project provides a AI assisted comprehensive pipeline for data preprocessing, data quanty assessment, and strategy evaluation.

## 🚀 Features

- **Collect Data** 
    - **Churn Predictions**: predicted churn risk from a existing churn prediction model.
    - **Intervention Data(out of this workflow)**: which stop save strategy was applied to customer.
    - **Call Center Data**: whether and when customer called to call center.
    - **Online Cancell Data**: whether and when customer cancelled the service via online.
    - **Subscriptions status Data**: whether and when the subscription status change to inactive.
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
│   └── sql/                        # BQ scipts
│   ├── data_processing.py          # SQL execution
│   ├── data_assessment.py          # Data quality assessment and logging
│   └── run_pipeline.py             # Python orchestrator for SQL execution, assessment and logging
├── pyproject.toml            # Project configuration and dependencies
├── prepare-data.skill        # Packaged agent skill for distribution
└── README.md                 # Project documentation
```

## 🏃 Usage

### Data Preparation
The data pipeline is orchestrated by an AI agent skill or can be run directly:

1.  **Via Agent**: 
    - **Install Skill**: If not already installed, run:
      ```bash
      gemini skills install prepare-data.skill --scope workspace
      ```
    - **Reload**: Type `/skills reload` in your Gemini CLI session to activate the skill.
    - **Run**: You can fire the skill using natural language prompts.
      - *Example 1 (Churn):* "Prepare the stop save source data for 2026-03-31, 04-07, 04-14, and 04-21. The target BQ table is gannett-datascience.test_activation_zone.ss_test_source."
      - *Example 2 (GA4):* "Run the online cancel GA4 data pipeline for days between 2026-04-03 and 2026-04-09. Date mode is "exact". The target BQ table is gannett-datascience.test_activation_zone.ss_test_online_cancel_raw, partitioned by event_date. Skip the local download."
2.  **Directly**: 
    ```bash
    python src/data_processing.py
    ```
    The pipeline executes SQL scripts against BigQuery, persists results in BQ tables, and saves local copies to the `data/` directory.

### Project specific workflow
1. Weekly Tuesday (Churn): 
    "Execute stop_save_source.sql for <this week>. The target BQ table is <gannett-datascience.test_activation_zone.stop_save_test_Bart>."
2. Weekly Tuesday (GA4 catch-up run for the past week): 
    "Execute raw_online_cancel.sql for <days until last Sunday>. Date mode is "exact". The target BQ table is <gannett-datascience.test_activation_zone.ss_test_online_cancel_raw>, partitioned by event_date. Skip the local download."
3. Weekly Friday (**Out of the workflow**): 
    Take Step 1 result --> <gannett-datascience.test_results_zone.stop_save_test_applied_Bart>
4. Weekly Monday (Stitch together): 
    Step 2 + 3 (check step 3 last Friday is ready. check step 2 until last Friday is ready.) --> <Final SQL> --> test_results_zone.ss_test_result 

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