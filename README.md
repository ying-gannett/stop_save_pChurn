# pChurn assistend Stop & Save 

**pChurn assistend Stop & Save** is a predicted churn risk assisted retention business use case framework. It is designed to applying different stop-save strategies based on predicted churn risk of customers and evaluate the effectiveness of retention strategies. The project provides a AI assisted comprehensive pipeline for data preprocessing, data quanty assessment, and strategy evaluation.

## 🚀 Features

- **Collect Data** 
    - **Churn Predictions**: customer churn risk level from the existing churn prediction model.
    - **Intervention Data**: which stop save strategy was applied to customer.
    - **Call Center Data**: whether and when customer called to call center.
    - **Online Cancell Data**: whether and when customer cancelled the service via online.
    - **Subscriptions status Data**: whether and when the subscription status changed.
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

2.  **Install dependencies**:   todo: this should be a uv managed project
    ```bash
    pip install -r requirements.txt
    ```

## 📂 Project Structure

```
stop_save_pChurn/
├── .agent/skills             # AI agent skills
│   ├── prepare-data          # A skill that prepare data and assess availablity and quality
│   │   ├── templates         # template files used by this skill
│   │   ├── Tools             # tools available to this skill 
│   │   └── SKILL.md          # instruction of this skill
│   ├── another-skill         # Another skill 
│   └── Tools                 # Generic tools that are availbel to all skills
├── data/                     # Input data files
├── notebooks/                # Jupyter notebooks for analysis
│   ├── 01_experiment.ipynb
├── src/                      # Source code
│   ├── data_processing.py
│   ├── intervention_analysis.py
│   └── utils.py
├── requirements.txt          # Project dependencies
└── README.md                 # Project documentation
```

## 🏃 Usage
...

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