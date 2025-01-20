# E-commerce Data Analysis and Visualization

This Python script is designed to analyze and visualize e-commerce session data. It uses various Python libraries, including pandas, matplotlib, seaborn, and plotly, to clean, process, and generate insights from the data.

## Features

1. **Data Cleaning and Preprocessing**:
   - Handles missing values by replacing them with appropriate defaults or modes.
   - Drops unnecessary columns to simplify analysis.
   - Converts time columns to appropriate datetime formats.
   - Removes duplicate rows.

2. **Data Analysis**:
   - Identifies top products based on total transaction revenue per day.
   - Detects anomalies in transaction counts using rolling averages and standard deviations.
   - Identifies the most profitable cities based on total transaction revenue.

3. **Visualizations**:
   - Line chart for top products based on daily transaction revenue.
   - Scatter plot with anomaly detection for transaction counts.
   - Bar chart for the top 10 most profitable cities.

4. **Data Export**:
   - Saves the cleaned and processed dataset to a CSV file for further use.

## Requirements

- Python
- Required libraries:
  - `pandas`
  - `plotly`

Install the required libraries using:
```bash
pip install pandas plotly
pip install plotly
