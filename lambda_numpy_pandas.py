""" 
How NumPy and Pandas can be used in AWS Lambda functions for various common tasks such as data processing, aggregation, and normalization.

Data Processing and Transformation
- Filtering and Cleaning Data
Data Analysis and Aggregation
- Grouping Data and Calculating Statistics
Machine Learning and Statistical Operations
- Normalizing Data

"""

import logging
import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler

# Configure logger
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger()

logger.info("Loading function")


def lambda_handler(event, context):
    try:
        # Log library versions
        logger.info("numpy version is {}".format(np.__version__))
        logger.info("pandas version is {}".format(pd.__version__))

        # Extract data from event
        data = event.get("data", [])

        # Create DataFrame
        df = pd.DataFrame(data)

        # Clean data: drop rows with any NaN values
        df_cleaned = df.dropna()
        logger.info("Cleaned DataFrame:\n%s", df_cleaned.to_string(index=False))

        # Remove duplicate rows
        df_cleaned = df_cleaned.drop_duplicates()

        # Add derived columns
        df_cleaned["age_in_10_years"] = df_cleaned["age"] + 10
        df_cleaned["name_length"] = df_cleaned["name"].apply(len)

        # Group by city and calculate mean age
        grouped = df.groupby("city").agg({"age": "mean"}).reset_index()
        # Add aggregated column
        grouped["age_plus_five"] = grouped["age"] + 5
        logger.info("Grouped DataFrame:\n%s", grouped.to_string(index=False))

        # Normalize numerical columns
        scaler = MinMaxScaler()
        df[["age"]] = scaler.fit_transform(df[["age"]])

        # Add statistical columns
        df["age_squared"] = df["age"] ** 2
        df["age_log"] = np.log(df["age"] + 1)  # Adding 1 to avoid log(0)

        logger.info("Normalized DataFrame:\n%s", df.to_string(index=False))

        # return df.to_dict(orient="records")
        # return grouped.to_dict(orient="records")
        return df_cleaned.to_dict(orient="records")
    except Exception as e:
        logger.error("Error occurred: %s", str(e))
        raise


# Simulated event data
test_event = {
    "data": [
        {"name": "Alice", "age": 25, "city": "New York"},
        {"name": "Bob", "age": np.nan, "city": "San Francisco"},
        {"name": "Charlie", "age": 30, "city": None},
        {"name": "John", "age": 25, "city": "New York"},
        {"name": "David", "age": 40, "city": "San Francisco"},
    ]
}


# Invoke the lambda handler for testing
lambda_handler(test_event, None)
