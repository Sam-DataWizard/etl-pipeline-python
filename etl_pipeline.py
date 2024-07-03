import pandas as pd
import sqlite3

def extract(file_path):
    """Extract data from CSV file."""
    data = pd.read_csv(file_path)
    return data

def transform(data):
    """Transform data by cleaning and adding new columns."""
    # Example transformation: Add a new column 'age_group'
    data['age_group'] = data['age'].apply(lambda x: 'Adult' if x >= 18 else 'Minor')
    return data

def load(data, db_name):
    """Load data into SQLite database."""
    conn = sqlite3.connect(db_name)
    data.to_sql('users', conn, if_exists='replace', index=False)
    conn.close()

def main():
    # Define file paths and database name
    csv_file_path = 'data/sample_data.csv'
    db_name = 'etl_pipeline.db'

    # ETL process
    data = extract(csv_file_path)
    transformed_data = transform(data)
    load(transformed_data, db_name)

    print("ETL process completed successfully!")

if __name__ == '__main__':
    main()
