import os
import psycopg2
import requests
import statistics
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configuration
db_config = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

data_table = os.getenv('DATA_TABLE')
gemini_api_key = os.getenv('GEMINI_API_KEY')

def connect_to_database():
    try:
        connection = psycopg2.connect(**db_config)
        print("Database connection successful.")
        return connection
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def fetch_data(connection):
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {data_table}")
            return cursor.fetchall()
    except Exception as e:
        print(f"Error fetching data: {e}")
        return []

def analyze_data(data):
    try:
        # Example: Extract a specific column for analysis
        column_data = [row[1] for row in data]  # Adjust index as needed
        insights = {
            'most_common': statistics.mode(column_data),
            'median': statistics.median(column_data),
            'average': statistics.mean(column_data)
        }
        return insights
    except Exception as e:
        print(f"Error analyzing data: {e}")
        return {}

def send_to_gemini(insights):
    try:
        url = "https://gemini.googleapis.com/v1/analyze"
        headers = {"Authorization": f"Bearer {gemini_api_key}"}
        response = requests.post(url, json=insights, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error from Gemini API: {response.status_code}, {response.text}")
            return None
    except Exception as e:
        print(f"Error connecting to Gemini API: {e}")
        return None

def main():
    connection = connect_to_database()
    if connection:
        data = fetch_data(connection)
        if data:
            insights = analyze_data(data)
            print("Local Insights:", insights)
            gemini_response = send_to_gemini(insights)
            print("Gemini API Response:", gemini_response)
        connection.close()

if __name__ == "__main__":
    main()