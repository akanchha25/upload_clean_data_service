import csv
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import RequestError, TransportError
from datetime import datetime
import re

# Connect to Elasticsearch
es = Elasticsearch(
    [{'host': 'localhost', 'port': 9200, 'scheme': 'https'}],
    basic_auth=('elastic search user name', 'password'),
    verify_certs=False  # Set to True for production with valid certificates
)

def convert_timestamp(timestamp_str):
    """Convert timestamp from MM/dd/yyyy HH:mm:ss to ISO 8601 format."""
    try:
        # Adjust the format to match your input
        dt = datetime.strptime(timestamp_str, '%m/%d/%Y %H:%M:%S')
        return dt.isoformat()  # Convert to ISO 8601 format
    except ValueError as e:
        print(f"Error parsing date: {str(e)}")
        return None  # Handle invalid date formats


def convert_age(age_str):
    """Convert age range (e.g., '35-44') or specific phrases (e.g., '65 or over', 'under 18') to an integer."""
    age_str = age_str.replace(' years', '').replace(' year', '').strip().lower()

    if age_str == 'under 18':
        return 17  # or whatever value you want to use for 'under 18'
    elif '-' in age_str:
        start, end = age_str.split('-')
        return (int(start) + int(end)) // 2
    elif 'or over' in age_str:
        return int(age_str.split(' or ')[0].strip())
    elif 'or less' in age_str:
        return int(age_str.split(' or ')[0].strip())
    else:
        return int(age_str)  # Assuming if it's not a range, it's a single age

def convert_years_of_experience(exp_str):
    """Convert experience to a float. Handle ranges and specific cases like '1 or less' or '41 or more'."""
    exp_str = exp_str.replace(' years', '').replace(' year', '').strip()

    if '-' in exp_str:
        start, end = exp_str.split('-')
        return (float(start) + float(end)) / 2
    elif 'or less' in exp_str:
        return float(exp_str.replace('or less', '').strip())
    elif 'or more' in exp_str:
        return float(exp_str.replace('or more', '').strip())
    else:
        return float(exp_str)  # Assuming if it's not a range, it's a single number

def clean_salary(salary_str):
    """Extract digits from the salary string and convert to a float, skipping all non-numeric characters."""
    # Extract only digits from the salary string using regular expression
    digits = re.sub(r'[^\d]', '', salary_str)

    # If no digits were found, return None
    if not digits:
        return None

    try:
        # Convert the result to a float (double in Python)
        return float(digits)
    except ValueError:
        # Return None if conversion fails
        return None

def read_and_transform_csv(file_path, csv_version):
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row_number, row in enumerate(reader, start=1):
            try:
                if csv_version == 1:
                    transformed_row = {
                        "_index": "employee_compensation_data_v71",
                        "_source": {
                            "timestamp": convert_timestamp(row.get('Timestamp', '')),
                            "age": convert_age(row.get('How old are you?', '')),
                            "industry": row.get('What industry do you work in?', ''),
                            "job_title": row.get('Job title', ''),
                            "base_salary": clean_salary(row.get('cleaned_salary', '')),
                            "currency": row.get('Please indicate the currency', ''),
                            "location": row.get('Where are you located? (City/state/country)', ''),
                            "years_of_experience": convert_years_of_experience(row.get('How many years of post-college professional work experience do you have?', '')),
                            "thoughts_about_industry": row.get('If your job title needs additional context, please clarify here:', '')
                        }
                    }
                    yield transformed_row
            except Exception as e:
                print(f"Error processing row {row_number}: {row}")
                print(f"Error details: {str(e)}")
                continue  # Skip this row and continue with the next

def upload_data():
    csv_files = [
        {"path": "path of cvs file", "version": 1},
    ]
    
    for csv_file in csv_files:
        try:
            actions = list(read_and_transform_csv(csv_file["path"], csv_file["version"]))
            for i in range(0, len(actions), 100):  # Process in batches of 100
                batch = actions[i:i+100]
                try:
                    success, failed = helpers.bulk(es, batch, raise_on_error=False, raise_on_exception=False)
                    print(f"Batch {i//100 + 1}: Successfully indexed {success} documents")
                    if failed:
                        print(f"Batch {i//100 + 1}: Failed to index {len(failed)} documents")
                        for item in failed:
                            print(f"Error: {item}")
                except (RequestError, TransportError) as e:
                    print(f"Elasticsearch error in batch {i//100 + 1}: {str(e)}")
        except Exception as e:
            print(f"An error occurred while uploading data from {csv_file['path']}: {str(e)}")

if __name__ == "__main__":
    upload_data()
