import csv
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import RequestError, TransportError
from datetime import datetime
import re
from flask import Flask, request, jsonify
import os

app = Flask(__name__)

# Connect to Elasticsearch
es = Elasticsearch(
    [{'host': 'localhost', 'port': 9200, 'scheme': 'https'}],
    basic_auth=('elastic', 'OMdt*psB6iJNUFHTOsQi'),
    verify_certs=False  # Set to True for production with valid certificates
)

def convert_timestamp(timestamp_str):
    """Convert timestamp from MM/dd/yyyy HH:mm:ss to ISO 8601 format."""
    try:
        dt = datetime.strptime(timestamp_str, '%m/%d/%Y %H:%M:%S')
        return dt.isoformat()
    except ValueError as e:
        print(f"Error parsing date: {str(e)}")
        return None

def convert_age(age_str):
    """Convert age range or phrases to an integer."""
    age_str = age_str.replace(' years', '').replace(' year', '').strip().lower()
    if age_str == 'under 18':
        return 17
    elif '-' in age_str:
        start, end = age_str.split('-')
        return (int(start) + int(end)) // 2
    elif 'or over' in age_str:
        return int(age_str.split(' or ')[0].strip())
    elif 'or less' in age_str:
        return int(age_str.split(' or ')[0].strip())
    else:
        return int(age_str)

def convert_years_of_experience(exp_str):
    """Convert experience range or specific values to a float."""
    exp_str = exp_str.replace(' years', '').replace(' year', '').strip()
    if '-' in exp_str:
        start, end = exp_str.split('-')
        return (float(start) + float(end)) / 2
    elif 'or less' in exp_str:
        return float(exp_str.replace('or less', '').strip())
    elif 'or more' in exp_str:
        return float(exp_str.replace('or more', '').strip())
    else:
        return float(exp_str)

def clean_salary(salary_str):
    """Extract digits from the salary string and convert to a float."""
    digits = re.sub(r'[^\d]', '', salary_str)
    if not digits:
        return None
    try:
        return float(digits)
    except ValueError:
        return None

def read_and_transform_csv(file_path, index_version):
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row_number, row in enumerate(reader, start=1):
            try:
                transformed_row = {
                    "_index": f"employee_compensation_data_v{index_version}",
                    "_source": {
                        "timestamp": convert_timestamp(row.get('Timestamp', '')),
                        "age": convert_age(row.get('How old are you?', '')),
                        "industry": row.get('What industry do you work in?', ''),
                        "job_title": row.get('Job title', ''),
                        "base_salary": clean_salary(row.get('What is your annual salary?', '')),
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
                continue

@app.route('/upload', methods=['POST'])
def upload_data():
    try:
        # Retrieve CSV and version from the request
        csv_file = request.files['file']
        index_version = request.form['version']

        # Save the uploaded CSV file temporarily
        file_path = f"/tmp/{csv_file.filename}"
        csv_file.save(file_path)

        # Transform and upload data
        actions = list(read_and_transform_csv(file_path, index_version))
        for i in range(0, len(actions), 100):
            batch = actions[i:i+100]
            try:
                success, failed = helpers.bulk(es, batch, raise_on_error=False, raise_on_exception=False)
                print(f"Batch {i//100 + 1}: Successfully indexed {success} documents")
                if failed:
                    print(f"Batch {i//100 + 1}: Failed to index {len(failed)} documents")
            except (RequestError, TransportError) as e:
                print(f"Elasticsearch error in batch {i//100 + 1}: {str(e)}")

        # Clean up the temporary file
        os.remove(file_path)

        return jsonify({"message": "Data uploaded successfully"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000)
