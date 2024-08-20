import csv
import re
from datetime import datetime


def clean_salary(salary_str):
    """Extract the first numeric value from the salary string."""
    # Print the original salary string for debugging
    print(f"Original salary string: '{salary_str}'")
    
    # Use regular expression to find the first numeric value
    match = re.search(r'\d+(?:,\d+)*', salary_str)
    
    if match:
        # Extract and clean the numeric value
        number_str = match.group(0)
        cleaned_number_str = number_str.replace(',', '')  # Remove commas
        try:
            cleaned_value = float(cleaned_number_str)
            print(f"Extracted and cleaned value: {cleaned_value}")
            return cleaned_value
        except ValueError:
            print(f"Error converting value: '{cleaned_number_str}'")
            return None
    else:
        print("No numeric value found")
        return None


def read_and_clean_csv(input_file_path, output_file_path):
    """Read the original CSV, clean the salary field, and write to a new CSV."""
    with open(input_file_path, mode='r', encoding='utf-8') as infile, \
         open(output_file_path, mode='w', encoding='utf-8', newline='') as outfile:

        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames
        
        if 'What is your annual salary?' not in fieldnames:
            raise ValueError("The input CSV file does not contain the expected salary column.")
        
        fieldnames.append('cleaned_salary')

        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row_number, row in enumerate(reader, start=1):
            try:
                salary_str = row.get('What is your annual salary?', '')
                cleaned_salary = clean_salary(salary_str)
                row['cleaned_salary'] = cleaned_salary
                
                writer.writerow(row)
            except Exception as e:
                print(f"Error processing row {row_number}: {row}")
                print(f"Error details: {str(e)}")
                continue  # Skip this row and continue with the next


if __name__ == "__main__":
    input_csv_path = ""
    output_csv_path = ""
    read_and_clean_csv(input_csv_path, output_csv_path)
    print(f"Cleaned data has been written to {output_csv_path}")
