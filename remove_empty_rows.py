import csv

# Read the CSV file
input_file = 'results.csv'
output_file = 'results_cleaned.csv'

with open(input_file, 'r', encoding='utf-8') as infile:
    reader = csv.reader(infile)
    data = list(reader)

# Filter out empty rows (where all columns are empty or just whitespace)
cleaned_data = [row for row in data if any(cell.strip() for cell in row)]

# Write the cleaned data to a new file
with open(output_file, 'w', encoding='utf-8', newline='') as outfile:
    writer = csv.writer(outfile)
    writer.writerows(cleaned_data)

print(f"Original rows: {len(data)}")
print(f"Rows after removing empty rows: {len(cleaned_data)}")
print(f"Cleaned file saved as: {output_file}")