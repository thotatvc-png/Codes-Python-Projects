import pandas as pd
import glob

# Pattern for files in the current working directory
file_pattern = "Prashanth_set_*.csv"

# Find all matching files in the current directory
files = glob.glob(file_pattern)

# Initialize an empty DataFrame
combined_df = pd.DataFrame()

# Loop through each file and extract KAM and LID
for file in files:
    print(f"\nReading file: {file}")
    try:
        df = pd.read_csv(file, usecols=["KAM", "LID","Seller Id","Correct cms_vertical","GSM TP","Offer ID"])
        df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
        print("Head of file:")
        print(df.head())  # Show first 5 rows
        combined_df = pd.concat([combined_df, df], ignore_index=True)
    except ValueError:
        print(f"Skipping {file}: KAM or LID column not found")

# Drop duplicates to ensure uniqueness
combined_df = combined_df.drop_duplicates()

# Save to new CSV file in the same directory
output_file = "/home/thotat.vc/analytics_scripts/Non_Trax_KAM_Mapping.csv"
combined_df.to_csv(output_file, index=False)

print(f"\nProcessed {len(files)} files.")
print(f"Final combined dataset has {len(combined_df)} unique rows.")
print(f"Combined unique KAM and LID saved to {output_file}")
