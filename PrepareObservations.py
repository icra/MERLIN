import pandas as pd
import xarray as xr
import numpy as np
import os
import glob

# ==== DEFINE DATE RANGE (start and end) ====
start_date = '2001-01-01'  # Change as needed (format: 'YYYY-MM-DD')
end_date = '2011-12-31'    # Change as needed (format: 'YYYY-MM-DD')

# Directory containing the .nc files
nc_directory = 'E'  # Change this path if needed

# Load the points file with `id_conca = 1`
points_df = pd.read_csv('gleamdata.csv')

from tqdm import tqdm

# Extract latitudes and longitudes from selected points
selected_latitudes = points_df['lat'].values
selected_longitudes = points_df['lon'].values

# Create a list to store data from all files
all_data = []

# Process each .nc file in the folder
files = glob.glob(os.path.join(nc_directory, "E_*.nc"))
selected_files = [f for f in files if 2000 <= int(os.path.basename(f).split('_')[1]) <= 2022]

for file in tqdm(selected_files, desc="Processing files"):
    # Extract year from filename
    year = os.path.basename(file).split('_')[1]
    
    # Load the .nc file
    ds = xr.open_dataset(file)
    
    # Filter dataset for selected coordinates
    filtered_ds = ds.sel(lat=selected_latitudes, lon=selected_longitudes, method='nearest')
    
    # Extract time and E variable
    for time_val, e_values in zip(filtered_ds['time'].values, filtered_ds['E'].values):
        # Get date as "Year-Month-01"
        date = pd.to_datetime(time_val)
        year_month = date.strftime('%Y-%m-01')
        
        # Calculate mean E for selected coordinates
        monthly_mean = np.nanmean(e_values)
        
        # Append to data list
        all_data.append([year_month, monthly_mean])

# Create a DataFrame with the results
results_df = pd.DataFrame(all_data, columns=['Date', 'Mean_E'])
results_df['Date'] = pd.to_datetime(results_df['Date'])

# ==== FILTER BY DEFINED DATE RANGE ====
results_df = results_df[
    (results_df['Date'] >= pd.to_datetime(start_date)) &
    (results_df['Date'] <= pd.to_datetime(end_date))
]

# Create fixed time column
df = results_df.copy()
df['Time'] = '00:00'

# Shuffle DataFrame and create Flag column
df = df.sample(frac=1, random_state=42).reset_index(drop=True)

# Calculate number of rows for 70% and assign flags
total_rows = len(df)
calibration_rows = int(0.7 * total_rows)

df['Flag'] = ['C'] * calibration_rows + ['V'] * (total_rows - calibration_rows)

# Optional: sort by date again to restore chronological order
df = df.sort_values('Date').reset_index(drop=True)

# Reorder columns
final_df = df[['Date', 'Time', 'Mean_E', 'Flag']]

# Save to tab-delimited .txt file
final_df.to_csv('obs_var_1.txt', sep='\t', index=False)

print("File 'obs_et.txt' generated with data between", start_date, "and", end_date)
print("Includes 70% for calibration and 30% for validation.")
