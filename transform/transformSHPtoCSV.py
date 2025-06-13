import geopandas as gpd
from datetime import datetime
import math
import os

# Load the shapefile
shapefile_path = "input/rawGeoDB_20250613/Parcels.shp"
gdf = gpd.read_file(shapefile_path)

# Add latitude and longitude (geometry centroids)
gdf["lat"] = gdf.geometry.centroid.y
gdf["lon"] = gdf.geometry.centroid.x

# Get today's date for filename
today = datetime.today().strftime("%Y%m%d")
base_filename = f"benton_parcels_with_coords_{today}"
output_folder = "input/transformedGeoDB"

# Drop geometry for export
df = gdf.drop(columns="geometry")

# Calculate chunk size
total_rows = len(df)
chunk_size = math.ceil(total_rows / 3)

# Split and save chunks
for i in range(3):
    start = i * chunk_size
    end = start + chunk_size
    chunk_df = df.iloc[start:end]
    chunk_filename = os.path.join(output_folder, f"{base_filename}_part{i+1}.csv")
    chunk_df.to_csv(chunk_filename, index=False)
    print(f"Saved chunk {i+1}: {chunk_filename}")