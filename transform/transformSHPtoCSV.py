import geopandas as gpd
from datetime import datetime
import math
import os

# === Configuration ===
shapefile_path = "input/rawGeoDB_20250613/Parcels.shp"
output_folder = "input/transformedGeoDB"
today = datetime.today().strftime("%Y%m%d")
base_filename = f"benton_parcels_with_coords_{today}"

# === Step 1: Load original shapefile ===
gdf = gpd.read_file(shapefile_path)

# === Step 2: Reproject to projected CRS for accurate centroids ===
# Use EPSG:3857 (Web Mercator) or a local CRS like EPSG:26915 (NAD83 / UTM zone 15N for Arkansas)
projected_gdf = gdf.to_crs(epsg=3857)

# === Step 3: Compute centroids in projected space ===
centroids = projected_gdf.geometry.centroid

# === Step 4: Convert centroids back to WGS84 to extract lat/lon ===
centroids_wgs84 = gpd.GeoSeries(centroids, crs="EPSG:3857").to_crs(epsg=4326)

# === Step 5: Assign lat/lon back to original data ===
gdf = gdf.to_crs(epsg=4326)
gdf["lat"] = centroids_wgs84.y
gdf["lon"] = centroids_wgs84.x

# === Step 6: Prepare to split ===
df = gdf.drop(columns="geometry")
os.makedirs(output_folder, exist_ok=True)

total_rows = len(df)
chunk_size = math.ceil(total_rows / 3)

# === Step 7: Export in 3 chunks ===
for i in range(3):
    start = i * chunk_size
    end = start + chunk_size
    chunk_df = df.iloc[start:end]
    chunk_filename = os.path.join(output_folder, f"{base_filename}_part{i+1}.csv")
    chunk_df.to_csv(chunk_filename, index=False)
    print(f"✅ Saved chunk {i+1}: {chunk_filename}")