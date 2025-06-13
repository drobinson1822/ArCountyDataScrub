import geopandas as gpd
from datetime import datetime

# Load the shapefile
shapefile_path = "/Users/davidrobinson/Library/Mobile Documents/com~apple~CloudDocs/Desktop/bvRealEstate/GeoDB Data/Parcels.shp"
gdf = gpd.read_file(shapefile_path)

# Add latitude and longitude (geometry centroids)
gdf["lat"] = gdf.geometry.centroid.y
gdf["lon"] = gdf.geometry.centroid.x

# Get today's date for filename
today = datetime.today().strftime("%Y%m%d")

# Define output file name
output_csv = f"/Users/davidrobinson/Library/Mobile Documents/com~apple~CloudDocs/Desktop/bvRealEstate/benton_parcels_with_coords_{today}.csv"

# Save without geometry
gdf.drop(columns="geometry").to_csv(output_csv, index=False)

print(f"Saved: {output_csv}")