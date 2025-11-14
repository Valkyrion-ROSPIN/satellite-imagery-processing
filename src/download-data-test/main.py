import ee
import time

# -------------------------------------------------------------------
# 1. Authenticate and Initialize
# -------------------------------------------------------------------
try:
    ee.Initialize(project='valkyrion')
    print("Google Earth Engine initialized successfully.")
except ee.ee_exception.EEException:
    print("Authentication needed. Running ee.Authenticate()...")
    ee.Authenticate()
    ee.Initialize(project='valkyrion')
    print("Google Earth Engine initialized successfully after authentication.")

# -------------------------------------------------------------------
# 2. Define Your Area of Interest (AOI)
# -------------------------------------------------------------------
# You MUST replace these coordinates with your target location.
# Using placeholder coordinates (Medgidia)
base_longitude = 26.102857
base_latitude = 44.427597

# Create a point geometry for the center of your AOI
aoi_point = ee.Geometry.Point([base_longitude, base_latitude])

# Buffer the point to create a 5km radius (5000 meters) study area
# You can also define a rectangle (ee.Geometry.Rectangle)
aoi_region = aoi_point.buffer(5000)

# -------------------------------------------------------------------
# 3. Define Time Range and Cloud Preference
# -------------------------------------------------------------------
START_DATE = '2023-01-01'
END_DATE = '2023-12-31'
MAX_CLOUD_PERCENTAGE = 20  # Filter out images that are mostly cloudy


# -------------------------------------------------------------------
# 4. Create a Cloud Masking Function
# -------------------------------------------------------------------
# This function uses the QA60 band to identify and mask clouds.
def maskS2clouds(image):
    """Masks clouds in a Sentinel-2 SR image using the QA60 band."""
    qa = image.select('QA60')

    # Bits 10 and 11 are clouds and cirrus, respectively.
    cloudBitMask = 1 << 10
    cirrusBitMask = 1 << 11

    # Both flags should be set to zero, indicating clear conditions.
    mask = qa.bitwiseAnd(cloudBitMask).eq(0).And(
        qa.bitwiseAnd(cirrusBitMask).eq(0))

    # Return the image with the cloud/cirrus-free pixels, scaling the
    # data to reflectance units (0-1) by dividing by 10000.
    return image.updateMask(mask).divide(10000)


# -------------------------------------------------------------------
# 5. Load, Filter, and Process the Sentinel-2 Collection
# -------------------------------------------------------------------
# Load the Harmonized Sentinel-2 Surface Reflectance collection
s2_collection = ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED') \
    .filterDate(START_DATE, END_DATE) \
    .filterBounds(aoi_region) \
    .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', MAX_CLOUD_PERCENTAGE)) \
    .map(maskS2clouds)  # Apply the cloud mask to every image

# Select the bands you want for image processing
# B4=Red, B3=Green, B2=Blue, B8=NIR
bands = ['B4', 'B3', 'B2', 'B8']
s2_collection = s2_collection.select(bands)

# -------------------------------------------------------------------
# 6. Create a Single Composite Image
# -------------------------------------------------------------------
# To get one clean image, create a median composite.
# This takes the median value of each pixel over the time range,
# which effectively removes most remaining clouds and shadows.
median_image = s2_collection.median()

print("Image collection filtered and median composite created.")

# -------------------------------------------------------------------
# 7. Export the Image to Google Cloud Storage
# -------------------------------------------------------------------
# This is the "fetch" part. The data moves from Earth Engine
# to your Google Cloud Storage (GCS) bucket.

BUCKET_NAME = 'valkyrion-satellite-data'
FILENAME = 'bucharest-sentinel2'

# Define the export task
export_task = ee.batch.Export.image.toCloudStorage(
    image=median_image,
    description='Sentinel2BaseExport',
    bucket=BUCKET_NAME,
    fileNamePrefix=FILENAME,
    region=aoi_region.bounds(),  # Use the bounds of your AOI
    scale=10,  # Sentinel-2 resolution (10m for RGB/NIR)
    crs='EPSG:4326',  # Standard lat/lon projection
    maxPixels=1e10  # Allows for large exports
)

# Start the export task
export_task.start()

print(f"Monitoring task: {export_task.id}")
while export_task.active():
    # Print the status every 10 seconds
    print(f"Task status: {export_task.status()['state']}")
    time.sleep(10)

# Once the loop finishes, check the final status
final_status = export_task.status()
if final_status['state'] == 'COMPLETED':
    print("Export COMPLETED successfully!")
    print(f"File should be in: gs://{BUCKET_NAME}/{FILENAME}.tif")
elif final_status['state'] == 'FAILED':
    print("Export FAILED.")
    print(f"Error message: {final_status['error_message']}")
else:
    print(f"Task finished with state: {final_status['state']}")