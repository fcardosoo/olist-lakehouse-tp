import    kagglehub
import    os
import    shutil
import    dlt
from      pyspark.sql.functions import current_timestamp, input_file_name

# Download dataset
path = kagglehub.dataset_download("olistbr/brazilian-ecommerce")
print(path)
target_path = "/Volumes/ecommerce_lakehouse/landing/olist_raw_files/"
for file in os.listdir(path):
    if file.endswith(".csv"):
        shutil.copy(os.path.join(path, file), target_path)
display(dbutils.fs.ls("/Volumes/ecommerce_lakehouse/landing/olist_raw_files/"))
