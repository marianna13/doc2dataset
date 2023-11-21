'''Write directly no AWS S3 bucket'''

from main import process_doc, pdf_extractor
import time



output_folder = f"s3a://laion-west/test_pdf_extract" # Use s3a if you want use pyspark distrinbutor
file_list = "/fsx/home-marianna/pdf_extraction/part-00000-314da966-e62f-40cd-9dc2-f1e407952dbd-c000.snappy.parquet?download=true"
s = time.time()
processes_count = 1


pdf_extractor(
    file_list=file_list, 
    output_folder=output_folder, 
    input_format="parquet", 
    output_format="parquet", 
    file_col='url',
    processes_count=processes_count,
    distributor = "pyspark",
    number_samples=10,
    filesystem="s3"
    )

e = time.time()
print(e - s)
