from main import process_doc, pdf_extractor
import time

file_list = "test_books_100.txt"

processes_counts = [1, 5, 10]

for processes_count in processes_counts:
    output_folder = f"/admin/home-marianna/dcnlp/pdf_pipeline/test_result_{processes_count}_proc"

    s = time.time()

    pdf_extractor(
        file_list=file_list, 
        output_folder=output_folder, 
        input_format="csv", 
        output_format="parquet", 
        file_col='url',
        processes_count=processes_count,
        )

    e = time.time()
    print(e - s)
