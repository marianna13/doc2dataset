from pdf_extraction import process_doc, pdf_extractor
import time

file_list = "/admin/home-marianna/dcnlp/pdf_pipeline/sample.txt"
output_folder = "/admin/home-marianna/dcnlp/pdf_pipeline/test_result"

s = time.time()

pdf_extractor(file_list=file_list, output_folder=output_folder, input_format="txt", output_format="txt")

e = time.time()
print(e - s)
