from main import process_doc, pdf_extractor
import time

sample_urls = [
    "https://www.hsgac.senate.gov/imo/media/doc/HSGAC_Finance_Report_FINAL.pdf",
    "https://blogs.sch.gr/lyktinou/files/2018/05/ΠΡΟΓΡΑΜΜΑ-ΕΞΕΤΑΣΕΩΝ-2018-1.pdf",
    "http://subs.emis.de/LNI/Proceedings/Proceedings51/GI-Proceedings.51-129.pdf",
    "https://bioplasticsnewsdotcom.files.wordpress.com/2018/06/greenpeace-report-plastics-antarctic.pdf",
    "https://millinocket.org/wp-content/uploads/2020/12/march-18-COVID-19-Updates.doc.pdf"
 ]

file_list = "test.txt"

with open(file_list, "w") as f:
    for url in sample_urls:
        f.write(url + "\n")

processes_count = 1
output_format = "parquet"
output_folder = f"test_result"
input_format = "txt"


s = time.time()

pdf_extractor(
    file_list=file_list, 
    output_folder=output_folder, 
    input_format=input_format, 
    output_format=output_format, 
    processes_count=processes_count,
    )

e = time.time()
print(e - s)
