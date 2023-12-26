# doc2dataset
[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/drive/1enfWFq6V-2qh5MKsoyTNHLiRjaXoQMAp?usp=sharing)

Easily extract text (and images) from a bunch of pdf files (while preserving the original text formatting)

## Install

`pip install git+https://github.com/marianna13/PDF_extraction.git`

## Python examples

Checkout these examples to use doc2dataset:
* [example.py](examples/example.py)
* [example_spark.py](examples/example_spark.py)

## API

This module exposes a single function `pdf_extractor` which takes the same arguments as the command line tool:


* **file_list** file (csv, parquet, txt etc) containing paths of documents. (*required*)
* **output_format**  Format of output dataset can be (default = "files")
    - files, samples saved in subdirectory for each shard (useful for debugging)
    - webdataset, samples saved in tars (useful for efficient loading)
    - parquet, sampels saved in parquet (as bytes)
* **output_folder**: Desired location of output dataset (default = "dataset")
* **input_format**: Format of the input, can be (default = "csv")
    - txt, text file with a url in each line
    - csv, csv file with urls, (and captions + metadata)
    - tsv, tsv - || -
    - parquet, loads urls and metadata as parquet
* **file_col**: Column in input (if has columns) that contains the filename (default = "filename")
* **distributor** whether to use multiprocessing or pyspark (default = "multiporocessing")
* **processes_count** number of parallel processes (default = 1)
* **save_figures** whether to save figures (default = True)
* **min_words_per_page** mininum words per page (default = 100)
* **max_images_per_page** maximum images per page (default: 5)
* **min_image_size** minumum image size (default = 0)
* **max_image_area** maximum image area (default = None)
* **max_aspect_ratio** max aspect ration (default = None)
* **get_language** whether to get the language of text using pycld2 (default = False)
* **remove_digits** whether to remove digits (default = False), can mess up with images
* **count_words** whether to count words(non-punctuation characters) (default = True)
* **max_pages** maximum number of pages per document (decreasing this param can help speed up) (default = None)
* **get_drawings** whether to extract  SVG images (default = False)

  
## For development


Setup a virtualenv:

```
python3 -m venv .env
source .env/bin/activate
pip install -e .
```

to run tests:
```
pip install -r requirements-test.txt
```
then 
```
make lint
make test
```

You can use `make black` to reformat the code

