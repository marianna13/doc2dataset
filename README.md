# python_template
[![pypi](https://img.shields.io/pypi/v/python_template.svg)](https://pypi.python.org/pypi/python_template)
[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/rom1504/python_template/blob/master/notebook/python_template_getting_started.ipynb)
[![Try it on gitpod](https://img.shields.io/badge/try-on%20gitpod-brightgreen.svg)](https://gitpod.io/#https://github.com/rom1504/python_template)

Easily extract text (and images) from a bunch of pdf files (while preserving the original text formatting)

## Install

pip install PDF_extraction

## Python examples

Checkout these examples to call this as a lib:
* [example.py](examples/example.py)

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

