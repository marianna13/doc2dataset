"""PDF exxtraction pipeline"""

from typing import List, Dict, Generator, Union, Tuple
import fire

import re
import time
import fitz
import numpy as np
from sklearn.cluster import DBSCAN
from bs4 import BeautifulSoup
from multiprocessing import Pool
from loguru import logger
import fsspec
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv_pq
import pandas as pd
import math
import os
import uuid
from timeit import default_timer as timer
import json


def get_font_size(style: str) -> float:
    """get font size from the style tag"""

    style_dict = {s.split(":")[0]: s.split(":")[1].replace("pt", "") for s in style.split(";")}

    return float(style_dict["font-size"])


def process_p(p) -> str:
    """
    Process each p tag to preserve subscripts and superscripts
    """
    font_size = get_font_size(p["style"])

    # preserve subscripts and superscripts
    if font_size < 8.5:
        if p.parent.name != "sup":
            p = f"<sub>{str(p)}</sub>"
        else:
            p = f"<sup>{str(p)}</sup>"

    p = re.sub(r' style=".*?"', "", str(p))  # remove style attributes for md conversion
    return p


def coords2html(coords, page):
    """
    Return HTML version for the page rectangle of given the coordinates
    """
    page.set_cropbox(fitz.Rect(coords))
    html = BeautifulSoup(page.get_text("html"), "html.parser")
    if len(html.find_all("img")) > 0:
        return f"<fig rect={coords}></fig>"
    html = " ".join([process_p(p) for p in html.find_all("span")])

    html = html.replace(' id="page0"', "")  # remove id which comes from pymupdf
    page.set_mediabox(page.mediabox)  # go back to the original media box
    return html


def process_block(block, page):
    """
    Return extracted version of extracted text
    """
    html = coords2html(block, page)
    text = re.sub(r"<span>|</span>", "", html)
    return text


def merge_coords(blocks, eps=100):
    clustering = DBSCAN(eps=eps, min_samples=1)
    coords = [b[:4] for b in blocks]

    clusters = clustering.fit_predict(coords)
    clustered_boxes = {str(i): [] for i in list(set(clusters))}

    merged_coords = []
    for b, c in zip(coords, clusters):

        clustered_boxes[str(c)].append(b)

    for k in clustered_boxes.keys():
        merged_coords.append(merge_boxes(clustered_boxes[k]))
    return merged_coords


def merge_boxes(boxes_list: list) -> list:
    """Merge boxes into one box"""

    boxes = np.array(boxes_list)

    return [np.min(boxes[:, 0]), np.min(boxes[:, 1]), np.max(boxes[:, 2]), np.max(boxes[:, 3])]


def process_page(page) -> str:
    """
    Return text version of a page
    """
    page_number = page.number
    extracted_data = []
    try:

        blocks = page.get_text("blocks", sort=True)

        text_blocks = [b[:4] for b in blocks if b[-1] == 0]
        # print(np.max(np.array(text_blocks)[:, 0]), np.max(np.array(text_blocks)[:, 1]))
        image_blocks = [b[:4] for b in blocks if b[-1] == 1]
        merged_blocks = merge_coords(text_blocks)
        merged_blocks.extend(image_blocks)

        for i, b in enumerate(merged_blocks):
            try:
                text = process_block(b, page)
                extracted_data.append(text)
            except Exception as err:
   
                continue

    except Exception as err:
        return "\n".join(extracted_data) # FIXME: this looks weird
        
    return "\n".join(extracted_data)


def process_doc(doc_path: Union[str, os.PathLike]) -> Tuple[List[str], str]:
    """Process one document"""
    
    try:
        with fsspec.open(doc_path, 'rb', timeout=1) as doc_file:
            doc = fitz.open(stream=doc_file.read())
    except Exception as err:
        return None, type(err).__name__+' '+str(err)

    pages = []
    for page in doc.pages():
        processed_page = process_page(page)
        if len(processed_page) == 0: # skip empty pages
            continue
        pages.append(processed_page)

    if len(pages) > 0: # don't add empty docs
        return pages, 'No error'
    return None, 'Empty doc'


def writer(data_generator: Generator, output_format: str, output_folder: str):

    
    if output_format == "txt":
        for data in data_generator:
            begin_write = timer()
            doc_path = data["doc_path"].replace("/", "_").replace(".pdf", ".txt")
            error_dict = data.pop('error')
            txt_path = os.path.join(output_folder, doc_path)
            with open(txt_path, "w") as f:
                for page in data["pages"]:
                    f.write(page + "\n")
            end_write = timer()
            with fsspec.open(path.replace('.txt', '')+ '_log.json', 'w') as f:
                log_data = {
                    'error_stats': error_dict,
                    'total_processing_time': end_write - begin_write
                }
                json.dump(log_data, f)

    elif output_format == "parquet":
        begin_write = timer()
        pandas_df = pd.DataFrame(data_generator, columns=["pages", "doc_path", "error"])
        error_dict = pandas_df["error"].value_counts().to_dict()
        pandas_df = pandas_df.drop('error', axis=1)
        path = output_folder + "/" + str(uuid.uuid4()) + ".parquet"
        pandas_df.dropna(axis=0).to_parquet(path, index=False)
        end_write = timer()
        with fsspec.open(path.replace('.parquet', '')+ '_log.json', 'w') as f:
            log_data = {
                'error_stats': error_dict,
                'total_processing_time': end_write - begin_write
            }
            json.dump(log_data, f)

    elif output_format == "csv":
        begin_write = timer()
        pandas_df = pd.DataFrame(data_generator, columns=["pages", "doc_path", "error"])
        error_dict = pandas_df["error"].value_counts().to_dict()
        pandas_df = pandas_df.drop('error', axis=1)
        path = output_folder + "/" + str(uuid.uuid4()) + ".csv"
        pandas_df.dropna(axis=0).to_csv(path, index=False)
        end_write = timer()
        with fsspec.open(path.replace('.csv', '')+ '_log.json', 'w') as f:
            log_data = {
                'error_stats': error_dict,
                'total_processing_time': end_write - begin_write
            }
            json.dump(log_data, f)
    else:
        ValueError(f"Unknown output format: {output_format}")


def process_multipart(file_list: list, output_format: str, output_folder: str, file_col: str):
    """Process multiple documents"""


    def extract(file_list):
        for doc_path in file_list:
            pages, err = process_doc(doc_path[file_col])
            yield {"pages": pages, "doc_path": doc_path[file_col], 'error': err}


    writer(extract(file_list), output_format, output_folder)


def get_shard(df, shard_start, shard_end, file_col) -> list:
    """Get a list for a given shard_id"""
    return df.slice(shard_start, shard_end - shard_start).select([file_col]).to_pylist()


def get_shard_indices(number_samples: int, number_shards: int) -> list:
    """Get indices for each shard"""
    k, m = divmod(number_samples, number_shards)
    return [(i * k + min(i, m), (i + 1) * k + min(i + 1, m)) for i in range(number_shards)]


def get_file_shards(input_file, input_format, file_col, processes_count) -> Generator:
    """Split input file list into shards for every process"""
    # FIXME: need to support a folder of files
    # FIXME: Sharding is not efficent - need to shard not according ro number of processing but rather by some number of samples per shard
    with fsspec.open(input_file, mode="rb") as file:
        if input_format == "txt":
            df = csv_pq.read_csv(file, read_options=csv_pq.ReadOptions(column_names=[file_col]))
        elif input_format == "json":
            df = pa.Table.from_pandas(pd.read_json(file))
        elif input_format == "csv":
            df = pa.Table.from_pandas(pd.read_csv(file, sep='delimiter', header=None, names=['url']))
        elif input_format == "tsv":
            df = csv_pq.read_csv(file, parse_options=csv_pq.ParseOptions(delimiter="\t"))
        elif input_format == "parquet":
            df = pq.read_table(file, columns=[file_col])
        else:
            raise ValueError(f"Unknown input format {input_format}")

    number_samples = df.num_rows

    number_sample_per_shards = math.ceil(df.num_rows / processes_count)

    logger.info(f"Sharded input into {processes_count} shards with {number_sample_per_shards} sample in shard")

    shard_indices = get_shard_indices(number_samples, processes_count)
    for (shard_start, shard_end) in shard_indices:
        yield get_shard(df, shard_start, shard_end, file_col)


def pdf_extractor(
    file_list: str,
    output_format: str = "files",
    output_folder: str = "dataset",
    input_format: str = "csv",
    file_col: str = "filename",
    distributor: str = "multiprocessing",
    processes_count: int = 1,
    verbose: bool = True
):
    """
    Create datasets from pdf files

    Args:
    file_list: list of input files - can be any of the supported input formats
        (csv, parquet, braceexpand tar paths etc.)
    output_folder: Desired location of output dataset
    output_format: Format of output dataset, can be
        - files, samples saved in subdirectory for each shard (useful for debugging)
        - webdataset, samples saved in tars (useful for efficient loading)
        - parquet, sampels saved in parquet (as bytes)
        - tfrecord, samples saved in tfrecord (as bytes)
        - dummy, does not save (useful for benchmarks)
    input_format: Format of the input, can be
        - txt, text file with a url in each line
        - csv, csv file with urls, (and captions + metadata)
        - tsv, tsv - || -
        - tsv.gz, - || - but compressed gzip
        - json, loads urls and metadata as json
        - parquet, loads urls and metadata as parquet
        - webdataset, usually braceexpand format of mutliple paths to tars to re-process
    file_col: Column in input (if has columns) that contains the filename
    interleaved: whether to include images, cretaes an interleaved version
    distributor: how to process documents (currently only supports multiprocessing)
    processes_count: number of parallel processes
    verbose: whether to print addition output
    """

    logger.info(f"Creating a directory to write output {output_folder}")

    # FIXME: create the output folder with fsspec to support other filesystems
    os.makedirs(output_folder, exist_ok=True)

    if distributor == "multiprocessing":

        shards = get_file_shards(file_list, input_format, file_col, processes_count)

        with Pool(processes_count) as process_pool:
            process_pool.starmap(process_multipart, [(shard, output_format, output_folder, file_col) for shard in shards])

    # FIXME: support for pyspark
    else:
        raise ValueError(f"Unknown distributor: {distributor}")


def main():
    fire.Fire(PDF_extractor)


if __name__ == "__main__":
    main()
