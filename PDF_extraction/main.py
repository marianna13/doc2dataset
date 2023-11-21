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
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, ArrayType

try:
    from huggingface_hub import HfFileSystem, create_repo
except:
    logger.warning("Plaase install HuggingFace hub with `pip install huggingface-hub` to write to HF!")
import sys

def get_font_size(style: str) -> float:
    """get font size from thsource e style tag"""

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
    prev_mediabox = page.mediabox
    page.set_cropbox(fitz.Rect(coords))
    html = BeautifulSoup(page.get_text("html"), "html.parser")
    if len(html.find_all("img")) > 0:
        return f"<fig rect=\"{tuple(coords)}\"></fig>"
    html = " ".join([process_p(p) for p in html.find_all("span")])
    html = html.replace(' id="page0"', "")  # remove id which comes from pymupdf
    page.set_mediabox(prev_mediabox)  # go back to the original media box
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
        return None, type(err)

    pages = []
    for page in doc.pages():
        processed_page = process_page(page)
        if len(processed_page) == 0: # skip empty pages
            continue
        pages.append(processed_page)

    if len(pages) > 0: # don't add empty docs
        return pages, 'No error'
    return None, 'Empty doc'


def writer(
        data_generator: Generator, 
        output_format: str, 
        output_folder: str, 
        drop_w_eror: bool, 
        fs
    ):

    sep = {
        "txt": " ",
        "csv": ","
    }

    if output_format == "parquet":
        begin_write = timer()
        pandas_df = pd.DataFrame(data_generator, columns=["pages", "doc_path", "error"])
        error_dict = pandas_df["error"].value_counts().to_dict()
        pandas_df = pandas_df.drop('error', axis=1)
        path = output_folder + "/" + str(uuid.uuid4()) + ".parquet"
        if drop_w_eror:
            pandas_df = pandas_df[pandas_df.error == 'No error']
        with fs.open(path, mode="wb") as o_file:
            pandas_df.dropna(axis=0).to_parquet(o_file, index=False)
        end_write = timer()
        with fs.open(path.replace('.parquet', '')+ '_log.json', 'w') as f:
            log_data = {
                'error_stats': error_dict,
                'total_processing_time': end_write - begin_write
            }
            json.dump(log_data, f)

    elif output_format in ["csv", 'txt']:
        begin_write = timer()
        pandas_df = pd.DataFrame(data_generator, columns=["pages", "doc_path", "error"])
        error_dict = pandas_df["error"].value_counts().to_dict()
        pandas_df = pandas_df.drop('error', axis=1)
        path = output_folder + "/" + str(uuid.uuid4()) + f".{output_format}"
        if drop_w_eror:
            pandas_df = pandas_df[pandas_df.error == 'No error']
        pandas_df.dropna(axis=0).to_csv(path, index=False, sep=sep[output_format])
        end_write = timer()
        with fsspec.open(path.replace(f".{output_format}", '')+ '_log.json', 'w') as f:
            log_data = {
                'error_stats': error_dict,
                'total_processing_time': end_write - begin_write
            }
            json.dump(log_data, f)
    else:
        ValueError(f"Unknown output format: {output_format}")

def extract(file_list, file_col):
    for doc_path in file_list:
        try:
            pages, err = process_doc(doc_path[file_col])
        except Exception as err:
            continue
        yield {"pages": pages, "doc_path": doc_path[file_col], 'error': err}

def local_session(num_cores=16, mem_gb=256):
    """Build a local spark session"""
    spark = (
        SparkSession.builder.config("spark.driver.memory", str(mem_gb) + "G")
        .master("local[" + str(num_cores) + "]")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider")
        # .config("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
        .config(
            "spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,org.apache.spark:spark-hadoop-cloud_2.13:3.3.1"
        )
        .appName("test")
        .getOrCreate()
    )

    return spark

def aws_ec2_s3_spark_session(master, num_cores=128, mem_gb=256):
    """Build a spark session on AWS EC2"""
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    main_memory = str(int(mem_gb * 0.9)) + "g"
    memory_overhead = str(mem_gb - int(mem_gb * 0.9)) + "g"
    spark = (
        SparkSession.builder.config("spark.submit.deployMode", "client")
        .config("spark.driver.cores", "20")
        .config("spark.driver.memory", "50g")
        .config("spark.driver.maxResultSize", "10g")
        .config("spark.executor.memory", main_memory)
        .config("spark.executor.cores", str(num_cores))  # this can be set to the number of cores of the machine
        .config("spark.task.cpus", "1")
        .config("spark.executor.memoryOverhead", memory_overhead)
        .config("spark.task.maxFailures", "10")
        .config(
            "spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,org.apache.spark:spark-hadoop-cloud_2.13:3.3.1"
        )
        # change to the appropriate auth method, see https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html
        # .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider")
        # ton of options to try and make s3a run faster
        .config("spark.hadoop.fs.s3a.threads.max", "512")
        .config("spark.hadoop.fs.s3a.connection.maximum", "2048")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.sql.shuffle.partitions", "4000")
        .config("spark.hadoop.fs.s3a.directory.marker.retention", "keep")
        .config("spark.hadoop.fs.s3a.max.total.tasks", "512")
        .config("spark.hadoop.fs.s3a.multipart.threshold", "5M")
        .config("spark.hadoop.fs.s3a.multipart.size", "5M")
        .config("spark.hadoop.fs.s3a.fast.upload.active.blocks", "512")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
        .config("spark.hadoop.fs.s3a.connection.timeout", "600000")
        .config("spark.hadoop.fs.s3a.readahead.range", "2M")
        .config("spark.hadoop.fs.s3a.socket.recv.buffer", "65536")
        .config("spark.hadoop.fs.s3a.socket.send.buffer", "65536")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.hadoop.fs.s3a.experimental.input.fadvise", "random")
        .config("spark.hadoop.fs.s3a.block.size", "2M")
        .config("spark.hadoop.fs.s3a.fast.buffer.size", "100M")
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "array")
        .config("spark.hadoop.fs.s3a.bucket.all.committer.magic.enabled", "true")
        .master(master)  # this should be set to the spark master url
        .appName("cc2dataset")
        .getOrCreate()
    )
    return spark


def process_multipart(
        file_list: list, 
        output_format: str, 
        output_folder: str, 
        file_col: str, 
        drop_w_eror: bool, 
        fs
    ):
    """Process multiple documents"""

    writer(extract(file_list, file_col), output_format, output_folder, drop_w_eror, fs)

def deduplicate_repartition_count(df, output_path, wat_count, spark, shuffle=True):
    """Deduplicate and repartition"""
    uniques = df.dropDuplicates(["uid"])
    s = time.time()
    if shuffle:
        uniques = uniques.sort(rand())
    repartitioned = uniques.repartition(max(256, wat_count // 500))
    repartitioned.write.mode("overwrite").parquet(output_path)
    e = time.time()
    logger.info(f"Took {e - s} seconds")
    logger.info("Computing size")
    df = spark.read.parquet(output_path)
    logger.info(f"Size: {df.count()}")

def process_spark(
        file_list, 
        processes_count, 
        output_folder, 
        file_col,
        mem_gb,
        drop_w_eror,
        fs,
        local,
        master
    ):
    schema = StructType([ \
        StructField("pages",ArrayType(StringType()),True), \
        StructField("doc_path",StringType(),True), \
        StructField("error",StringType(),True), \
    ])

    if local:
        spark = local_session(num_cores=processes_count, mem_gb=mem_gb)
    else:
        spark = aws_ec2_s3_spark_session(master=master, num_cores=processes_count, mem_gb=mem_gb)
    sc = SparkContext.getOrCreate()
    wat_rdd = sc.parallelize(file_list, processes_count)
    output = wat_rdd.mapPartitions(lambda x: extract(list(x)[0], file_col))
    df = output.toDF(schema=schema)
    if drop_w_eror:
        df = df.filter(df.error == "No error")
    df.write.mode('overwrite').parquet(output_folder)
    
    # with fs.open(output_folder, mode='ab') as o_file:
    #     df.write.mode('overwrite').parquet(o_file)
    # deduplicate_repartition_count(df, output_path + "/merged", wat_count, spark, shuffle)


def get_shard(df, shard_start, shard_end, file_col) -> list:
    """Get a list for a given shard_id"""
    return df.slice(shard_start, shard_end - shard_start).select([file_col]).to_pylist()


def get_shard_indices(number_samples: int, number_shards: int) -> list:
    """Get indices for each shard"""
    k, m = divmod(number_samples, number_shards)
    return [(i * k + min(i, m), (i + 1) * k + min(i + 1, m)) for i in range(number_shards)]


def get_file_shards(input_file, input_format, file_col, processes_count, number_samples=None) -> Generator:
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

    num_rows = df.num_rows
    number_samples = number_samples if number_samples else num_rows
  
    # df = df.take(list(np.random.randint(0, num_rows-1, number_samples))) # shuffle
    

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
    verbose: bool = True,
    number_samples: int = None,
    mem_gb: int=256,
    drop_w_eror: bool=False,
    filesystem="file",
    master="local"
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
    number_samples: number of samples
    mem_gb: memory in GB for the spark session to use
    drop_w_eror: drop row with error
    filesystem: file sytem to write output files to
    """

    logger.info(f"Creating a directory to write output {output_folder}")


    fs = fsspec.filesystem(filesystem)
    try:
        fs.makedirs(output_folder, exist_ok=True)
    except:
        logger.warning(f"Couldn't create {output_folder}")

    shards = get_file_shards(file_list, input_format, file_col, processes_count, number_samples)

    if distributor == "multiprocessing":
        with Pool(processes_count) as process_pool:
            process_pool.starmap(process_multipart, [(shard, output_format, output_folder, file_col, drop_w_eror, fs) for shard in shards])

    # FIXME: support for pyspark
    elif distributor == "pyspark":
        process_spark(shards, processes_count, output_folder, file_col, mem_gb, drop_w_eror, fs, filesystem != 's3', master)
    else:
        raise ValueError(f"Unknown distributor: {distributor}")


def main():
    fire.Fire(PDF_extractor)


if __name__ == "__main__":
    main()
