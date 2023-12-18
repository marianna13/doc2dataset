import fsspec
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkConf
from urllib.parse import urljoin
from fastwarc.warc import ArchiveIterator, WarcRecordType
import simdjson
import random
import gc
import os
from typing import Dict


import re
import io
import time


def make_link_absolute(url: str, base_url: str) -> str:
    """Make a link absolute"""
    if url.startswith("http://") or url.startswith("https://"):
        return url
    try:
        return urljoin(base_url, url)
    except ValueError:
        return url


def make_links_absolute(links: list, base_url: str) -> list:
    """Make links absolute"""
    abs_links = []
    for link in links:
        try:
            abs_links.append({"url": make_link_absolute(link["url"], base_url)})
        except:
            continue
    return abs_links


def local_session(num_cores=4, mem_gb=16):
    """Build a local spark session"""
    spark = (
        SparkSession.builder.config("spark.driver.memory", str(mem_gb) + "G")
        .master("local[" + str(num_cores) + "]")
        .appName("cc2dataset")
        .getOrCreate()
    )
    return spark


def read_wat_index_file(wat_index: str):
    with fsspec.open(wat_index, "r", compression="gzip") as f:
        index_lines = f.readlines()

    wats = [a.strip() for a in index_lines]
    return wats


def process_wat(path: str):
    """
    Process a single wat file
    path here is the path of an index file to select random wat from
    """

    wat = "s3://commoncrawl/" + random.choice(read_wat_index_file(path))  # random wat from index

    while True:
        try:
            with fsspec.open(wat, mode="rb") as f:
                tf = io.BytesIO(f.read())
                break
        except Exception as err:
            print(err)
            time.sleep(1)

    for record in ArchiveIterator(tf, record_types=WarcRecordType.metadata, parse_http=False):
        try:
            record_data = simdjson.load(record.reader)  # type: ignore
        except:  # pylint: disable=bare-except
            print("A shard record failed")
            continue
        envelope = record_data["Envelope"]
        payload = envelope["Payload-Metadata"]
        if "HTTP-Response-Metadata" not in payload:
            continue
        http_resp = payload["HTTP-Response-Metadata"]
        if "HTML-Metadata" not in http_resp:
            continue
        metadata = http_resp["HTML-Metadata"]
        if "Links" not in metadata:
            continue

        links = metadata["Links"]
        cc_filename = record_data["Container"]["Filename"]
        page_url = envelope["WARC-Header-Metadata"]["WARC-Target-URI"]
        # extract base URL to resolve relative URLs
        base_url = envelope["WARC-Header-Metadata"]["WARC-Target-URI"]
        if "Head" in metadata and "Base" in metadata["Head"]:
            try:
                base_url = urljoin(base_url, metadata["Head"]["Base"])
            except ValueError:
                pass

        filtered_links = make_links_absolute(links, base_url)
        for link in filtered_links:
            link = link["url"]

            if link.startswith("http://") or link.startswith("https://"):
                if link.endswith(".pdf"):
                    yield (link,)


if __name__ == "__main__":
    spark = local_session()

    wat_count = 10

    fs, p = fsspec.core.url_to_fs("s3://commoncrawl/crawl-data/")

    sc = SparkContext.getOrCreate()
    wats = ["s3://" + e for e in fs.glob(p + "/*/wat.paths.gz")]
    wats = random.sample(wats, wat_count)  # randomly select wat_count wat links
    rdd = sc.parallelize(wats, wat_count)

    link_df = rdd.mapPartitions(lambda x: process_wat(list(x)[0])).toDF(["url"])
    link_df = link_df.dropDuplicates(["url"])

    link_df.show()
    print(link_df.count())
    link_df.write.parquet(f"PDF_links")
