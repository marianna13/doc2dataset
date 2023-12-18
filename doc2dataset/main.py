"doc2dataset"
from typing import List, Optional
import fire
import logging
import fsspec
import sys
import signal
import os
from .logger import LoggerProcess
from .extractor import Extractor
from .writer import (
    WebDatasetSampleWriter,
    FilesSampleWriter,
    ParquetSampleWriter,
    TFRecordSampleWriter,
    DummySampleWriter,
)
from .reader import Reader
from .downloader import Downloader
from .distributor import (
    multiprocessing_distributor,
    pyspark_distributor,
)

logging.getLogger("exifread").setLevel(level=logging.CRITICAL)


def arguments_validator(params):
    """Validate the arguments"""
    if params["compute_hash"] not in [None, "md5", "sha256", "sha512"]:
        hash_type = params["compute_hash"]
        raise ValueError(f"Unsupported hash to compute: {hash_type}")

    if params["verify_hash"] is not None:
        _, verify_hash_type = params["verify_hash"]
        if verify_hash_type != params["compute_hash"]:
            raise ValueError(
                "verify_hash and compute_hash must be the same "
                f"but got {verify_hash_type} and {params['compute_hash']}"
            )

    if params["save_additional_columns"] is not None:
        save_additional_columns_set = set(params["save_additional_columns"])

        forbidden_columns = set(
            [
                "key",
                "url",
                "status",
                "error_message",
                "exif",
                "md5",
                "sha256",
                "sha512",
            ]
        )
        intersection = save_additional_columns_set.intersection(forbidden_columns)
        if intersection:
            raise ValueError(
                f"You cannot use in save_additional_columns the following columns: {intersection}."
                + "doc2dataset reserves these columns for its own use. Please remove them from save_additional_columns."
            )


def download(
    url_list: str,
    output_folder: str = "documents",
    processes_count: int = 1,
    output_format: str = "files",
    input_format: str = "txt",
    url_col: str = "url",
    thread_count: int = 256,
    number_sample_per_shard: int = 10000,
    extract_exif: bool = True,
    save_additional_columns: Optional[List[str]] = None,
    timeout: int = 10,
    enable_wandb: bool = False,
    wandb_project: str = "doc2dataset",
    oom_shard_count: int = 5,
    compute_hash: Optional[str] = "sha256",
    verify_hash: Optional[List[str]] = None,
    distributor: str = "multiprocessing",
    subjob_size: int = 1000,
    retries: int = 0,
    save_figures: bool = True,
    min_words_per_page: int = 100,
    max_images_per_page: int = 5,
    disable_all_reencoding: bool = False,
    min_image_size: Optional[int] = 0,
    max_image_area: Optional[float] = None,
    max_aspect_ratio: Optional[float] = None,
    get_language: Optional[bool] = False,
    remove_digits: bool = False,
    count_words: bool = True,
    max_num_pages: Optional[int] = None,
    incremental_mode: str = "incremental",
    max_shard_retry: int = 1,
    user_agent_token: Optional[str] = None,
    disallowed_header_directives: Optional[List[str]] = None,
    encode_format: str = "txt",
    get_drawings: bool = False,
    max_pages: Optional[int] = None,
):
    """Download is the main entry point of doc2dataset, it uses multiple processes and download multiple files"""

    config_parameters = dict(locals())
    arguments_validator(config_parameters)

    def make_path_absolute(path):
        fs, p = fsspec.core.url_to_fs(path)
        if fs.protocol == "file":
            return os.path.abspath(p)
        return path

    output_folder = make_path_absolute(output_folder)
    url_list = make_path_absolute(url_list)

    logger_process = LoggerProcess(output_folder, enable_wandb, wandb_project, config_parameters)

    tmp_path = output_folder + "/_tmp"
    fs, tmp_dir = fsspec.core.url_to_fs(tmp_path)
    if not fs.exists(tmp_dir):
        fs.mkdir(tmp_dir)

    def signal_handler(signal_arg, frame):  # pylint: disable=unused-argument
        try:
            fs.rm(tmp_dir, recursive=True)
        except Exception as _:  # pylint: disable=broad-except
            pass
        logger_process.terminate()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    fs, output_path = fsspec.core.url_to_fs(output_folder)

    if not fs.exists(output_path):
        fs.mkdir(output_path)
        done_shards = set()
    else:
        if incremental_mode == "incremental":
            done_shards = set(int(x.split("/")[-1].split("_")[0]) for x in fs.glob(output_path + "/*.json"))
        elif incremental_mode == "overwrite":
            fs.rm(output_path, recursive=True)
            fs.mkdir(output_path)
            done_shards = set()
        else:
            raise ValueError(f"Unknown incremental mode {incremental_mode}")

    logger_process.done_shards = done_shards
    logger_process.start()

    if verify_hash is not None:
        verify_hash_col, verify_hash_type = verify_hash
    else:
        verify_hash_col = None
        verify_hash_type = None

    reader = Reader(
        url_list,
        input_format,
        url_col,
        verify_hash_col,
        verify_hash_type,
        save_additional_columns,
        number_sample_per_shard,
        done_shards,
        tmp_path,
    )

    if output_format == "webdataset":
        sample_writer_class = WebDatasetSampleWriter
    elif output_format == "parquet":
        sample_writer_class = ParquetSampleWriter  # type: ignore
    elif output_format == "files":
        sample_writer_class = FilesSampleWriter  # type: ignore
    elif output_format == "tfrecord":
        sample_writer_class = TFRecordSampleWriter  # type: ignore
    elif output_format == "dummy":
        sample_writer_class = DummySampleWriter  # type: ignore
    else:
        raise ValueError(f"Invalid output format {output_format}")

    extractor = Extractor(
        save_figures=save_figures,
        min_words_per_page=min_words_per_page,
        max_images_per_page=max_images_per_page,
        disable_all_reencoding=disable_all_reencoding,
        min_image_size=min_image_size,
        max_image_area=max_image_area,
        max_aspect_ratio=max_aspect_ratio,
        get_language=get_language,
        remove_digits=remove_digits,
        count_words=count_words,
        max_num_pages=max_num_pages,
        get_drawings=get_drawings,
    )

    downloader = Downloader(
        sample_writer_class=sample_writer_class,
        extractor=extractor,
        thread_count=thread_count,
        extract_exif=extract_exif,
        output_folder=output_folder,
        column_list=reader.column_list,
        timeout=timeout,
        number_sample_per_shard=number_sample_per_shard,
        oom_shard_count=oom_shard_count,
        compute_hash=compute_hash,
        verify_hash_type=verify_hash_type,
        encode_format=encode_format,
        retries=retries,
        user_agent_token=user_agent_token,
        disallowed_header_directives=disallowed_header_directives,
        save_figures=save_figures,
        get_language=get_language,
        count_words=count_words,
        get_drawings=get_drawings,
        max_pages=max_pages,
    )

    print("Starting the downloading of this file")
    if distributor == "multiprocessing":
        distributor_fn = multiprocessing_distributor
    elif distributor == "pyspark":
        distributor_fn = pyspark_distributor
    else:
        raise ValueError(f"Distributor {distributor} not supported")

    distributor_fn(processes_count, downloader, reader, subjob_size, max_shard_retry)
    logger_process.join()

    if not hasattr(fs, "s3"):
        fs.rm(tmp_dir, recursive=True)


def main():
    fire.Fire(download)


if __name__ == "__main__":
    main()
