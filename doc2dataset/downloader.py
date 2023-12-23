"""the downloader module handles the downloading"""

from multiprocessing.pool import ThreadPool
from threading import Semaphore
import urllib.request
import io
import math
import time
import hashlib
import pyarrow as pa
import traceback

import fsspec
import fitz
import re
from .logger import CappedCounter
from .logger import write_stats


def is_disallowed(headers, user_agent_token, disallowed_header_directives):
    """Check if HTTP headers contain an X-Robots-Tag directive disallowing usage"""
    for values in headers.get_all("X-Robots-Tag", []):
        try:
            uatoken_directives = values.split(":", 1)
            directives = [x.strip().lower() for x in uatoken_directives[-1].split(",")]
            ua_token = uatoken_directives[0].lower() if len(uatoken_directives) == 2 else None
            if (ua_token is None or ua_token == user_agent_token) and any(
                x in disallowed_header_directives for x in directives
            ):
                return True
        except Exception as err:  # pylint: disable=broad-except
            traceback.print_exc()
            print(f"Failed to parse X-Robots-Tag: {values}: {err}")
    return False


def download_doc(row, timeout, user_agent_token, disallowed_header_directives):
    """Download a document with urllib"""
    key, url = row
    doc_stream = None
    user_agent_string = "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:72.0) Gecko/20100101 Firefox/72.0"
    if user_agent_token:
        user_agent_string += f" (compatible; {user_agent_token}; +https://github.com/rom1504/doc2dataset)"
    try:
        request = urllib.request.Request(url, data=None, headers={"User-Agent": user_agent_string})
        with urllib.request.urlopen(request, timeout=timeout) as r:
            if disallowed_header_directives and is_disallowed(
                r.headers,
                user_agent_token,
                disallowed_header_directives,
            ):
                return key, None, "Use of image disallowed by X-Robots-Tag directive"
            doc_stream = io.BytesIO(r.read())
        return key, doc_stream, None
    except Exception as err:  # pylint: disable=broad-except
        if doc_stream is not None:
            doc_stream.close()
        return key, None, str(err)


def download_doc_with_retry(row, timeout, retries, user_agent_token, disallowed_header_directives):
    for _ in range(retries + 1):
        key, doc_stream, err = download_doc(row, timeout, user_agent_token, disallowed_header_directives)
        if doc_stream is not None:
            return key, doc_stream, err
    return key, None, err


def compute_key(key, shard_id, oom_sample_per_shard, oom_shard_count):
    true_key = (10**oom_sample_per_shard) * shard_id + key
    key_format = oom_sample_per_shard + oom_shard_count
    str_key = "{true_key:0{key_format}d}".format(  # pylint: disable=consider-using-f-string
        key_format=key_format, true_key=true_key
    )
    return str_key


class Downloader:
    """The downloader class gets calls with shards, download them then call the writer to write them down"""

    def __init__(
        self,
        sample_writer_class,
        extractor,
        thread_count,
        extract_exif,
        output_folder,
        column_list,
        timeout,
        number_sample_per_shard,
        oom_shard_count,
        compute_hash,
        verify_hash_type,
        encode_format,
        retries,
        user_agent_token,
        disallowed_header_directives,
        save_figures,
        get_language,
        count_words,
        get_drawings,
        max_pages,
    ) -> None:
        self.sample_writer_class = sample_writer_class
        self.extractor = extractor
        self.thread_count = thread_count
        self.extract_exif = extract_exif
        self.output_folder = output_folder
        self.column_list = column_list
        self.timeout = timeout
        self.number_sample_per_shard = number_sample_per_shard
        self.oom_shard_count = oom_shard_count
        self.compute_hash = compute_hash
        self.verify_hash_type = verify_hash_type
        self.encode_format = encode_format
        self.retries = retries
        self.user_agent_token = None if user_agent_token is None else user_agent_token.strip().lower()
        self.disallowed_header_directives = (
            None
            if disallowed_header_directives is None
            else {directive.strip().lower() for directive in disallowed_header_directives}
        )

        self.save_figures = save_figures
        self.get_language = get_language
        self.count_words = count_words
        self.get_drawings = get_drawings
        self.max_pages = max_pages

    def __call__(
        self,
        row,
    ):
        try:
            self.download_shard(row)
            return (True, row)
        except Exception as err:  # pylint: disable=broad-except
            traceback.print_exc()
            print(f"shard {row[0]} failed with error {err}")
            return (False, row)

    def process_doc(self, doc, sample_writer):
        """Process a doc stream"""
        total_words = 0
        error_message = None
        language = None
        page_no = 0
        for page in doc.pages():
            if self.max_pages and page_no > self.max_pages:
                break

            if page_no == 0 and self.extractor.get_language:
                (
                    doc,
                    total_words,
                    language,
                    images_per_page,
                    drawings,
                    error_process,
                ) = self.extractor.process_page(page, get_language=self.extractor.get_language)
            else:
                (
                    doc,
                    total_words,
                    _,
                    images_per_page,
                    drawings,
                    error_process,
                ) = self.extractor.process_page(page, get_language=False)

            if error_process is not None:
                failed_to_extract += 1
                status = "failed_to_extract"
                status_dict.increment(error_process)
                meta["status"] = status
                meta["error_message"] = error_process
                if self.get_drawings:
                    meta["drawings"] = None

                if self.get_language:
                    meta["language"] = None

                if self.count_words:
                    meta["total_words"] = None

                if self.save_figures:
                    meta["images_per_page"] = None
                sample_writer.write(
                    None,
                    str_key + str(page_no),
                    meta,
                )
                continue
            if len(re.sub(r"\s|\n|\t", "", doc)) == 0:  # skip empty pages
                continue
            status = "success"
            meta["status"] = status
            if self.get_drawings:
                meta["drawings"] = drawings

            if self.get_language:
                meta["language"] = language

            if self.count_words:
                meta["total_words"] = total_words

            if self.save_figures:
                meta["images_per_page"] = images_per_page

            sample_writer.write(
                doc,
                str_key + str(page_no),
                meta,
            )

            page_no += 1
        failed_to_extract = failed_to_extract / page_no

        return status, error_message, failed_to_extract

    def download_shard(
        self,
        row,
    ):
        """Function to start a doc downloading in one process"""

        shard_id, shard_file = row
        start_time = time.time()

        fs, shard_path = fsspec.core.url_to_fs(shard_file)
        with fs.open(shard_path, "rb") as f:
            df = pa.ipc.open_file(f).read_all()
        schema = df.schema
        schema = (
            schema.append(pa.field("key", pa.string()))
            .append(pa.field("status", pa.string()))
            .append(pa.field("error_message", pa.string()))
        )
        if self.extract_exif:
            schema = schema.append(pa.field("exif", pa.string()))

        if self.compute_hash is not None and self.compute_hash not in schema.names:
            schema = schema.append(pa.field(self.compute_hash, pa.string()))

        if self.get_drawings:
            schema = schema.append(pa.field("drawings", pa.string()))

        if self.get_language:
            schema = schema.append(pa.field("language", pa.string()))

        if self.count_words:
            schema = schema.append(pa.field("total_words", pa.int32()))

        if self.save_figures:
            schema = schema.append(pa.field("images_per_page", pa.int32()))

        pydict = df.select(self.column_list).to_pydict()
        shard_to_dl = list(enumerate(zip(*(pydict[col] for col in self.column_list))))
        del pydict
        del df

        status_dict = CappedCounter()

        count = len(shard_to_dl)
        successes = 0
        failed_to_download = 0
        failed_to_extract = 0
        url_indice = self.column_list.index("url")
        hash_indice = (
            self.column_list.index(self.verify_hash_type) if self.verify_hash_type in self.column_list else None
        )
        key_url_list = [(key, x[url_indice]) for key, x in shard_to_dl]

        # this prevents an accumulation of more than twice the number of threads in sample ready to resize
        # limit the memory usage
        semaphore = Semaphore(self.thread_count * 2)

        def data_generator():
            for e in key_url_list:
                semaphore.acquire()  # pylint: disable=consider-using-with
                yield e

        loader = data_generator()

        # give schema to writer
        sample_writer = self.sample_writer_class(
            shard_id,
            self.output_folder,
            self.oom_shard_count,
            schema,
            self.encode_format,
        )
        oom_sample_per_shard = math.ceil(math.log10(self.number_sample_per_shard))

        with ThreadPool(self.thread_count) as thread_pool:
            for key, doc_stream, error_message in thread_pool.imap_unordered(
                lambda x: download_doc_with_retry(
                    x,
                    timeout=self.timeout,
                    retries=self.retries,
                    user_agent_token=self.user_agent_token,
                    disallowed_header_directives=self.disallowed_header_directives,
                ),
                loader,
            ):
                try:
                    _, sample_data = shard_to_dl[key]
                    str_key = compute_key(key, shard_id, oom_sample_per_shard, self.oom_shard_count)
                    meta = {
                        # Skip columsn containing a the verification hash and only save the compute hash
                        **{
                            self.column_list[i]: sample_data[i]
                            for i in range(len(self.column_list))
                            if (hash_indice is None or i != hash_indice)
                        },
                        "key": str_key,
                        "status": None,
                        "error_message": error_message,
                    }
                    if self.extract_exif:
                        meta["exif"] = None

                    if self.compute_hash is not None:
                        meta[self.compute_hash] = None

                    if error_message is not None:
                        failed_to_download += 1
                        status = "failed_to_download"
                        status_dict.increment(error_message)
                        meta["status"] = status

                        if self.get_drawings:
                            meta["drawings"] = None

                        if self.get_language:
                            meta["language"] = None

                        if self.count_words:
                            meta["total_words"] = None

                        if self.save_figures:
                            meta["images_per_page"] = None

                        sample_writer.write(
                            None,
                            str_key,
                            meta,
                        )
                        semaphore.release()
                        continue

                    if hash_indice is not None:
                        doc_stream.seek(0)
                        test_hash = getattr(hashlib, self.verify_hash_type)(doc_stream.read()).hexdigest()
                        if test_hash != sample_data[hash_indice]:
                            failed_to_download += 1
                            status = "failed_to_download"
                            status_dict.increment("hash mismatch")
                            meta["status"] = status
                            meta["error_message"] = "hash mismatch"
                            if self.get_drawings:
                                meta["drawings"] = None

                            if self.get_language:
                                meta["language"] = None

                            if self.count_words:
                                meta["total_words"] = None

                            if self.save_figures:
                                meta["images_per_page"] = None

                            sample_writer.write(
                                None,
                                str_key,
                                meta,
                            )
                            doc_stream.close()
                            del doc_stream
                            semaphore.release()
                            continue

                    doc_stream.seek(0)
                    try:
                        doc = fitz.open(stream=doc_stream)
                        status, error_message, failed_to_extract = self.process_doc(doc, sample_writer)
                    except Exception as err:  # pylint: disable=broad-except
                        error_message = str(err)

                    if error_message is not None:
                        failed_to_extract += 1
                        status = "failed_to_extract"
                        status_dict.increment(error_message)
                        meta["status"] = status
                        meta["error_message"] = error_message
                        if self.get_drawings:
                            meta["drawings"] = None

                        if self.get_language:
                            meta["language"] = None

                        if self.count_words:
                            meta["total_words"] = None

                        if self.save_figures:
                            meta["images_per_page"] = None

                        sample_writer.write(
                            None,
                            str_key,
                            meta,
                        )
                        doc_stream.close()
                        del doc_stream
                        semaphore.release()
                        continue
                    successes += 1
                    status = "success"
                    status_dict.increment(status)

                    if self.compute_hash is not None:
                        doc_stream.seek(0)
                        meta[self.compute_hash] = getattr(hashlib, self.compute_hash)(doc_stream.read()).hexdigest()

                    doc_stream.close()
                    del doc_stream

                except Exception as err:  # pylint: disable=broad-except
                    traceback.print_exc()
                    print(f"Sample {key} failed to download: {err}")
                semaphore.release()

            sample_writer.close()
            thread_pool.terminate()
            thread_pool.join()
            del thread_pool

        end_time = time.time()
        write_stats(
            self.output_folder,
            shard_id,
            count,
            successes,
            failed_to_download,
            failed_to_extract,
            start_time,
            end_time,
            status_dict,
            self.oom_shard_count,
        )
        fs.rm(shard_path)
