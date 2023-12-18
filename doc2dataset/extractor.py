"""extractor module handle doc extracting"""

import re

from resiliparse.parse.encoding import detect_encoding
from resiliparse.parse.html import HTMLTree
from resiliparse.extract.html2text import extract_plain_text


import pycld2 as cld2


ALL_TAGS = re.compile(r"(?i)<.*?>", re.DOTALL | re.MULTILINE | re.IGNORECASE | re.UNICODE)
ALL_TAGS_EXCEPT_IMG = re.compile(r"(?i)<(?!img|/img).*?>", re.DOTALL | re.UNICODE)
ANY_WORD = re.compile(r"[^][\s,<>]+", re.DOTALL | re.MULTILINE | re.IGNORECASE | re.UNICODE)
IMG_TAG = re.compile(r"<img.*?>", re.DOTALL | re.UNICODE)
ANY_DIGIT = re.compile(r"[.\d]+", re.DOTALL | re.MULTILINE | re.IGNORECASE | re.UNICODE)


class MaxWordsPerPage(Exception):
    pass


class MaxImagesperPage(Exception):
    pass


def count_words_in_page(page: str) -> int:
    clean_page = remove_all_tags(page)
    num_words = len(re.findall(ANY_WORD, clean_page))
    return num_words


def get_all_images_in_page(page: str) -> list:
    return re.findall(IMG_TAG, page)


def get_size(img: str, dim: str) -> int:
    pattern = f'{dim}=".*?"'
    result = re.search(pattern, img)
    if result is not None:
        size = int(result.group().replace(f"{dim}=", "").replace('"', ""))
        return size
    return 0


def remove_all_tags(page: str) -> str:
    return ALL_TAGS.sub("\n", page, re.DOTALL)


def remove_all_tags_except_img(tree: HTMLTree) -> tuple:
    imgs = tree.body.get_elements_by_tag_name("img")
    for img in imgs:
        img_tag = f'<img height="{img.getattr("height")}" width="{img.getattr("width")}" src="{img.getattr("src")}"/>'
        img.setattr("alt", img_tag)
    return extract_plain_text(tree, preserve_formatting=True, alt_texts=True), imgs


def remove_img_tag(page, img):
    return re.sub(img, "", page, re.DOTALL)


def rm_digits(page):
    return ANY_DIGIT.sub("", page, re.DOTALL)


def detect_language(page):
    _, _, details = cld2.detect(page)
    return details[0][1]


def get_svg(page):
    return page.get_svg_image()


class Extractor:
    """
    Extract text and images from documents
    Expose a __call__ method to be used as a callable object

    Should be used to resize one image at a time

    Options:
        resize_mode: "no", "keep_ratio", "center_crop", "border"
        resize_only_if_bigger: if True, resize only if image is bigger than image_size
        image_size: size of the output image to resize
    """

    def __init__(
        self,
        save_figures,
        min_words_per_page,
        max_images_per_page,
        disable_all_reencoding,
        min_image_size,
        max_image_area,
        max_aspect_ratio,
        get_language,
        remove_digits,
        count_words,
        max_num_pages,
        get_drawings,
    ):
        self.save_figures = save_figures
        self.min_words_per_page = min_words_per_page
        self.max_images_per_page = max_images_per_page
        self.disable_all_reencoding = disable_all_reencoding
        self.min_image_size = min_image_size
        self.max_image_area = max_image_area
        self.max_aspect_ratio = max_aspect_ratio
        self.get_language = get_language
        self.remove_digits = remove_digits
        self.count_words = count_words
        self.max_num_pages = max_num_pages
        self.get_drawings = get_drawings

    def get_img_rm_criteria(self, img):
        w = get_size(img, "width")
        h = get_size(img, "height")
        img_size_criteria = self.min_image_size and (w < self.min_image_size or h < self.min_image_size)
        img_ratio_criteria = self.max_aspect_ratio and (w / h > self.max_aspect_ratio or h / w > self.max_aspect_ratio)
        return img_size_criteria or img_ratio_criteria

    def process_page(self, page, get_language):
        """Exatrct dat from one PDF page"""
        error_message = None
        language = None
        images_per_page = None
        drawings = None
        try:
            processed_page = page.get_text("xhtml")
            processed_page = processed_page.replace(' id="page0"', "").encode()
            encoding = detect_encoding(processed_page)
            tree = HTMLTree.parse_from_bytes(processed_page, encoding)  # deocde

            if self.save_figures:
                processed_page, images = remove_all_tags_except_img(tree)
            else:
                processed_page = remove_all_tags(str(tree))

            if self.count_words:
                count_words = count_words_in_page(processed_page)
                if self.min_words_per_page and count_words < self.min_words_per_page:
                    raise MaxWordsPerPage()

            if self.save_figures:
                images_per_page = len(images)

                if self.max_images_per_page and images_per_page > self.max_images_per_page:
                    raise MaxImagesperPage()

                if self.min_image_size or self.max_aspect_ratio:
                    for img in images:
                        img = img["alt"]
                        rm_img = self.get_img_rm_criteria(img)
                        if rm_img:
                            processed_page = remove_img_tag(processed_page, img)

            if self.remove_digits and not self.save_figures:
                processed_page = rm_digits(processed_page)

            if get_language:
                language = detect_language(processed_page)
            if self.get_drawings:
                drawings = get_svg(page)

        except Exception as err:
            error_message = str(err)
            processed_page, count_words = None, None

        return processed_page, count_words, language, images_per_page, drawings, error_message
