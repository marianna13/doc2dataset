import pytest
from PDF_extraction.extractor import *

def test_count_words_in_page():
    page = "نکیمن دیلوت هوضو هب ار"
    num_words_true = 5
    assert count_words_in_page(page) == num_words_true

    page = "<h1> header \n header </h2>نکیمن دیلوت  <img jslfndl />هوضو هب ار бла бла"
    num_words_true = 9
    assert count_words_in_page(page) == num_words_true

def test_get_all_images_in_page():
    page = "نکیمن دیلوت هوضو هب ار"
    assert len(get_all_images_in_page(page)) == 0
    
    page = """<h1> header \n header </h2>نکیمن دیلوت  <img width="270" height="180" src="data:image/jpeg;base64,\n/9j/4AAQSkZJRgABAQEAAAAAAAD/2wBDAAgGBgcGBQgHBwcJCQgKDBQNDAsLDBkS\nEw8UHRofHh0aHBwgJC4nICIsIxwcKDcpLDAxNDQ0Hyc5PTgyPC4zNDL/2wBDAQkJ\nCQwLDBgNDRgyIRwhMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIy\nMjIyMjIyMjIyMjIyMjL/wAARCAC3ARMDASIAAhEBAxEB/8QAHwAAAQUBAQEBAQEA\nAAAAAAAAAAECAwQFBgcICQoL/8QAtRAAAgEDAwIEAwUFBAQAAAF9AQIDAAQRBRIh\nMUEGE1FhByJxFDKBkaEII0KxwRVS0fAkM2JyggkKFhcYGRolJicoKSo0NTY3ODk6\nQ0RFRkdISUpTVFVWV1hZWmNkZWZnaGlqc3R1dnd4eXqDhIWGh4iJipKTlJWWl5iZ\nmqKjpKWmp6ipqrKztLW2t7i5usLDxMXGx8jJytLT1NXW19jZ2uHi4+Tl5ufo6erx\n8vP09fb3+Pn6/8QAHwEAAwEBAQEBAQEBAQAAAAAAAAECAwQFBgcICQoL/8QAtREA/>هوضو هب ار бла бла"""
    assert get_all_images_in_page(page)[0] == '<img width="270" height="180" src="data:image/jpeg;base64,\n/9j/4AAQSkZJRgABAQEAAAAAAAD/2wBDAAgGBgcGBQgHBwcJCQgKDBQNDAsLDBkS\nEw8UHRofHh0aHBwgJC4nICIsIxwcKDcpLDAxNDQ0Hyc5PTgyPC4zNDL/2wBDAQkJ\nCQwLDBgNDRgyIRwhMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIy\nMjIyMjIyMjIyMjIyMjL/wAARCAC3ARMDASIAAhEBAxEB/8QAHwAAAQUBAQEBAQEA\nAAAAAAAAAAECAwQFBgcICQoL/8QAtRAAAgEDAwIEAwUFBAQAAAF9AQIDAAQRBRIh\nMUEGE1FhByJxFDKBkaEII0KxwRVS0fAkM2JyggkKFhcYGRolJicoKSo0NTY3ODk6\nQ0RFRkdISUpTVFVWV1hZWmNkZWZnaGlqc3R1dnd4eXqDhIWGh4iJipKTlJWWl5iZ\nmqKjpKWmp6ipqrKztLW2t7i5usLDxMXGx8jJytLT1NXW19jZ2uHi4+Tl5ufo6erx\n8vP09fb3+Pn6/8QAHwEAAwEBAQEBAQEBAQAAAAAAAAECAwQFBgcICQoL/8QAtREA/>'

def test_remove_all_tags():
    page = "نکیمن دیلوت هوضو هب ار"
    assert remove_all_tags(page) == page

    page = "<h1> header \n header </h2>نکیمن دیلوت  <img jslfndl \n\n\n />هوضو هب ار бла бла"
    assert remove_all_tags(page) == "\n header \n header \nنکیمن دیلوت  \nهوضو هب ار бла бла"

def test_remove_all_tags_except_img():
    page = "نکیمن دیلوت هوضو هب ار"
    assert remove_all_tags_except_img(page) == page

    page = "<h1> header \n header </h2>نکیمن دیلوت  <img jslfndl \n\n\n />هوضو هب ار бла бла"
    assert remove_all_tags_except_img(page) == "\n header \n header \nنکیمن دیلوت  <img jslfndl \n\n\n />هوضو هب ار бла бла"

def test_remove_img_tag():
    page = "<h1> header \n header </h2>نکیمن دیلوت  <img jslfndl \n\n\n />هوضو هب ار бла бла"
    img = "<img jslfndl \n\n\n />"
    assert remove_img_tag(page=page, img=img) == "<h1> header \n header </h2>نکیمن دیلوت  هوضو هب ار бла бла"

def test_remove_digits():
    page = "<h1> 7494 header \n header </h2>نکیمن دیلوت  <img jslfndl \n\n\n />هوضو هب ار бла бла 34-89 34.67"
    assert remove_digits(page=page) == "<h>  header \n header </h>نکیمن دیلوت  <img jslfndl \n\n\n />هوضو هب ار бла бла - "

def test_detect_language():
    page = '''خداحافظ'''
    assert detect_language(page, encoding=None) == "fa"

    page = "а неправильный формат идентификатора дн назад"
    assert detect_language(page, encoding=None) == "ru"

    page = "OK I fixed the Python bindings to always return 3 languages even"
    assert detect_language(page, encoding=None) == "en"