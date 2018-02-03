class Tags(object):
    """docstring for Tags"""
    IMAGE, SRC, CAPTION = 'image', 'src', 'caption'
    LINK, WEBLINK, XLINK_HREF = 'link', 'weblink', 'xlink:href'
    POPULARITY = 'popularity'
    ID, TEXT, HEADER, TITLE, WIKI09_BODY = 'id', 'text', 'header', 'title', 'text_wiki09'
    ARTICLE_ID, LINK_ID, IMAGE_ID = 'article_id', 'link_id', 'image_id'
    
    tbl_article = 'tbl_article_wiki13'
    tbl_link = 'tbl_link_wiki13'
    tbl_image = 'tbl_image_13'
    tbl_article_image = 'tbl_article_image_13'
    tbl_article_link = 'tbl_article_link_13'

    def __init__(self):
        super(Tags, self).__init__()