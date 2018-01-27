class Tags(object):
    """docstring for Tags"""
    IMAGE, SRC, CAPTION = 'image', 'src', 'caption'
    LINK, WEBLINK, XLINK_HREF = 'link', 'weblink', 'xlink:href'
    POPULARITY = 'popularity'
    ID, TEXT, HEADER = 'id', 'text', 'header'
    ARTICLE_ID, LINK_ID, IMAGE_ID = 'article_id', 'link_id', 'image_id'
    
    tbl_article = 'tbl_article'
    tbl_link = 'tbl_link'
    tbl_image = 'tbl_image'
    tbl_article_image = 'tbl_article_image'
    tbl_article_link = 'tbl_article_link'

    def __init__(self):
        super(Tags, self).__init__()