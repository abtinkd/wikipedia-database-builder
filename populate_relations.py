from __future__ import print_function
from bs4 import BeautifulSoup
from collections import defaultdict
import db_adaptor as dba
from xml_tags import XmlTags as xt

POPULARITY_FILENAME, POPULARITIES_DICT ='wiki09_count09_xml.csv', {}

def convert_to_sql_text(txt, truncate_size = -1):
    if txt == None:
        return '\'\''
    txt =  txt.strip().replace('\'','\'\'').replace('\"','\'\'').replace('\n',' ')
    if truncate_size > 0 and len(txt) > truncate_size-2:
        txt = txt[:truncate_size-2]
    return u'\'' + txt + '\''

def extract_images(soup, popularity):    
    images = []
    image_set = set()
    for img in soup.find_all(xt.IMAGE):
        src = img.get(xt.SRC)
        if src == None:
            continue
        img_src = convert_to_sql_text(src,255)        
        if img_src not in image_set:
            images += [{xt.SRC:img_src, xt.CAPTION:convert_to_sql_text(img.get(xt.CAPTION)), xt.POPULARITY:popularity}]
            image_set.add(img_src)
    return images


def extract_links(soup, popularity):    
    links = []
    link_set = set()
    for lk in soup.find_all(xt.LINK):
        wiki_relative_link = lk.get(xt.XLINK_HREF)
        if wiki_relative_link == None:
            continue
        wiki_link = '//wikipedia.org/' + wiki_relative_link.strip().rsplit('/',1)[1]
        wiki_link = convert_to_sql_text(wiki_link,255)
        if wiki_link not in link_set:
            links += [{xt.XLINK_HREF:wiki_link, xt.POPULARITY:popularity}]
            link_set.add(wiki_link)
    for lk in soup.find_all(xt.WEBLINK):
        web_link = lk.get(xt.XLINK_HREF)
        if web_link == None:
            continue
        web_link = convert_to_sql_text(web_link,255)
        if web_link not in link_set:
            links += [{xt.XLINK_HREF:web_link, xt.POPULARITY:popularity}]
            link_set.add(web_link)
    return links

def populate_db(db, soup):    
    artic_id = soup.find(xt.HEADER).find(xt.ID).get_text(strip=True)
    if artic_id == None:
        return True
    artic_text = '\'TE\'\'X\'\'T\''
    # artic_text = convert_to_sql_text(soup.get_text())
    # artic_text = '\'' + artic_text.replace('\'','\'\'').replace('\"','\'\'').replace('\n',' ') + '\''    
    artic_popularity = int(POPULARITIES_DICT[artic_id])
    db.insert_articles([{xt.ID:artic_id, xt.TEXT:artic_text, xt.POPULARITY:artic_popularity}])

    artic_image_dict_list = extract_images(soup, artic_popularity)
    db.insert_images(artic_image_dict_list)    
    
    artic_links_dict_list = extract_links(soup, artic_popularity)
    db.insert_links(artic_links_dict_list)

    # art_img_id_dict_list = []
    # for img_id in image_ids:
    #     art_img_id_dict_list += [{xt.ARTICLE_ID:artic_id, xt.IMAGE_ID:img_id, xt.POPULARITY:artic_popularity}]
    # db.insert_article_image(art_img_id_dict_list)

    # art_lnk_id_dict_list = []
    # for lnk_id in link_ids:
    #     art_lnk_id_dict_list += [{xt.ARTICLE_ID:artic_id, xt.LINK_ID:lnk_id, xt.POPULARITY:artic_popularity}]
    # db.insert_article_link(art_lnk_id_dict_list)
    return False

import os
def parse_direcotry(db, rootname, replace_strs=REPLACE):
    bad_files = []
    count_files = 0
    for root, dirs, files in os.walk(rootname):                            
        for f in files:
            xmlfilename = root+'/'+f
            count_files += 1
            print ('\rsuccess-rate:{:.4f}\t{}.scanning {}...'.format(len(bad_files)/float(count_files), count_files,
                os.path.abspath(xmlfilename)), end='')            
            filestr = ''
            with open(xmlfilename, 'r') as fr:                
                filestr = fr.read() 

            try:                
                soup = BeautifulSoup(filestr, 'lxml')
                populate_db(db, soup)
            except Exception as e:
                raise e              
                bad_files += [os.path.abspath(xmlfilename)]            

    print ('\nunsuccessful tries:\n', '\n'.join(bad_files))

def get_popularities(filename):
    pop_dict = defaultdict(int)
    with open(filename, 'r') as fr:
        for l in fr:
            l = l.encode('utf-8')
            l,pop = l.rsplit(',',1)
            art_id = l.rsplit('/',1)[1].split('.')[0]
            pop_dict[art_id.strip()] = int(pop.strip())
    return pop_dict

from db_adaptor import DatabaseAdaptor
import sys
import getpass
if __name__ == '__main__':
    rootname = './xml'
    if len(sys.argv) > 1:
            rootname = sys.argv[1]
    # user_name = raw_input('username: ')
    # pass_word = getpass.getpass()
    # db = DatabaseAdaptor(username = user_name, password=pass_word)
    db = DatabaseAdaptor()
    POPULARITIES_DICT = get_popularities(POPULARITY_FILENAME)
    parse_direcotry(db, rootname)
    
