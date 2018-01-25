from __future__ import print_function
from bs4 import BeautifulSoup
from collections import defaultdict
import db_adaptor as dba
from xml_tags import XmlTags as xt
REPLACE = [('&nbsp;',' '), ('&ndash;', '-'), ('&mdash;', '--'), ('&middot;','.'), ('&rsaquo;', '>')]

POPULARITY_FILENAME, POPULARITIES_DICT ='wiki09_count09_xml.csv', {}

def extract_images(soup, popularity):    
    images = []
    image_set = set()
    for img in soup.find_all(xt.IMAGE):
        img_src = img.get(xt.SRC).strip()
        if img_src not in image_set:
            images += [{xt.SRC:img_src, xt.CAPTION:img.get(xt.CAPTION), xt.POPULARITY:popularity}]
            image_set.add(img_src)
    return images


def extract_links(soup, popularity):    
    links = []
    link_set = set()
    for lk in soup.find_all(xt.LINK):
        wiki_relative_link = lk.get(xt.XLINK_HREF).strip()
        wiki_link = '//wikipedia.org/' + wiki_relative_link.rsplit('/',1)[1]
        if wiki_link not in link_set:
            links += [{xt.XLINK_HREF:wiki_link, xt.POPULARITY:popularity}]
            link_set.add(wiki_link)
    for lk in soup.find_all(xt.WEBLINK):
        web_link = lk.get(xt.XLINK_HREF).strip()
        if web_link not in link_set:
            links += [{xt.XLINK_HREF:web_link, xt.POPULARITY:popularity}]
            link_set.add(web_link)
    return links

def populate_db(db, soup):    
    artic_text = soup.get_text(strip=True)
    artic_id = soup.find(xt.HEADER).find(xt.ID).get_text(strip=True)
    artic_popularity = POPULARITIES_DICT[artic_id]
    db.insert_articles([{xt.ID:artic_id, xt.TEXT:artic_text, xt.POPULARITY:artic_popularity}])

    artic_image_dict_list = extract_images(soup, artic_popularity)
    image_ids = db.insert_images(artic_image_dict_list)
    
    artic_links_dict_list = extract_links(soup, artic_popularity)
    link_ids = db.insert_links(artic_links_dict_list)


def parse_direcotry(db, rootname, replace_strs=REPLACE):
    bad_files = []
    count_files = 0
    for root, dirs, files in os.walk(rootname):                            
        for f in files:
            xmlfilename = root+'/'+f
            count_files += 1
            print ('\rsuccess-rate:{.2f}\tscanning {}...'.format(len(bad_files)/count_files,
                os.path.abspath(xmlfilename)), end='')            
            filestr = ''
            f = open(xmlfilename, 'r')
            for l in f.readlines():
                filestr += l.strip()
            for s in replace_strs:
                filestr.replace(s[0],s[1])
            
            try:
                soup = BeautifulSoup(filestr, 'lxml')                
            except Exception as e:
                bad_files += [os.path.abspath(xmlfilename)]            

            populate_db(db, soup)

    print ('\nunsuccessful tries:\n', '\n'.join(bad_files))

def get_popularities(filename):
    pop_dict = defaultdict(int)
    with open(filename, 'r') as fr:
        for l in fr:
            l,pop = l.rsplit(',',1)
            art_id = l.rsplit('/',1)[1].split('.')[0]
            pop_dict[art_id.strip()] = int(pop.strip())
    return pop_dict

from db_adaptor import DatabaseAdaptor
if __name__ == '__main__':
    username = raw_input('username: ')
    password = getpass.getpass()
    db = DatabaseAdaptor(user_name = username, pass_word=password)
    POPULARITIES_DICT = get_popularities(POPULARITY_FILENAME)
    parse_direcotry(db)
    
