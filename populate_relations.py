from __future__ import print_function
from collections import defaultdict
from bs4 import BeautifulSoup
from xml_tags import Tags as xt
import codecs, os, time

POPULARITY_FILENAME, WIKI13_CSV_FILENAME, POPULARITIES_DICT, TITLES_DICT = 'wiki09_count09_xml.csv', 'wiki13_counts13_title.csv', {}, {}
ERROR_LOG_FILENAME = 'failure_{}.log'.format(time.strftime('%m%d_%H%M'))

def read_file(filepath, return_repr=False):
    with codecs.open(filepath, 'r', encoding='utf-8') as fp:
        ustr = fp.read()
        if return_repr:
            return repr(ustr)
        else:
            return ustr

def convert_to_sql_text (txt, truncate_size = -1): 
    if txt == None or txt == '':
        return '\'\''
    txt =  txt.strip().replace('\'','\'\'').replace('\"','\'\'').replace('\n',' ').replace('\\\'', '\'')
    if truncate_size > 0 and len(txt) > truncate_size-2:
        txt = txt[:truncate_size-2]
    if txt[-1] == '\\':
        txt = txt[:-1]+'/'
    return '\'' + txt + '\''

def extract_images(soup, popularity):    
    images = []
    image_set = set()
    for img in soup.find_all(xt.IMAGE):
        src = img.get(xt.SRC)
        if src == None or src.strip()== '':
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
        if wiki_relative_link.find('/') != -1:
            wiki_relative_link = wiki_relative_link.strip().rsplit('/',1)[1]
        wiki_link = wiki_relative_link.split('.')[0]
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

def populate_db(artic_id, db, soup):    
    header_id = int(soup.find(xt.HEADER).find(xt.ID).get_text(strip=True))    
    if artic_id != header_id:
        with open('unmatched_id.log', 'a') as fp:
            fp.write('h:{} id:{}\n'.format(header_id, artic_id))

    artic_text = convert_to_sql_text(soup.get_text())     
    artic_title = convert_to_sql_text(soup.find(xt.TITLE).get_text())
    artic_popularity = POPULARITIES_DICT[artic_id]
    db.insert_articles([{xt.ID:artic_id, xt.TEXT:artic_text, xt.POPULARITY:artic_popularity, xt.TITLE:artic_title}])

    artic_image_dict_list = extract_images(soup, artic_popularity)
    db.insert_images(artic_image_dict_list)
    db.insert_article_image(artic_id, artic_popularity, artic_image_dict_list) 

    artic_links_dict_list = extract_links(soup, artic_popularity)
    db.insert_links(artic_links_dict_list)        
    db.insert_article_link(artic_id, artic_popularity, artic_links_dict_list)

import re
def get_article_id_from_file_name(filename):    
    n = re.sub('[^0-9]', '', filename)
    if n != '' and n != None:
        return int(n)
    else:
        return -1

def populate_db_wiki13_article(artic_id, artic_text):
    if artic_id == -1:
        return

    artic_text = convert_to_sql_text(artic_text)     
    artic_title = convert_to_sql_text(TITLES_DICT[artic_id])
    artic_popularity = POPULARITIES_DICT[artic_id]
    db.insert_articles([{xt.ID:artic_id, xt.TEXT:artic_text, xt.POPULARITY:artic_popularity, xt.TITLE:artic_title}])
    return


def parse_direcotry(db, rootname):    
    bad_files = []
    count = [0,0]
    tm = time.time()
    speed = 1
    for root, dirs, files in os.walk(rootname):
        for f in files:
            count[0] += 1
            if count[0]%1000 == 0:
                speed = (time.time()-tm)/1000.0
                tm = time.time()
            xmlfilename = root+'/'+f
            print ('\rfailure-rate:{:.6f}     {} | {:.5f}(s) | {}...      '.format(len(bad_files)/float(count[1]+1), count, speed,
                os.path.abspath(xmlfilename)), end='')

            aid = get_article_id_from_file_name(f)
            if aid == -1:
                bad_files += [os.path.abspath(xmlfilename)]
                continue
            if aid in db.id_article_list:
                continue                        

            filestr = read_file(xmlfilename)            
            try:                
                soup = BeautifulSoup(filestr, 'lxml')
                populate_db(aid, db, soup)
                count[1] +=1
            except Exception as e:
                abs_filename = os.path.abspath(xmlfilename)
                bad_files += [abs_filename]
                with open(ERROR_LOG_FILENAME,'a') as f:
                    f.write(abs_filename+'  '+str(e)+'\n')
                # raise e            
    bd_str = '\n'.join(bad_files)
    print ('\nunsuccessful tries:\n{}'.format(bd_str))    

def get_popularities_from_csv_09(filename):
    print ('Reading popularities from {}...'.format(filename))
    pop_dict = defaultdict(int)
    with codecs.open(filename, 'r', encoding='utf-8') as fr:
        for l in fr:            
            lparts = l.rsplit(',',1)
            artic_id = get_article_id_from_file_name(lparts[0].rsplit('/',1)[1].strip())
            pop_dict[artic_id] = int(lparts[1].strip())
    return pop_dict

def get_popularities_title_from_csv_13(filename):
    print ('Reading csv for popularities and title from {}...'.format(filename))
    pop_dict = defaultdict(int)
    title_dict = defaultdict(str)    
    with codecs.open(filename, 'r', encoding='utf-8') as fr:
        for l in fr:            
            lparts = l.split(',')
            artic_id = get_article_id_from_file_name(lparts[0].rsplit('/',1)[1].strip())
            artic_popularity = int(lparts[1].strip())
            artic_title = lparts[2].strip()
            pop_dict[artic_id] = artic_popularity
            title_dict[artic_id] = artic_title
    return pop_dict, title_dict

from db_adaptor import DatabaseAdaptor
import sys
import getpass
if __name__ == '__main__':
    rootname = './xml'
    if len(sys.argv) > 1:
            rootname = sys.argv[1]
    hostname = raw_input('host: ')
    db_name = raw_input('database name: ')
    username = raw_input('username: ')
    password = getpass.getpass()
    socket = raw_input('socket (if nothing press enter): ')
    creditentials = {}
    
    if os.path.exists('credentials.txt'):
        with open('credentials.txt', 'r') as f:
            cred = f.readlines()
    creditentials['username'] = cred[0].strip()
    creditentials['password'] = cred[1].strip()
    creditentials['hostname'] = cred[2].strip()
    creditentials['db_name'] = cred[3].strip()
    creditentials['socket'] = cred[4].strip()
    
    if hostname != '':
        creditentials['hostname'] = hostname
    if db_name != '':
        creditentials['db_name'] = db_name
    if username != '':
        creditentials['username'] = username
    if password != '':
        creditentials['password'] = password
    if socket != '':
        creditentials['socket'] = socket

    db = DatabaseAdaptor(**creditentials)    
    POPULARITIES_DICT = get_popularities_from_csv_09(POPULARITY_FILENAME)    
    # POPULARITIES_DICT, TITLES_DICT = get_popularities_title_from_csv_13(WIKI13_CSV_FILENAME)
    parse_direcotry(db, rootname)