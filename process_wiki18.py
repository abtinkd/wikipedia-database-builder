import re, os, time, sys
import traverse_dir
from collections import defaultdict
from functools import partial

DEBUG_LOG_FILENAME = 'prepage_contents_{}.log'.format(time.strftime('%m%d_%H%M'))
prog_id = re.compile(r'<id>(.*)</id>', re.UNICODE | re.IGNORECASE)
prog_title = re.compile(r'<title>(.*)</title>', re.UNICODE | re.IGNORECASE)
prog_prepage = re.compile(r'.*[^\s]+.*<page>', re.UNICODE | re.IGNORECASE | re.DOTALL)


def extract_placeholders(dict, filepath):
    # pattern = r"([\[])\[([^\[\]]+)\]([\]])"
    pattern = r"(\[\[)([^\[\]]+)(\]\])"
    prog = re.compile(pattern)
    with open(filepath, 'r', encoding="utf-8") as fp:
        filetext = fp.read()

    result = prog.findall(filetext)
    pattern2 = r"(\[{2})[\w\d\s\(\)\.]+([^\s\w\d\]\(\)\.])[^\]]*(\]{2})"
    prog2 = re.compile(pattern2)
    for res in result:
        x = prog2.findall(res[1])
        if len(x) != 0:
            print(filepath)
            print(res[1])
            input()


def extract_pages(filepath, output_path):
    path = filepath.rsplit('/',1)[0]
    with open(filepath, 'r', encoding="utf-8") as f:
        page = ''
        page_count = 0
        for l in f:
            page += l
            if l.find(u'</page>') != -1:
                id = prog_id.search(page).group(1).strip()
                title = prog_title.search(page).group(1).strip()
                with open(output_path + '/'+ id +'.xml', 'w', encoding="utf-8") as fo:
                    fo.write(page)
                with open(path + '/id_title.csv', 'a', encoding="utf-8") as fo:
                    fo.write('{},{}\n'.format(id,title))

                prepage = prog_prepage.match(page)
                if prepage != None:
                    with open(path+'/'+DEBUG_LOG_FILENAME, 'a') as f:
                        f.write(id + ' -> ' + prepage.group() + '\n')

                page_count += page.count(u'<page>')
                page = ''
        with open(DEBUG_LOG_FILENAME, 'a') as f:
            f.write(page_count + '\n')

if __name__ == '__main__':
    path = sys.argv[1]
    out = sys.argv[2]

    extract_pages(path, output_path = out)
    # pd = defaultdict(str)
    # ep = partial(extract_placeholders, pd)
    # traverse_dir.apply_to('./data/articles/', ep)


