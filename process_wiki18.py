import re, regex, os, time, sys
import traverse_dir
from collections import defaultdict
from functools import partial

DEBUG_LOG_FILENAME = 'prepage_contents_{}.log'.format(time.strftime('%m%d_%H%M'))
prog_id = re.compile(r'<id>(.*)</id>', re.UNICODE | re.IGNORECASE)
prog_title = re.compile(r'<title>(.*)</title>', re.UNICODE | re.IGNORECASE)
prog_prepage = re.compile(r'^\s*[\S]+.*<page>', re.UNICODE | re.IGNORECASE | re.DOTALL)
# regex
prog_article_text = regex.compile(r'\<text[^\>]*\>(.*)\<\/text\>', regex.UNICODE | regex.IGNORECASE | regex.DOTALL)
prog_single_bracket = regex.compile(r'[^\[]\[((?>[^\[\]]+|(?R))*)\][^\]]', regex.UNICODE | regex.IGNORECASE)
prog_double_brackets = regex.compile(r'[^\[]\[{2}((?>[^\[\]]+|(?R))*)\]{2}[^\]]', regex.UNICODE | regex.IGNORECASE)
prog_web_links = regex.compile(r'(https?://)([^\s\]]+)(\]|\s)')
prog_images = regex.compile(r'(File|Image)(:|=)([^:=]+\.(png|jpe?g|svg|gif|tiff|xcf))', regex.UNICODE| regex.IGNORECASE)

def extract_placeholders(count, dict, filepath):
    with open(filepath, 'r', encoding="utf-8") as fp:
        filetext = fp.read()

    article_title = prog_title.search(filetext).group(1)
    article_id = prog_id.search(filetext).group(1)
    article_body = prog_article_text.search(filetext).group(1)

    # pat = prog_double_brackets.findall(article_body)
    # pat = prog_single_bracket.findall(article_body)
    # pat = prog_web_links.findall(article_body)
    pat = prog_images.findall(article_body)
    for p in pat:
        dict[p[2].strip()] +=1
        count[0] += 1

def extract_pages(filepath, output_path):
    path = filepath.rsplit('/',1)[0]
    stime = time.time()
    with open(filepath, 'r', encoding="utf-8") as fpoint:
        page = ''
        page_count = 0
        line_count = 0
        for l in fpoint:
            line_count +=1
            page += l
            if l.find(u'</page>') != -1:
                id = prog_id.search(page).group(1).strip()
                title = prog_title.search(page).group(1).strip()
                with open(output_path + '/'+ id +'.xml', 'w', encoding="utf-8") as fo:
                    fo.write(page)
                with open(path + '/id_title.csv', 'a', encoding="utf-8") as fo:
                    fo.write('{},{}\n'.format(id,title))

                prepage = prog_prepage.match(page)
                if prepage is not None:
                    with open(path+'/'+DEBUG_LOG_FILENAME, 'a', encoding="utf-8") as f:
                        f.write(id + ' -> ' + prepage.group() + '\n')

                page_count += page.count(u'<page>')
                page = ''
                if page_count%100 == 0:
                    print('{} lines   {} pages   {:.0f} minutes'.format(line_count, page_count, (time.time()-stime)/60))

        with open(DEBUG_LOG_FILENAME, 'a') as f:
            f.write(page_count + '\n')

if __name__ == '__main__':
    path = sys.argv[1]
    # out = sys.argv[2]

    # extract_pages(path, output_path = out)
    pd = defaultdict(int)
    count = [0]
    ep = partial(extract_placeholders, count, pd)
    traverse_dir.apply_to(path , ep)
    for p in pd:
        if pd[p] > 1:
            print(p, pd[p])
    print('Count: ', count)

