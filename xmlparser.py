from __future__ import print_function
import os, sys, collections
from datetime import datetime
import xmltodict
from collections import defaultdict

IGNORE_TAGS = 'sec p table row col ss1 ss2 ss3 ss4 list entry it b st'.encode('utf-8').split()

def extract_xml_tag_tree(dictdata, root, path, tags, tags_tree, tags_siblings):    
    if isinstance(dictdata, collections.OrderedDict):
        tags[root] += 1
        siblings = root + ' ->'
        for key,value in dictdata.iteritems():
            if key not in IGNORE_TAGS:
                extract_xml_tag_tree(value, key, path+'.'+key, tags, tags_tree, tags_siblings)
                siblings += '  ' + key
            else:            
                extract_xml_tag_tree(value, root, path, tags, tags_tree, tags_siblings)            
        tags_siblings[siblings] += 1
    elif isinstance(dictdata, list):        
        for i in dictdata:            
            extract_xml_tag_tree(i, root, path, tags, tags_tree, tags_siblings)
    else:
        tags[root] += 1
        tags_tree[path] += 1    



def parse_direcotry(rootname):       
    bad_files = []
    tags_tree, tags, tags_siblings = defaultdict(int), defaultdict(int), defaultdict(int)
    for root, dirs, files in os.walk(rootname):                            
        for f in files:
            xmlfilename = root+'/'+f
            print ('\rscanning {}...'.format(os.path.abspath(xmlfilename)), end='')            
            filestr = ''
            f = open(xmlfilename, 'r')
            for l in f.readlines():
                filestr += l.strip()                                        
            filestr = filestr.replace('&nbsp;',' ')\
                                .replace('&ndash;', '-')\
                                .replace('&mdash;', '--')\
                                .replace('&middot;','.')\
                                .replace('&rsaquo;', '>')        
            # dictdata = xmltodict.parse(filestr)        
            # extract_xml_tag_tree(dictdata, 'root', 'root', tags, tags_tree)
            try:                
                dictdata = xmltodict.parse(filestr)        
                extract_xml_tag_tree(dictdata, 'root', 'root', tags, tags_tree, tags_siblings)
            except Exception as e:
                bad_files += [os.path.abspath(xmlfilename)]
    print ('\nunsuccessful tries:\n', '\n'.join(bad_files))                
    with open('tree.txt','w') as fo:
        for key, value in sorted(tags_tree.iteritems(), key=lambda (k,v): (v,k), reverse=True):
            fo.write(key.encode('utf-8')+'\t'+str(value)+'\n')
    with open('tags.txt','w') as fo:
        for key, value in sorted(tags.iteritems(), key=lambda (k,v): (v,k), reverse=True):
            fo.write(key.encode('utf-8')+'\t'+str(value)+'\n')
    with open('siblings.txt','w') as fo:
        for key, value in sorted(tags_siblings.iteritems(), key=lambda (k,v): (v,k), reverse=True):
            fo.write(key.encode('utf-8')+'\t'+str(value)+'\n')




if __name__ == '__main__':
    rootname = './xml'
    if len(sys.argv) > 1:
            rootname = sys.argv[1]
    parsedtree = parse_direcotry(rootname)

