import sys
from collections import defaultdict


def extract_tree_dict(treetxt):
    treedict = defaultdict(int)
    pass

def extract_tags_dict(tagstxt):
    tagsdict = defaultdict(int)
    with open(tagstxt, 'r') as fr:
        for line in fr:
            tag, count  = line.strip().split()
            tagsdict[tag] += int(count)
    return tagsdict


def extract_siblings_dict(siblingstxt):
    siblingsdict = defaultdict(lambda: defaultdict(int))
    with open(siblingstxt, 'r') as fr:
        for line in fr:                
            root, right = line.replace('->','').strip().split(None, 1)                        
            right = right.strip().rsplit(None,1)
            if len(right) == 1:
                sibling, count = '', right[0]
            else:
                sibling, count = right
            # siblingsdict[root][sibling] += int(count)
            for s in sibling.split():            
                siblingsdict[root][s] += int(count)        
    return siblingsdict


if __name__ == '__main__':
    treetxt, tagstxt, siblingstxt = 'tree.txt', 'tags.txt', 'siblings.txt'
    if len(sys.argv) >=2:
        treetxt = sys.argv[1]
        if len(sys.argv) >= 3:
            tagstxt = sys.argv[2]
            if len(sys.argv) >= 4:
                siblingstxt = sys.argv[3]

    # treedict = extract_tree_dict(treetxt)
    tagsdict = extract_tags_dict(tagstxt)
    siblingsdict = extract_siblings_dict(siblingstxt)
    word = 'root'
    while word != 'y' and word !='Y':
        print '{} #{}'.format(word,tagsdict[word])
        for s,v in sorted(siblingsdict[word].iteritems(), key=lambda (k,v): (v,k), reverse=True):
            print '{}\t\t({} % {:.1f} % {:.3f})'.format(s,v,float(v)/tagsdict[word],float(v)/tagsdict[s])
        print '------------------------------------------------------------'
        word = raw_input('root = ')