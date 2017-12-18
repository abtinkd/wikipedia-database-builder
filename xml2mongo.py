from __future__ import print_function
import os, sys, json
from datetime import datetime
from collections import OrderedDict
import xmltodict
from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client['wikipedia']
coll = db['articles']
# print client.database_names()
# print db.collection_names()
# cursor = coll.find({})
# for document in cursor:
#         print document

def parse_direcotry(rootname, outputdir):       
        bad_files = []
        for root, dirs, files in os.walk(rootname):                
                for d in dirs:
                        curdir = outputdir + '/' + root.replace(rootname, '') + '/' + d
                        if not os.path.exists(curdir):
                                os.makedirs(curdir)                
		for f in files:
			xmlfilename = root+'/'+f
                        print ('\rimporting {}...'.format(os.path.abspath(xmlfilename)), end='')
			jsonfilename = root.replace(rootname, outputdir) + '/' + f + '.json'
			filestr = ''
			f = open(xmlfilename, 'r')
			for l in f.readlines():
				filestr += l.strip()                                        
			filestr = filestr.replace('&nbsp;',' ')\
                                .replace('&ndash;', '-')\
                                .replace('&mdash;', '--')\
                                .replace('&middot;','.')\
                                .replace('&rsaquo;', '>')
                        try:
                                dictdata = xmltodict.parse(filestr)
                                with open(jsonfilename, 'w') as wf:                                             
                                        json.dump(dictdata, wf)                                                    
                                coll.insert_one(dictdata)
                        except Exception as e:
                                bad_files += [os.path.abspath(xmlfilename)]
        print ('\nunsuccessful tries:\n', '\n'.join(bad_files))


if __name__ == '__main__':
        rootname = './xml'
        # output = './output_' + datetime.now().strftime('%Y%m%d_%H%M%Z') + '_1'
        output = './output'
        # if os.path.exists(output):
        #         output = output[:-1] + str(int(output[-1])+1)
	if not os.path.exists(output):                
                os.makedirs(output)
        print ('Json directory: {}'.format(os.path.abspath(output)),)
        if len(sys.argv) > 1:
                rootname = sys.argv[1]
        parsedtree = parse_direcotry(rootname, output)

