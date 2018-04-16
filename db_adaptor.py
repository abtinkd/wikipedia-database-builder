import MySQLdb
from xml_tags import Tags as xt

class DatabaseAdaptor(object):
    """docstring for DatabaseAdaptor"""    
    def __init__(self, username, password, hostname,
                 db_name, socket):        
        super(DatabaseAdaptor, self).__init__()        
        print 'Connecting to database \'{}\' for \'{}\''.format(db_name, username+'@'+hostname)        
        try:
            self.__db = MySQLdb.connect(host=hostname,
                         user=username,
                         passwd=password,
                         db=db_name,
                         unix_socket = socket,
                         charset='utf8')
            
            self.__create_tables()
            self.id_article_list = self.__get_existed_article_ids()
            self.link_id_dict = self.__get_all_links_dict()
            self.image_id_dict = self.__get_all_images_dict()
        except Exception as e:            
            print 'Database connection failed!'
            raise e        
        print 'Database connection established successfully!'
        print '---------------------------------------------'

    def __drop_all_tables(self):
        sql_script_list = [\
        'DROP TABLE IF EXISTS {};'.format(xt.tbl_article_link),
        'DROP TABLE IF EXISTS {};'.format(xt.tbl_article_image),
        'DROP TABLE IF EXISTS {};'.format(xt.tbl_image),
        'DROP TABLE IF EXISTS {};'.format(xt.tbl_link),
        'DROP TABLE IF EXISTS {};'.format(xt.tbl_article)]
        return self.execute_sql(sql_script_list)

    def __get_existed_article_ids(self):
        sql_script = u'SELECT DISTINCT {id} FROM {tbl};'.format(id=xt.ID, tbl=xt.tbl_article)
        _, res = self.execute_sql([sql_script])        
        ids = [int(o[0]) for o in res[0]]
        return ids
    
    def __get_all_links_dict(self):
        sql_script = u'SELECT {id}, {lnk} FROM {tbl}'.format(id=xt.ID, lnk=xt.XLINK_HREF.replace(':','_'), tbl=xt.tbl_link)
        _, res = self.execute_sql([sql_script])
        mp = {o[0]:o[1] for o in res[0]}
        return mp

    def __get_all_images_dict(self):
        sql_script = u'SELECT {id}, {src} FROM {tbl}'.format(id=xt.ID, src=xt.SRC, tbl=xt.tbl_image)
        _, res = self.execute_sql([sql_script])
        mp = {o[0]:o[1] for o in res[0]}
        return mp

    def execute_sql(self, sql_script_list):        
        cursor = self.__db.cursor()        
        count, output = 0L, []
        for sql_script in sql_script_list:
            try:            
                count += cursor.execute(sql_script)
                output += [cursor.fetchall()]                
            except Exception as e:                
                print '\nSQL SCRIPT:\n{}\n!!!!!!!!!!!!!!!!!!!!!\n'.format(sql_script)
                raise e
        self.__db.commit()
        cursor.close()
        return count, output

    def insert_articles(self, list_attrib_value):
        sql_script_list = []
        for att_val in list_attrib_value:
            sql_script = u'''INSERT INTO {0} ({1}, {3}, {5}, {7}) VALUES ({2}, {4}, {6}, {8})
                            ON DUPLICATE KEY UPDATE {5} = {5}+{6};'''\
                                            .format(xt.tbl_article,
                                            xt.ID, att_val[xt.ID],
                                            xt.TEXT, att_val[xt.TEXT],
                                            xt.POPULARITY, att_val[xt.POPULARITY],
                                            xt.TITLE, att_val[xt.TITLE])
            sql_script_list += [sql_script]
        return self.execute_sql(sql_script_list)    

    def insert_images(self, list_attrib_value):
        sql_script_list = []
        for att_val in list_attrib_value:
            img_src, img_id = att_val[xt.SRC], -1
            if self.image_id_dict.has_key(img_src):
                img_id = self.image_id_dict[img_src]
            else:
                img_id = len(self.image_id_dict)+1
                self.image_id_dict[img_src] = img_id
            sql_script = u'''INSERT INTO {tbl} ({id}, {src}, {cap}, {pop}) VALUES ({id_v}, {src_v}, {cap_v}, {pop_v})
                            ON DUPLICATE KEY UPDATE {pop} = {pop}+{pop_v};'''\
                                            .format(tbl=xt.tbl_image,
                                            id=xt.ID, id_v=img_id,
                                            src=xt.SRC, src_v=img_src,
                                            cap=xt.CAPTION, cap_v=att_val[xt.CAPTION],
                                            pop=xt.POPULARITY, pop_v=att_val[xt.POPULARITY])
            sql_script_list += [sql_script]
        return self.execute_sql(sql_script_list)

    def insert_links(self, list_attrib_value):        
        sql_script_list = []
        for att_val in list_attrib_value:
            lnk_xref, lnk_id = att_val[xt.XLINK_HREF], -1
            if self.link_id_dict.has_key(lnk_xref):
                lnk_id = self.link_id_dict[lnk_xref]
            else:
                lnk_id = len(self.link_id_dict)+1
                self.link_id_dict[lnk_xref] = lnk_id
            sql_script = u'''INSERT INTO {tbl} ({id}, {xlk}, {pop}) VALUES ({id_v}, {xlk_v}, {pop_v})
                            ON DUPLICATE KEY UPDATE {pop} = {pop}+{pop_v};'''\
                                            .format(tbl=xt.tbl_link,
                                            id=xt.ID, id_v=lnk_id,
                                            xlk=xt.XLINK_HREF.replace(':','_'), xlk_v=lnk_xref,
                                            pop=xt.POPULARITY, pop_v=att_val[xt.POPULARITY])
            sql_script_list += [sql_script]
        return self.execute_sql(sql_script_list)
    
    def insert_article_link(self, article_id, popularity, lnk_attrib_value_list):    
        href_list = [lnk[xt.XLINK_HREF] for lnk in lnk_attrib_value_list]        
        lnk_id_list = [self.link_id_dict[hr] for hr in href_list] 

        sql_script_list = []
        for lnk_id in lnk_id_list:
            sql_script = u'''INSERT INTO {t_a_l} ({aid}, {lid}, {pop}) VALUES ({vaid}, {vlid}, {vpop})
                            ON DUPLICATE KEY UPDATE {pop} = {pop}+{vpop};'''\
                                            .format(t_a_l=xt.tbl_article_link,
                                            aid=xt.ARTICLE_ID, vaid=article_id,
                                            lid=xt.LINK_ID, vlid=lnk_id,
                                            pop=xt.POPULARITY, vpop=popularity)
            sql_script_list += [sql_script]
        return self.execute_sql(sql_script_list)

    def insert_article_image(self, article_id, popularity, img_attrib_value_list):
        src_list = [img[xt.SRC] for img in img_attrib_value_list]
        img_id_list = [self.image_id_dict[sr] for sr in src_list]

        sql_script_list = []
        for img_id in img_id_list:
            sql_script = u'''INSERT INTO {t_a_i} ({aid}, {iid}, {pop}) VALUES ({vaid}, {viid}, {vpop})
                            ON DUPLICATE KEY UPDATE {pop} = {pop}+{vpop};'''\
                                            .format(t_a_i=xt.tbl_article_image,
                                            aid=xt.ARTICLE_ID, vaid=article_id,
                                            iid=xt.IMAGE_ID, viid=img_id,
                                            pop=xt.POPULARITY, vpop=popularity)
            sql_script_list += [sql_script]
        return self.execute_sql(sql_script_list)

    def close_connection(self):
        self.__db.close()    

    def __create_tables(self):        
        sql = 'SHOW TABLES LIKE \'{tbl_article}\';'.format(tbl_article=xt.tbl_article)
        c,_ = self.execute_sql([sql])    
        if c != 0L:
            return

        sql_list = []
        sql_list += [u'''CREATE TABLE {t} ({id} INT NOT NULL PRIMARY KEY,{ttl} TEXT,{txt} MEDIUMTEXT,{pop} INTEGER) CHARACTER SET utf8 COLLATE utf8_bin;
                                        \n'''.format(t=xt.tbl_article, id=xt.ID, txt=xt.TEXT, pop=xt.POPULARITY, ttl=xt.TITLE)]
        sql_list += [u'''CREATE TABLE {t} ({id} INT NOT NULL AUTO_INCREMENT PRIMARY KEY,{xref} VARCHAR(255) NOT NULL UNIQUE,{pop} INT) CHARACTER SET utf8 COLLATE utf8_bin;
                                        \n'''.format(t=xt.tbl_link, id=xt.ID, xref=xt.XLINK_HREF.replace(':','_'), pop=xt.POPULARITY)]
        sql_list += [u'''CREATE TABLE {t} ({id} INT NOT NULL AUTO_INCREMENT PRIMARY KEY,{src} VARCHAR(255) NOT NULL UNIQUE,{cap} TEXT,{pop} INT) CHARACTER SET utf8 COLLATE utf8_bin;
                                        \n'''.format(t=xt.tbl_image, id=xt.ID, src=xt.SRC, cap=xt.CAPTION, pop=xt.POPULARITY)]
        sql_list += [u'''CREATE TABLE {t_a_l} ({aid} INT NOT NULL, {lid} INT NOT NULL, {pop} INT,
                                        PRIMARY KEY ({aid}, {lid}),
                                        FOREIGN KEY ({aid}) REFERENCES {t_a} ({id}) ON DELETE RESTRICT ON UPDATE CASCADE,
                                        FOREIGN KEY ({lid}) REFERENCES {t_l} ({id}) ON DELETE RESTRICT ON UPDATE CASCADE) CHARACTER SET utf8 COLLATE utf8_bin;
                                        \n'''.format(t_a_l=xt.tbl_article_link, aid=xt.ARTICLE_ID, lid=xt.LINK_ID,
                                                    pop=xt.POPULARITY, t_a=xt.tbl_article, t_l=xt.tbl_link, id=xt.ID)]
        sql_list += [u'''CREATE TABLE {t_a_i} ({aid} INT NOT NULL, {iid} INT NOT NULL, {pop} INT,
                                        PRIMARY KEY ({aid}, {iid}),
                                        FOREIGN KEY ({aid}) REFERENCES {t_a} ({id}) ON DELETE RESTRICT ON UPDATE CASCADE,
                                        FOREIGN KEY ({iid}) REFERENCES {t_i} ({id}) ON DELETE RESTRICT ON UPDATE CASCADE) CHARACTER SET utf8 COLLATE utf8_bin;
                                        \n'''.format(pop=xt.POPULARITY, id=xt.ID, t_a_i=xt.tbl_article_image, aid=xt.ARTICLE_ID,
                                                    iid=xt.IMAGE_ID, t_i=xt.tbl_image, t_a=xt.tbl_article)]        
        
        self.execute_sql(sql_list)