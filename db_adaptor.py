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
            
            self.__drop_all_tables()            
            self.__create_tables()
        except Exception as e:            
            print 'Database connection failed!'
            raise e        
        print 'Database connection established successfully!'
        print '---------------------------------------------'

    def __drop_all_tables(self):
        sql_script_list = [\
        'DROP TABLE IF EXISTS tbl_article_link;',
        'DROP TABLE IF EXISTS tbl_article_image;',
        'DROP TABLE IF EXISTS tbl_image;',
        'DROP TABLE IF EXISTS tbl_link;',
        'DROP TABLE IF EXISTS tbl_article;']
        return self.execute_sql(sql_script_list)

        
    def execute_sql(self, sql_script_list):        
        cursor = self.__db.cursor()        
        count, output = 0L, []
        for sql_script in sql_script_list:
            try:            
                count += cursor.execute(sql_script)
                output += [cursor.fetchall()]
                self.__db.commit()                                
            except Exception as e:                
                print '\nSQL SCRIPT:\n{}\n!!!!!!!!!!!!!!!!!!!!!'.format(sql_script)
                raise e                        
        cursor.close()
        return count, output

    def insert_articles(self, list_attrib_value):
        sql_script_list = []
        for att_val in list_attrib_value:
            sql_script = u'''INSERT INTO {0} ({1}, {3}, {5}) VALUES ({2}, {4}, {6})
                            ON DUPLICATE KEY UPDATE {5} = {5}+{6};'''\
                                            .format(xt.tbl_article,
                                            xt.ID, att_val[xt.ID],
                                            xt.TEXT, att_val[xt.TEXT],
                                            xt.POPULARITY, att_val[xt.POPULARITY])
            sql_script_list += [sql_script]
        return self.execute_sql(sql_script_list)    

    def insert_images(self, list_attrib_value):
        sql_script_list = []
        for att_val in list_attrib_value:
            sql_script = u'''INSERT INTO {0} ({1}, {3}, {5}) VALUES ({2}, {4}, {6})
                            ON DUPLICATE KEY UPDATE {5} = {5}+{6};'''\
                                            .format(xt.tbl_image,
                                            xt.SRC, att_val[xt.SRC],
                                            xt.CAPTION, att_val[xt.CAPTION],
                                            xt.POPULARITY, att_val[xt.POPULARITY])
            sql_script_list += [sql_script]
        return self.execute_sql(sql_script_list)

    def insert_links(self, list_attrib_value):        
        sql_script_list = []
        for att_val in list_attrib_value:
            sql_script = u'''INSERT INTO {0} ({1}, {3}) VALUES ({2}, {4})
                            ON DUPLICATE KEY UPDATE {3} = {3}+{4};'''\
                                            .format(xt.tbl_link,
                                            xt.XLINK_HREF.replace(':','_'), att_val[xt.XLINK_HREF],
                                            xt.POPULARITY, att_val[xt.POPULARITY])
            sql_script_list += [sql_script]
        return self.execute_sql(sql_script_list)
    
    def insert_article_link(self, article_id, popularity, lnk_attrib_value_list):    
        href_list = [lnk[xt.XLINK_HREF] for lnk in lnk_attrib_value_list]        
        sql_list = []
        for h in href_list:
            sql_list += [u'SELECT {id} FROM {t_l} WHERE {href} = {h};'.format(id=xt.ID, t_l=xt.tbl_link, href=xt.XLINK_HREF.replace(':','_'), h=h)]
        _,lnk_id_list = self.execute_sql(sql_list)

        sql_script_list = []
        for li in lnk_id_list:
            sql_script = u'''INSERT INTO {t_a_l} ({aid}, {lid}, {pop}) VALUES ({vaid}, {vlid}, {vpop})
                            ON DUPLICATE KEY UPDATE {pop} = {pop}+{vpop};'''\
                                            .format(t_a_l=xt.tbl_article_link,
                                            aid=xt.ARTICLE_ID, vaid=article_id,
                                            lid=xt.LINK_ID, vlid=li[0][0],
                                            pop=xt.POPULARITY, vpop=popularity)
            sql_script_list += [sql_script]
        return self.execute_sql(sql_script_list)

    def insert_article_image(self, article_id, popularity, img_attrib_value_list):
        src_list = [img[xt.SRC] for img in img_attrib_value_list]        
        sql_list = []
        for s in src_list:
            sql_list += [u'SELECT {id} FROM {t_i} WHERE {src} = {s};'.format(id=xt.ID, t_i=xt.tbl_image, src=xt.SRC, s=s)]
        _,img_id_list = self.execute_sql(sql_list)

        sql_script_list = []
        for ii in img_id_list:
            sql_script = u'''INSERT INTO {t_a_i} ({aid}, {iid}, {pop}) VALUES ({vaid}, {viid}, {vpop})
                            ON DUPLICATE KEY UPDATE {pop} = {pop}+{vpop};'''\
                                            .format(t_a_i=xt.tbl_article_image,
                                            aid=xt.ARTICLE_ID, vaid=article_id,
                                            iid=xt.IMAGE_ID, viid=ii[0][0],
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
        sql_list += [u'CREATE TABLE {t} ({id} INT NOT NULL PRIMARY KEY,{txt} MEDIUMTEXT,{pop} INTEGER) CHARACTER SET utf8 COLLATE utf8_bin;\n'\
                                        .format(t=xt.tbl_article, id=xt.ID, txt=xt.TEXT, pop=xt.POPULARITY)]
        sql_list += [u'CREATE TABLE {t} ({id} INT NOT NULL AUTO_INCREMENT PRIMARY KEY,{xref} VARCHAR(255) NOT NULL UNIQUE,{pop} INT) CHARACTER SET utf8 COLLATE utf8_bin;\n'\
                                        .format(t=xt.tbl_link, id=xt.ID, xref=xt.XLINK_HREF.replace(':','_'), pop=xt.POPULARITY)]
        sql_list += [u'CREATE TABLE {t} ({id} INT NOT NULL AUTO_INCREMENT PRIMARY KEY,{src} VARCHAR(255) NOT NULL UNIQUE,{cap} TEXT,{pop} INT) CHARACTER SET utf8 COLLATE utf8_bin;\n'\
                                        .format(t=xt.tbl_image, id=xt.ID, src=xt.SRC, cap=xt.CAPTION, pop=xt.POPULARITY)]
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