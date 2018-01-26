import MySQLdb
from xml_tags import XmlTags as xt



class DatabaseAdaptor(object):
    """docstring for DatabaseAdaptor"""    
    def __init__(self, username='wiki_admin', password='!q@w#e4r', host_name='localhost', db_name='wikipedia'):        
        super(DatabaseAdaptor, self).__init__()        
        try:
            self.__db = MySQLdb.connect(host=host_name,
                         user=username,
                         passwd=password,
                         db=db_name,
                         unix_socket = '/var/run/mysqld/mysqld.sock',
                         charset='utf8')
            
            self.__drop_all_tables()
            self.__create_tables()
        except Exception as e:
            raise e
        print 'Database connection to \"{}\" established successfully for \"{}\".'.format(db_name,username+'@'+host_name)                

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
        count = 0L
        for sql_script in sql_script_list:
            try:            
                count += cursor.execute(sql_script)                                
                self.__db.commit()                                
            except Exception as e:
                print '+++++++++++++++++++++++++++++++++++++++++++++++++++++++'
                print sql_script
                raise e                        
        cursor.close()
        return count

    def insert_articles(self, list_attrib_value):
        sql_script_list = []
        for att_val in list_attrib_value:
            sql_script = u'''INSERT INTO {0} ({1}, {3}, {5}) VALUES ({2}, {4}, {6})
                            ON DUPLICATE KEY UPDATE {5} = {5}+{6};'''\
                                            .format('tbl_article',
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
                                            .format('tbl_image',
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
                                            .format('tbl_link',
                                            xt.XLINK_HREF.replace(':','_'), att_val[xt.XLINK_HREF],
                                            xt.POPULARITY, att_val[xt.POPULARITY])
            sql_script_list += [sql_script]
        return self.execute_sql(sql_script_list)
    
    def insert_article_link(self, list_attrib_value):
        sql_script_list = []
        for att_val in list_attrib_value:
            sql_script = u'''INSERT INTO {0} ({1}, {3}, {5}) VALUES ({2}, {4}, {6})
                            ON DUPLICATE KEY UPDATE {5} = {5}+{6};'''\
                                            .format('tbl_article_link',
                                            xt.ARTICLE_ID, att_val[xt.ARTICLE_ID],
                                            xt.LINK_ID, att_val[xt.LINK_ID],
                                            xt.POPULARITY, att_val[xt.POPULARITY])
            sql_script_list += [sql_script]
        return self.execute_sql(sql_script_list)

    def insert_article_image(self, list_attrib_value):
        sql_script_list = []
        for att_val in list_attrib_value:
            sql_script = u'''INSERT INTO {0} ({1}, {3}, {5}) VALUES ({2}, {4}, {6})
                            ON DUPLICATE KEY UPDATE {5} = {5}+{6};'''\
                                            .format('tbl_article_image',
                                            xt.ARTICLE_ID, att_val[xt.ARTICLE_ID],
                                            xt.IMAGE_ID, att_val[xt.IMAGE_ID],
                                            xt.POPULARITY, att_val[xt.POPULARITY])
            sql_script_list += [sql_script]
        return self.execute_sql(sql_script_list)

    def close_connection(self):
        self.__db.close()

    def __create_tables(self):        
        sql = 'SHOW TABLES LIKE \'tbl_article\';'
        c = self.execute_sql([sql])    
        if c != 0L:
            return

        sql_list = []
        sql_list += [u'CREATE TABLE tbl_article({1} INT NOT NULL PRIMARY KEY,{2} MEDIUMTEXT,{0} INTEGER) CHARACTER SET utf8 COLLATE utf8_bin;\n'\
                                        .format(xt.POPULARITY, xt.ID, xt.TEXT)]
        sql_list += [u'CREATE TABLE tbl_link({1} INT NOT NULL AUTO_INCREMENT PRIMARY KEY,{2} VARCHAR(255) NOT NULL UNIQUE,{0} INT) CHARACTER SET utf8 COLLATE utf8_bin;\n'\
                                        .format(xt.POPULARITY, xt.ID, xt.XLINK_HREF.replace(':','_'))]
        sql_list += ['CREATE TABLE tbl_image({1} INT NOT NULL AUTO_INCREMENT PRIMARY KEY,{2} VARCHAR(255) NOT NULL UNIQUE,{3} TEXT,{0} INT) CHARACTER SET utf8 COLLATE utf8_bin;\n'\
                                        .format(xt.POPULARITY, xt.ID, xt.SRC, xt.CAPTION)]
        sql_list += [u'''CREATE TABLE tbl_article_link (article_id INT NOT NULL, link_id INT NOT NULL, {0} INT,                                        
                                        PRIMARY KEY (article_id, link_id),
                                        FOREIGN KEY (article_id) REFERENCES tbl_article ({1}) ON DELETE RESTRICT ON UPDATE CASCADE,
                                        FOREIGN KEY (link_id) REFERENCES tbl_link ({1}) ON DELETE RESTRICT ON UPDATE CASCADE);
                                        \n'''.format(xt.POPULARITY, xt.ID)]
        sql_list += [u'''CREATE TABLE tbl_article_image (article_id INT NOT NULL, image_id INT NOT NULL, {0} INT,
                                        PRIMARY KEY (article_id, image_id),
                                        FOREIGN KEY (article_id) REFERENCES tbl_article ({1}) ON DELETE RESTRICT ON UPDATE CASCADE,
                                        FOREIGN KEY (image_id) REFERENCES tbl_image ({1}) ON DELETE RESTRICT ON UPDATE CASCADE);
                                        \n'''.format(xt.POPULARITY, xt.ID)]        
        
        self.execute_sql(sql_list)