import MySQLdb
from xml_tags import XmlTags as xt



class DatabaseAdaptor(object):
    """docstring for DatabaseAdaptor"""    
    def __init__(self, username='wiki_admin', password='!q@w#e4r', host_name='localhost', db_name='wikipedia'):        
        super(DatabaseAdaptor, self).__init__()        
        try:
            self.__db = MySQLdb.connect(host=host_name,    # your host, usually localhost
                         user=username,         # your username
                         passwd=password,  # your password
                         db=db_name,
                         unix_socket = '/var/run/mysqld/mysqld.sock')        # name of the data base
        except Exception as e:
            raise e
        
        self.create_tables()
        
    def __insert(self, sql_script_list):        
        cursor = self.__db.cursor()
        id_list = []
        for sql_script in sql_script_list:
            try:            
                cursor.execute(sql_script)
                self.__db.commit()
                id_list += [cursor.lastrowid()]
            except Exception as e:
                raise e                        
        cursor.close()
        return id_list

    def insert_images(self, list_attrib_value):
        sql_script_list = []
        for att_val in list_attrib_value:
            sql_script = '''INSERT INTO {0} ({1}, {3}, {5}) VALUES ({2}, {4}, {6})
                            ON DUPLICATE KEY UPDATE {5} = {5}+{6}'''\
                                            .format('tbl_image',
                                            xt.SRC, att_val[xt.SRC],
                                            xt.CAPTION, att_val[xt.CAPTION],
                                            xt.POPULARITY, att_val[xt.POPULARITY])
            sql_script_list += [sql_script]
        return self.__insert(sql_script_list)


    def insert_links(self, list_attrib_value):        
        sql_script_list = []
        for att_val in list_attrib_value:
            sql_script = '''INSERT INTO {0} ({1}, {3}) VALUES ({2}, {4})
                            ON DUPLICATE KEY UPDATE {3} = {3}+{4}'''\
                                            .format('tbl_link',
                                            xt.XLINK_HREF.replace(':','_'), att_val[xt.XLINK_HREF],
                                            xt.POPULARITY, att_val[xt.POPULARITY])
            sql_script_list += [sql_script]
        return self.__insert(sql_script_list)

    def insert_articles(self, list_attrib_value):
        sql_script_list = []
        for att_val in list_attrib_value:
            sql_script = '''INSERT INTO {0} ({1}, {3}, {5}) VALUES ({2}, {4}, {6})
                            ON DUPLICATE KEY UPDATE {5} = {5}+{6}'''\
                                            .format('tbl_article',
                                            xt.ID, att_val[xt.ID],
                                            xt.TEXT, att_val[xt.TEXT],
                                            xt.POPULARITY, att_val[xt.POPULARITY])
            sql_script_list += [sql_script]
        return self.__insert(sql_script_list)

    def insert_article_link(self, list_attrib_value):
        sql_script_list = []
        for att_val in list_attrib_value:
            sql_script = '''INSERT INTO {0} ({1}, {3}, {5}) VALUES ({2}, {4}, {6})
                            ON DUPLICATE KEY UPDATE {5} = {5}+{6}'''\
                                            .format('tbl_article_link',
                                            xt.ARTICLE_ID, att_val[xt.ARTICLE_ID],
                                            xt.LINK_ID, att_val[xt.LINK_ID],
                                            xt.POPULARITY, att_val[xt.POPULARITY])
            sql_script_list += [sql_script]
        return self.__insert(sql_script_list)

    def insert_article_image(self, list_attrib_value):
        sql_script_list = []
        for att_val in list_attrib_value:
            sql_script = '''INSERT INTO {0} ({1}, {3}, {5}) VALUES ({2}, {4}, {6})
                            ON DUPLICATE KEY UPDATE {5} = {5}+{6}'''\
                                            .format('tbl_article_image',
                                            xt.ARTICLE_ID, att_val[xt.ARTICLE_ID],
                                            xt.IMAGE_ID, att_val[xt.IMAGE_ID],
                                            xt.POPULARITY, att_val[xt.POPULARITY])
            sql_script_list += [sql_script]
        return self.__insert(sql_script_list)

    def close_connection(self):
        self.__db.close()

    def create_tables(self):        
        try:
            cursor = self.__db.cursor()
            o = cursor.execute('SHOW TABLES LIKE \'tbl_article\';')
            self.__db.commit()                                    
        except Exception as e:
            raise e                                
        finally:
            cursor.close()
        
        if o != 0:            
            return

        
        sql_list = []
        sql_list += ['CREATE TABLE tbl_article({1} VARCHAR(10) NOT NULL,{2} TEXT,{0} INT,PRIMARY KEY ({1}));\n'\
                                        .format(xt.POPULARITY, xt.ID, xt.TEXT)]
        sql_list += ['CREATE TABLE tbl_link({1} INT NOT NULL AUTO_INCREMENT,{2} VARCHAR(255) NOT NULL,{0} INT,PRIMARY KEY ({1}));\n'\
                                        .format(xt.POPULARITY, xt.ID, xt.XLINK_HREF.replace(':','_'))]
        sql_list += ['CREATE TABLE tbl_image({1} INT NOT NULL AUTO_INCREMENT,{2} VARCHAR(255) NOT NULL,{3} TEXT,{0} INT,PRIMARY KEY ({1}));\n'\
                                        .format(xt.POPULARITY, xt.ID, xt.SRC, xt.CAPTION)]
        sql_list += ['''CREATE TABLE tbl_article_link (article_id VARCHAR(10) NOT NULL, link_id INT NOT NULL, {0} INT,
                                        PRIMARY KEY (article_id, link_id),
                                        FOREIGN KEY (article_id) REFERENCES tbl_article ({1}) ON DELETE RESTRICT ON UPDATE CASCADE,
                                        FOREIGN KEY (link_id) REFERENCES tbl_link ({1}) ON DELETE RESTRICT ON UPDATE CASCADE);
                                        \n'''.format(xt.POPULARITY, xt.ID)]
        sql_list += ['''CREATE TABLE tbl_article_image (article_id VARCHAR(10) NOT NULL, image_id INT NOT NULL, {0} INT,
                                        PRIMARY KEY (article_id, image_id),
                                        FOREIGN KEY (article_id) REFERENCES tbl_article ({1}) ON DELETE RESTRICT ON UPDATE CASCADE,
                                        FOREIGN KEY (image_id) REFERENCES tbl_image ({1}) ON DELETE RESTRICT ON UPDATE CASCADE);
                                        \n'''.format(xt.POPULARITY, xt.ID)]        
        cursor = self.__db.cursor()
        for sql in sql_list:            
            try:
                cursor.execute(sql)
                self.__db.commit()            
            except Exception as e:
                raise e                        
        cursor.close()