

import os
from collections import OrderedDict
import sqlite3
import dbf


class Dbf2sqlite:
    
    def __init__(self, dbf_path, dbf_filename, sql_db, ignore_memos=True):     
        
        dbf_file = os.path.join(dbf_path, dbf_filename)
        
        self.__table = dbf.Table(dbf_file, ignore_memos=ignore_memos)
            
        self.__table.open(dbf.DbfStatus.READ_ONLY)        
                
        self.table_name = dbf_filename.lower().replace('.dbf', '')
 
        self.structure = self.__structure()
        
        self.db = sqlite3.connect(sql_db)
        self.cur = self.db.cursor()
        
        self.__update_table()
        
        self.__output()
    
    
    def __structure(self):
        _structure = OrderedDict()
        for field in self.__table.structure():
            name, _type = field.split(' ')
            _type = _type[0]
            if _type == 'N':
                _type = 'REAL'
            else:
                _type = 'TEXT'
            _structure[name.strip()] = _type
        return _structure
    
    def __find_junk(self, e):
        p1 = e.args[2]
        p2 = e.args[3]
        junk = e.args[1][p1:p2]
        return junk
    
    def __drop_table(self):
        query = 'DROP TABLE IF EXISTS {}'.format(self.table_name)
        self.cur.execute(query)
        self.db.commit()
        
        
    def __make_table(self):
        
        fields = ', '.join(['{} {}'.format(k, v) for k,v in self.structure.items()])
        
        query = 'CREATE TABLE IF NOT EXISTS {table} ({fields});'.format(table=self.table_name, fields=fields)
        
        self.cur.execute(query)
        self.db.commit()
    
    def __update_table(self):
        
        self.__drop_table()
        
        self.__make_table()
        
        query = '''INSERT INTO {table} ({fields})
                   VALUES ({values})'''.format(table=self.table_name, 
                                              fields=', '.join(self.__table.field_names),
                                              values=', '.join(['?' for x in range(len(self.__table.field_names))]))
        self.error_list = []
        for record in self.__table:
            try:
                self.cur.execute(query, record)
            except Exception as err:
                self.error_list.append((err, record))
                junk = self.__find_junk(err)
                print(junk, err.args[1])

        self.db.commit()
        
        
    def __output(self):
        n = self.cur.execute('SELECT COUNT(*) FROM {}'.format(self.table_name)).fetchone()[0]
        self.db.close()
        print('{} of {} records imported'.format(n, len(self.__table)))
        
