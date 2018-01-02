import os
from collections import OrderedDict
import sqlite3
import dbf


class Dbf2sqlite:
    
    def __init__(self, dbf_path, dbf_filename, #dbf file path and name
                 sql_db,                       #sqlite db name
                 table_name=None,              #sqlite table name
                 ignore_memos=True,            #ignore memo field
                 append=False):                #append data to existing table   
        
        #open the dbf table as a read only dbf.table object
        dbf_file = os.path.join(dbf_path, dbf_filename)
        self.__table = dbf.Table(dbf_file, ignore_memos=ignore_memos)            
        self.__table.open(dbf.DbfStatus.READ_ONLY)   
        
        #if a table name is given for the sqlite db use it else use the dbf name
        if table_name:
            self.table_name = table_name
        else:
            self.table_name = dbf_filename.lower().replace('.dbf', '')
 
        #work out the structure of the dbf table
        self.structure = self.__structure()
        
        #connect the the sqlite db
        self.db = sqlite3.connect(sql_db)
        self.cur = self.db.cursor()
        
        #in append mode, count how many records already existed
        if append:
            n = self.__existing_record_count(self.table_name)
        else:
            n = 0
        
        #update the sqlite table
        self.__update_table(append=append)
        
        #stdio output to show some numbers of the update
        self.__output(existing_count=n)
    
    
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
        '''find and return the junk bytes causing encoding problems'''
        p1 = e.args[2]
        p2 = e.args[3]
        junk = e.args[1][p1:p2]
        return junk
    
    
    def __drop_table(self):
        query = 'DROP TABLE IF EXISTS {}'.format(self.table_name)
        self.cur.execute(query)
        self.db.commit()
        
        
    def __make_table(self):
        '''make table if not exists'''
        fields = ', '.join(['{} {}'.format(k, v) for k,v in self.structure.items()])
        
        query = 'CREATE TABLE IF NOT EXISTS {table} ({fields});'.format(table=self.table_name, 
                                                                        fields=fields)
        self.cur.execute(query)
        self.db.commit()
        
    
    def __update_table(self, append):
        '''update the existing table or drop then update depending on 'append' param '''
        if append:
            pass
        else:
            self.__drop_table()
        
        self.__make_table()
        
        field_names = self.__table.field_names
        
        query = '''INSERT INTO {table} 
                   VALUES ({values})'''.format(table=self.table_name, 
                                               values=', '.join(['?' for x in range(len(field_names))]))
        #capture the errors
        self.error_list = []
        for record in self.__table:
            try:
                self.cur.execute(query, record)
            except Exception as err:
                self.error_list.append((err, record))
                junk = self.__find_junk(err)
                print('Error {}:'.format(len(self.error_list)), junk, err.args[1])

        self.db.commit()
        
    def __existing_record_count(self, table_name):
        n = self.cur.execute('SELECT COUNT(*) FROM {}'.format(table_name)).fetchone()[0]
        return n

        
        
    def __output(self, existing_count):
        n = self.cur.execute('SELECT COUNT(*) FROM {}'.format(self.table_name)).fetchone()[0]
        self.db.close()
        print('{} of {} records imported. Total = {}'.format(n - existing_count, 
                                                             len(self.__table), 
                                                             n))  
        
