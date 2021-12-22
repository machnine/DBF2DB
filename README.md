# DBF to Databases

Convert dBase tables to SQLite, Microsoft Access and Microsoft SQL server tables

## Example

```
    from Dbf2Db import Dbf2Db
    import sqlite3

    with sqlite3.connect('test.db') as testdb:
        Dbf2Db(dbf_path='.', dbf_filename='test_file.dbf', target_db=testdb).update_target()
        testdb.close()
```
    
