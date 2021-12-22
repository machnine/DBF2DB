import datetime
from os import path

from dbf import DbfStatus, Table
import pyodbc

# mapping the datatypes of DBF to those of the target DBs
DTYPE_MAP = {
    "access": {"C": "TEXT", "D": "DATE", "M": "TEXT", "N": "NUMBER", "L": "BIT"},
    "sqlite": {"C": "TEXT", "D": "TEXT", "M": "TEXT", "N": "REAL", "L": "TEXT"},
    "sqlserver": {"C": "NVARCHAR", "D": "DATE", "M": "TEXT", "N": "REAL", "L": "BIT"},
}


class Dbf2Db:
    def __init__(
        self,
        dbf_path: str,  # full path to dbf filename
        dbf_filename: str,  # file/table name
        target_db=None,  # target db connection
        target_tablename=None,  # target table name
        append=False,  # append or drop and create new
        ignore_memos=True,  # ignore memo field
        codepage=None,  # codepage (https://en.wikipedia.org/wiki/Code_page)
        quietmode=False,  # don't output to StdIO
    ):

        self.__quietmode = quietmode
        dbf_file = path.join(dbf_path, dbf_filename)
        # open the dbf table as read only
        self.__table = Table(dbf_file, ignore_memos=ignore_memos, codepage=codepage)
        self.__table.open(DbfStatus.READ_ONLY)

        self.__record_count = len(self.__table)
        self.__written_count = 0

        # if target table name is not given use file name
        if target_tablename:
            self.__table_name = target_tablename
        else:
            self.__table_name = dbf_filename.lower().replace(".dbf", "")

        self.__structure = self.__table.structure()
        self.__target_db = target_db
        self.__dbms_name = self.__get_dbms_info()
        self.__dtype_map = DTYPE_MAP[self.__dbms_name]
        self.__append = append

    def update_target(self, new_table_name=None, close_dbf=True):
        """update the target database"""

        if new_table_name:
            self.__table_name = new_table_name

        time_start = datetime.datetime.now()

        if not self.__quietmode:
            print(f"Extracting {self.__record_count} records to target database...")

        self.__update_table(append=self.__append)

        time_finish = datetime.datetime.now()
        time_elapsed = (time_finish - time_start).total_seconds()
        if not self.__quietmode:
            print(
                f"{self.__written_count} records of {self.__record_count} updated successfully in {time_elapsed} seconds."
            )

        if close_dbf:
            # close the link to dbf file
            self.__table.close()
        else:
            print("dbf source table remains open.")

    def close_dbf(self):
        # close the link to dbf file
        self.__table.close()

    @property
    def table_structure(self):
        return self.__structure

    @property
    def dbms_name(self):
        return self.__dbms_name

    @property
    def record_count(self):
        return self.__record_count

    @property
    def written_count(self):
        return self.__written_count

    def __table_def(self, field):
        """extract the name and datatype of each field"""
        field_name, field_def = field.split(" ")
        field_def = field_def.replace(")", "").split("(")

        if len(field_def) == 1 or "," in field_def[1]:
            field_def = field_def[0], ""
            return " ".join([f"[{field_name}]", self.__dtype_map[field_def[0]]])
        else:
            field_def = field_def[0], f"({field_def[1]})"
            return " ".join(
                [f"[{field_name}]", self.__dtype_map[field_def[0]], field_def[1]]
            )

    def __get_dbms_info(self):
        """try to find out what kind of target database is it"""
        info = None
        # databases handled by pypyodbc
        try:
            info = self.__target_db.getinfo(pyodbc.SQL_DBMS_NAME)
            if info.lower() == "access":
                return "access"
            elif info.lower() == "microsoft sql server":
                return "sqlserver"
        # sqlite3 - not handled by pypyodbc
        except:
            try:
                info = self.__target_db.Warning.__dict__["__module__"]
                if info.lower() == "sqlite3":
                    return "sqlite"
            except:
                pass

        return info

    def __update_table(self, append):
        """create and update table with source data"""
        if self.__table_exists():
            if append:
                self.__insert_data()
            else:
                self.__drop_table()
                self.__make_table()
                self.__insert_data()
        else:
            self.__make_table()
            self.__insert_data()

    def __table_exists(self):
        """function to check if table exists"""
        cur = self.__target_db.cursor()

        if self.__dbms_name == "sqlite":
            cur.execute(
                f'SELECT name FROM sqlite_master WHERE type="table" AND name="{self.__table_name}"'
            )
            if len(cur.fetchall()) > 0:
                if not self.__quietmode:
                    print(f"... table [{self.__table_name}] exists")
                return True
            else:
                return False

        elif self.__dbms_name in ["access", "sqlserver"]:
            if len(
                [
                    x
                    for x in cur.tables()
                    if x[2].lower() == self.__table_name and x[3].lower() == "table"
                ]
            ):
                return True
            else:
                return False

    def __drop_table(self):
        """drop a given table_name in db"""
        if not self.__quietmode:
            print(f"... dropping table [{self.__table_name}]")
        self.__target_db.cursor().execute(f"DROP TABLE {self.__table_name}")
        self.__target_db.commit()

    def __make_table_sql(self):
        """assemble a create table sql command"""
        fields = [self.__table_def(x) for x in self.__structure]
        fields = " ,".join(fields)
        sql = f"CREATE TABLE {self.__table_name} ({fields})"
        return sql

    def __make_table(self):
        """make table if not exists"""
        query = self.__make_table_sql()
        self.__target_db.cursor().execute(query)
        self.__target_db.commit()
        if not self.__quietmode:
            print(f"... table [{self.__table_name}] created")

    def __insert_data(self):
        """insert data into table"""
        if not self.__quietmode:
            print(f"... inserting data into [{self.__table_name}]")
        field_size = len(self.__structure)
        values = ", ".join(["?" for x in range(field_size)])
        query = f"insert into {self.__table_name} values ({values})"
        cur = self.__target_db.cursor()
        count = 0
        for record in self.__table:
            try:
                _record = [self.__record_processing(x) for x in record]
                cur.execute(query, _record)
                count += 1
            except Exception as e:
                print(e)
                print(_record)
        self.__target_db.commit()
        self.__written_count = count

    def __record_processing(self, record):
        """clean up the record a bit"""
        # access and sql server doesn't like dates < 1753-01-01
        # https://msdn.microsoft.com/en-us/library/system.data.sqltypes.sqldatetime(v=vs.110).aspx
        # tables with many datetime fields seem to be much slower
        if (
            self.__dbms_name in ["access", "sqlserver"]
            and isinstance(record, (datetime.date, datetime.datetime))
            and record < datetime.date(1753, 1, 1)
        ):
            return None
        # get rid of unneccessary white space
        # this doesn't seem to affect speed much
        elif isinstance(record, str):
            return record.strip()
        else:
            return record
