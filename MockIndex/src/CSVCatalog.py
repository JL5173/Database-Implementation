import pymysql
import pymysql.cursors
import csv
import logging
import json
from src import DataTableExceptions
temp = {"definition":{},"columns":[],"indexes":{}}
def get_header(path):
    file_path = path  #'/Users/apple/Desktop/HW3/src/Data/People.csv'
    f = open(file_path)
    lines = [l for l in f]
    header = lines[0]
    s = header.split(",")
    return s
people_header = get_header('/Users/apple/Desktop/HW3/src/Data/People.csv')
batting_header = get_header('/Users/apple/Desktop/HW3/src/Data/Batting.csv')
teams_header = get_header('/Users/apple/Desktop/HW3/src/Data/Teams.csv')

class ColumnDefinition:
    """
    Represents a column definition in the CSV Catalog.
    """

    # Allowed types for a column.
    column_types = ("text", "number")
    
    def __init__(self, column_name, column_type="text", not_null=False):
        """

        :param column_name: Cannot be None.
        :param column_type: Must be one of valid column_types.
        :param not_null: True or False
        """
        self.column_name = column_name
        self.column_type = column_type
        self.not_null = not_null
        self.tmp = {"column_name":column_name,
                     "column_type" : column_type,
                     "not_null" : not_null}
    
        
        

    def __str__(self):
        pass

    def to_json(self):
        """
    
        :return: A JSON object, not a string, representing the column and it's properties.
        """
        a = {}
        a["column_name"] = self.column_name
        a["column_type"] = self.column_type
        a["not_null"] = self.not_null
        c = json.dumps(a,indent=2)
        print(c)
        return c


class IndexDefinition:
    """
    Represents the definition of an index.
    """
    index_types = ("PRIMARY", "UNIQUE", "INDEX")

    def __init__(self, index_name, index_type):
        """

        :param index_name: Name for index. Must be unique name for table.
        :param index_type: Valid index type.
        """
        self.index_name = index_name
        self.index_type = index_type
        self.tmp = {"index_name":index_name,
                     "column_type" : index_name}
         
        

class TableDefinition:
    """
    Represents the definition of a table in the CSVCatalog.
    """

    def __init__(self, t_name=None, csv_f=None, column_definitions=None, index_definitions=None, cnx=None):
        """

        :param t_name: Name of the table.
        :param csv_f: Full path to a CSV file holding the data.
        :param column_definitions: List of column definitions to use from file. Cannot contain invalid column name.
            May be just a subset of the columns.
        :param index_definitions: List of index definitions. Column names must be valid.
        :param cnx: Database connection to use. If None, create a default connection.
        """
        self.t_name = t_name
        self.csv_f = csv_f
        if column_definitions:
            self.column_definitions = [col.tmp for col in column_definitions]
        if index_definitions:
            self.index_definitions = index_definitions
        if cnx:
            self.cnx = cnx
        self.prep = {"definition":{},"columns":[],"indexes":{}}
        

    def __str__(self):
        pass

    @classmethod
    def load_table_definition(cls,cnx,table_name):
        """

        :param cnx: Connection to use to load definition.
        :param table_name: Name of table to load.
        :return: Table and all sub-data. Read from the database tables holding catalog information.
        """
        
        show={}
        index_show = {}
        b = CSVCatalog()
        q = 'SELECT * FROM definitions WHERE name = ' + '"' +table_name+'"'
        q1 = 'SELECT * FROM columns WHERE name = ' + '"' +table_name+'"'
        q2 = 'SELECT * FROM indexes WHERE name = ' + '"' +table_name+'"'
        result = b._query(q,fetch=True)
        result1 = b._query(q1,fetch=True)
        result2 = b._query(q2,fetch=True)
        
        columns_definitions=[]
        
        if result:
            show['definition'] = result[0]
        else:
            show['definition'] = {}
        if result1:
            for i in result1:
                del i['name']
                columns_definitions.append(i)
            show['columns'] = columns_definitions
        else:
            show['columns'] = []
        
        if result2:
            show['indexes']={}
            for i in result2:
                s = {}
                k = []
                s["index_name"] = i["index_name"]
                for j in i["columns"].split(","):
                    k.append(j)
                #s["columns"] = i["columns"]
                s["columns"] = k
                s['kind'] = i['kind']
                index_show[i['index_name']] = s
                show['indexes'] = dict(show['indexes'],**index_show)
            #show['indexes'] = result2[0]
        else:
            show['indexes'] = {}
        #print('prep: ',show)
        return show
        
        

    def add_column_definition(self, c):
        """
        Add a column definition.
        :param c: New column. Cannot be duplicate or column not in the file.
        :return: None
        """
        #self.prep["columns"].append(c.tmp)
        b = CSVCatalog()
        #c = ColumnDefinition(self.t_name)
        q = 'insert into columns values ( ' + '"' +self.t_name+ '"' +', '+ '"' + c.column_name + '"' +', '+'"' +c.column_type+ '"' +', '+'"' +str(c.not_null)+'"' +' )'
        
        b._query(q, False)
        

    def drop_column_definition(self, c):
        """
        Remove from definition and catalog tables.
        :param c: Column name (string)
        :return:
        """
        b = CSVCatalog()
        q = 'DELETE FROM columns WHERE name = ' + '"' +self.t_name+'"' +' And column_name='+'"' +c.column_name+'"'
        #print("q:",q)
        b._query(q, False)
        

    def to_json(self):
        """
        I don't need this function here
        :return: A JSON representation of the table and it's elements.
        """
        return json.dumps(temp,indent=2)
        
    def examine_primary_key(self,columns):
        defined_columns = []
        for i in self.column_definitions:
            defined_columns.append(i['column_name'])
        #print("table:",header_list)
        for i in columns:
            if i not in defined_columns:
                raise DataTableExceptions.DataTableException(code=-1000,message="Invalid key columns")
            
    def define_primary_key(self, columns):
        """
        Define (or replace) primary key definition.
        :param columns: List of column values in order.
        :return:
        """
        
        
        self.examine_primary_key(columns)
        
        index_name = 'PRIMARY'
        kind = 'PRIMARY'
        b = CSVCatalog()
        s=""
        for i in columns:
            s += i +","
        s = s[0:-1]
        q = 'insert into indexes values ( ' + '"' +self.t_name+ '"' +', '+ '"' + index_name + '"' +', '+'"' +s+ '"' +', '+'"' +kind+'"' +' )'
        b._query(q)
         
        
        

    def define_index(self, index_name, columns, kind="index"):
        """
        Define or replace and index definition.
        :param index_name: Index name, must be unique within a table.
        :param columns: Valid list of columns.
        :param kind: One of the valid index types.
        :return:
        
        
        """
        b = CSVCatalog()
        s=""
        for i in columns:
            s += i +","
        s = s[0:-1]
        q = 'insert into indexes values ( ' + '"' +self.t_name+ '"' +', '+ '"' + index_name + '"' +', '+'"' +s+ '"' +', '+'"' +kind+'"' +' )'
        b._query(q)
        

    def drop_index(self, index_name):
        """
        Remove an index.
        :param index_name: Name of index to remove.
        :return:
        """
        a = CSVCatalog()
        q = 'DELETE FROM indexes WHERE name = ' + '"' +self.t_name+'"' +' And index_name='+'"' +index_name+'"'
        a._query(q, False)
        

    def describe_table(self):
        """
        Simply wraps to_json()
        :return: JSON representation.
        """
        s = CSVCatalog()
        #print("table:",self.t_name)
        a = self.load_table_definition(s.cnx, self.t_name)
        
        return a
    

        


class CSVCatalog:

    def __init__(self, dbhost="localhost",dbname="CSVCatalog1", dbuser="dbuser", dbpw="dbuser"):
        self.cnx = pymysql.connect(host=dbhost,
                             user=dbuser,
                             password=dbpw,
                             db=dbname,
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)


    def __str__(self):
        ''' I want to show the table in the test results'''
    def _query(self,q,fetch = False):
        cnx = self.cnx
        cursor=cnx.cursor()
        #print ("Query = ", q)
        cursor.execute(q);
        if fetch:
            result = cursor.fetchall()
        else:
            result = None
        cnx.commit()
        return result
    def examine_column_name(self,table_name,column_name):
        if table_name == 'people':
            if column_name not in people_header:
                raise DataTableExceptions.DataTableException(code=-100,message="Column " + column_name +" definition is invalid ")
            if table_name == 'batting':
                if column_name not in batting_header:
                    raise DataTableExceptions.DataTableException(code=-100,message="Column " + column_name +" definition is invalid ")
            if table_name == 'teams':
                if column_name not in batting_header:
                    raise DataTableExceptions.DataTableException(code=-100,message="Column " + column_name +" definition is invalid ")

    
    def create_table(self, table_name, file_name, column_definitions=None, primary_key_columns=None):
        
        # Determine copy table
        a = TableDefinition(table_name,file_name,column_definitions,primary_key_columns)
        previous_info = a.load_table_definition(self.cnx,table_name)
        if previous_info['definition'] != {}:
            if previous_info['definition']['name'] == table_name:
                raise DataTableExceptions.DataTableException(code=-101,message="Table name is duplicate")
        
        q = 'insert into definitions values ( ' + '"' +table_name+ '"' +', '+ '"' + file_name + '"' +' )'
        if column_definitions:
            for i in column_definitions:
                self.examine_column_name(table_name, i.column_name)
                q1 = 'insert into columns values ( ' + '"' +table_name+ '"' +', '+ '"' + i.column_name + '"' +', '+'"' +i.column_type+ '"' +', '+'"' +str(i.not_null)+'"' +' )'
                #print("q1:",q1)
                self._query(q1)
            #self.run_q(q,None,self.cnx)
        self._query(q)
        
            #raise DataTableExceptions.DataTableException(code=-101,message="Table name is duplicate")
        
        return a
        

    def drop_table(self, table_name):
        """
        if temp['definition'] is not None:
            if temp['definition']['name'] == table_name:
                temp = {"definition":{},"columns":[],"indexes":{}} # it seems like an empty table
        """
        # DELETE FROM table_name [WHERE Clause]
        a = TableDefinition(table_name)
        q = 'DELETE FROM definitions WHERE name = ' + '"' +table_name+'"'
        q1 = 'DELETE FROM columns WHERE name = ' + '"' +table_name+'"'
        q2 = 'DELETE FROM indexes WHERE name = ' + '"' +table_name+'"'
        self._query(q)
        self._query(q1)
        self._query(q2)
        return a 

    def get_table(self, table_name):
        """
        Returns a previously created table.
        :param table_name: Name of the table.
        :return:
        """
        a = TableDefinition(table_name)
        #q = 'SELECT * FROM definitions WHERE name = ' + '"' +table_name+'"'
        s = a.load_table_definition(self.cnx,table_name)
        #self._query(q)
        #print("s:",s)
        return a
        
        
        
        














