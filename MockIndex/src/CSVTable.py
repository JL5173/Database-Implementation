import csv  # Python package for reading and writing CSV files.

# You MAY have to modify to match your project's structure.
from src import DataTableExceptions
from src import CSVCatalog



import json





class CSVTable:
    # Table engine needs to load table definition information.
    __catalog__ = CSVCatalog.CSVCatalog()
    max_rows_to_print = 10

    def __init__(self, t_name, load=True):
        """
        Constructor.
        :param t_name: Name for table.
        :param load: Load data from a CSV file. If load=False, this is a derived table and engine will
            add rows instead of loading from file.
        """

        self.__table_name__ = t_name
        self.index = {}
        self.headers = None

        # Holds loaded metadata from the catalog. You have to implement  the called methods below.
        self.__description__ = {}
        if load:
            self.__load_info__()  # Load metadata
            self.__rows__ = []
            self.__load__()  # Load rows from the CSV file.

            # Build indexes defined in the metadata. We do not implement insert(), update() or delete().
            # So we can build indexes on load.
            self.__build_indexes__()
        else:
            self.__file_name__ = "DERIVED"

    def __load_info__(self):
        """
        Loads metadata from catalog and sets __description__ to hold the information.
        :return:
        """
        cat = self.__catalog__.get_table(self.__table_name__)
        self.__description__ = cat.describe_table()
        return self.__description__
    def __get_file_name__(self):
        path = self.__description__['definition']['path']
        #print("path:",path)
        return path
    
    def __get_columns_names__(self):
        a = self.__load_info__()
        #print("info:",a)
        column_selected = []
        #print("info:",a)
        for i in self.__description__['columns']:
            column_selected.append(i['column_name'])
        return column_selected
    def __add_row__(self,project_r):
        self.__rows__.append(project_r)
        

    # Load from a file and creates the table and data.
    def __load__(self):

        try:
            fn = self.__get_file_name__()
            with open(fn, "r") as csvfile:
                # CSV files can be pretty complex. You can tell from all of the options on the various readers.
                # The two params here indicate that "," separates columns and anything in between " ... " should parse
                # as a single string, even if it has things like "," in it.
                reader = csv.DictReader(csvfile, delimiter=",", quotechar='"')

                # Get the names of the columns defined for this table from the metadata.
                column_names = self.__get_columns_names__()
                

                # Loop through each line (well dictionary) in the input file.
                for r in reader:
                    # Only add the defined columns into the in-memory table. The CSV file may contain columns
                    # that are not relevant to the definition.
                    self.headers = list(r.keys())
                    projected_r = self.project([r], column_names)[0]
                   # print("r:",projected_r)
                    self.__add_row__(projected_r)

        except IOError as e:
            raise DataTableExceptions.DataTableException(
                code=DataTableExceptions.DataTableException.invalid_file,
                message="Could not read file = " + fn)

    
    
    def get_index_items(self):
        a = self.__description__
        #print("a:",a)
        index_items={}
        for (k,v) in a['indexes'].items():
            #print('k:',k)
            if v['kind'] == 'INDEX':
                index_items[k] = v['columns']
                
        return index_items

    def __build_indexes__(self):
        """
        This function creates indexes based on the indexes defined on the tables.
        """
        ROW = []
        #index={}
        index_items=self.get_index_items()
        s = 0
        getting={}
        for r in self.__rows__:
            for (k,v) in index_items.items():
                values = []
                for i in v:
                    values.append(r[i])
                    Key = '_'.join(values)
                if Key not in list(getting.keys()):
                    getting[Key] = [s]
                else:
                    getting[Key]+=[s]
                s += 1
        
                self.index[k] = getting
        return self.index
    
    
    def get_index_selectivity(self):
        best = {}
        for (key,value) in self.index.items():
            best[key] = len(self.__rows__)/len(value)
        return best

    def __get_access_path__(self, tmp):
        """
        Returns best index matching the set of keys in the template.

        Best is defined as the most selective index, i.e. the one with the most distinct index entries.

        An index name is of the form "colname1_colname2_coluname3" The index matches if the
        template references the columns in the index name. The template may have additional columns, but must contain
        all of the columns in the index definition.
        :param tmp: Query template.
        :return: Index or None
        """
        
        lst=[]
        b = []
        s = []
        if self.__description__['indexes']:
            index_items = self.get_index_items()
            best = self.get_index_selectivity()
            for (key,value) in index_items.items():
                if set(value).issubset(set(tmp)):
                    lst.append(key)
            
            if best:
                for (k,v) in best.items():
                    b.append(k)
                    s.append(v)
                return b[s.index(max(s))]
            else:
                return None
        
    

    def matches_template(self, row, t):
        """

        :param row: A single dictionary representing a row in the table.
        :param t: A template
        :return: True if the row matches the template.
        """

        # Basically, this means there is no where clause.
        if t is None:
            return True

        try:
            c_names = list(t.keys())
            for n in c_names:
                if row[n] != t[n]:
                    return False
            else:
                return True
        except Exception as e:
            raise (e)

    def project(self, rows, fields):
        """
        Perform the project. Returns a new table with only the requested columns.
        :param fields: A list of column names.
        :return: A new table derived from this table by PROJECT on the specified column names.
        """
        
        try:
            if fields is None:  # If there is not project clause, return the base table
                return rows  # Should really return a new, identical table but am lazy.
            else:
                result = []
                for r in rows:  # For every row in the table.
                    tmp = {}  # Not sure why I am using range.
                    for j in range(0, len(fields)):  # Make a new row with just the requested columns/fields.
                        v = r[fields[j]]
                        tmp[fields[j]] = v
                    else:
                        result.append(tmp)  # Insert into new table when done.
                
                return result

        except KeyError as ke:
            # happens if the requested field not in rows.
            raise DataTableExceptions.DataTableException(-2, "Invalid field in project")
    

    def __find_by_template_scan__(self, t, fields=None, limit=None, offset=None):
        """
        Returns a new, derived table containing rows that match the template and the requested fields if any.
        Returns all row if template is None and all columns if fields is None.
        :param t: The template representing a select predicate.
        :param fields: The list of fields (project fields)
        :param limit: Max to return. Not implemented
        :param offset: Offset into the result. Not implemented.
        :return: New table containing the result of the select and project.
        """
        if limit is not None or offset is not None:
            raise DataTableExceptions.DataTableException(-101, "Limit/offset not supported for CSVTable")

        # If there are rows and the template is not None
        if self.__rows__ is not None:

            result = []

            # Add the rows that match the template to the newly created table.
            for r in self.__rows__:
                if self.matches_template(r, t):
                    result.append(r)

            result = self.project(result, fields)
        else:
            result = None

        return result


        
    def __find_by_template_index__(self, t, idx, fields=None, limit=None, offset=None):
        """
        Find using a selected index
        :param t: Template representing a where clause/
        :param idx: Name of index to use.
        :param fields: Fields to return.
        :param limit: Not implemented. Ignore.
        :param offset: Not implemented. Ignore
        :return: Matching tuples.
        """
        if self.__rows__ is not None:

            result = []
            K_values = []
            
            index_items = self.get_index_items()
            #tmp_not = list(set(t).difference(set(index_items[idx])))
            #print("index_items:",index_items)
            #print("t:,",t)
            #print("idx:",idx)
            for i in index_items[idx]:
                K_values.append(t[i])
                #t.pop(i)

            values = '_'.join(K_values)
            for i in self.index[idx][values]:
                if self.matches_template(self.__rows__[i],t):
                    result.append(self.__rows__[i])

            result = self.project(result, fields)
        else:
            result = None
        return result
        
                
                

    def find_by_template(self, t, fields=None, limit=None, offset=None):
        # 1. Validate the template values relative to the defined columns.
        # 2. Determine if there is an applicable index, and call __find_by_template_index__ if one exists.
        # 3. Call __find_by_template_scan__ if not applicable index.
        access_index = None
        if t is not None:
            if self.__get_access_path__(list(t.keys())):
                access_index= self.__get_access_path__(list(t.keys()))
        else:
            access_index = None
        
        if access_index is None:
            result = self.__find_by_template_scan__(t, fields=fields, limit=None, offset=None)
        else:
            result = self.__find_by_template_index__(t, access_index, fields, limit, offset)
        #print("index:",result)
        return result
    def __get_on_template__(self,rows,on_fields):
        on_Clause={}
        if type(rows) == list:
            for i in rows:
                on_Clause[on_fields[0]] = i[on_fields[0]]
            #on_Clause={s:l_r[s] for s in on_fields}
            return on_Clause
        elif type(rows) == dict:
            on_Clause[on_fields[0]] = rows[on_fields[0]]
            return on_Clause
            
    
    def join(self, right_r, on_fields, where_template=None, project_fields=None, optimize=False):
        """
        Implements a JOIN on two CSV Tables. Support equi-join only on a list of common
        columns names.
        :param left_r: The left table, or first input table
        :param right_r: The right table, or second input table.
        :param on_fields: A list of common fields used for the equi-join.
        :param where_template: Select template to apply to the result to determine what to return.
        :param project_fields: List of fields to return from the result.
        :return: List of dictionary elements, each representing a row.
        """
        if optimize:
            l_first = self.find_by_template(where_template)
            r_first = right_r.find_by_template(where_template)

            #right_r.__rows__ = r_first
            final_rows=[]
            if self.__get_access_path__(on_fields):
                for current_right_rows in r_first:
                    on_template = {f:current_right_rows[f] for f in on_fields}
                    #on_template = self.__get_on_template__(current_right_rows, on_fields)
                    #print("on_template:",on_template)
                    l_r = self.find_by_template(on_template)
                    if l_r:
                        for r in l_r:
                            current_right_rows.update(r)
                            final_rows.append(current_right_rows)
            
            elif right_r.__get_access_path__(on_fields):
                for l_r in l_rows:
                    on_template = {f:l_r[f] for f in on_fields}
                    r_r = self.find_by_template(on_template)
                    if r_r:
                        for r in r_r:
                            l_r.update(r)
                            final_result.append(l_r)
            

            return final_rows
        else:
            return self.nested_loop_join(right_r, on_fields, where_template, project_fields) 

    def nested_loop_join(self, right_r, on_fields, where_template=None, project_fields=None):
        """
        Implements a JOIN on two CSV Tables. Support equi-join only on a list of common
        columns names.
        :param left_r: The left table, or first input table
        :param right_r: The right table, or second input table.
        :param on_fields: A list of common fields used for the equi-join.
        :param where_template: Select template to apply to the result to determine what to return.
        :param project_fields: List of fields to return from the result.
        :return: List of dictionary elements, each representing a row.
        """

        # If not optimizations are possible, do a simple nested loop join and then apply where_clause and
        # project clause to result.
        #
        # At least two vastly different optimizations are be possible. You should figure out two different optimizations
        # and implement them.
        scan_rows = self.__rows__
        join_result = []
        
        for l_r in scan_rows:
            on_template = self.__get_on_template__(l_r, on_fields)
            current_right_rows = right_r.find_by_template(on_template)
            
            if current_right_rows is not None and len(current_right_rows) > 0:
                #Merge
                for i in current_right_rows:
                    l_r.update(i)
                    join_result.extend(l_r)
                #new_rows = self.__join_rows__([l_r],current_right_rows,on_fields)
                #join_result.extend(new_rows)
                
        final_rows=[]
        for r in join_result:
            if self.matches_template(r, where_template):
                r = self.project([r], fields=project_fields)
                final_rows.append(r[0])
        return final_rows















