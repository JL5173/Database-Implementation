from src import CSVCatalog
from src import CSVTable
import csv
import time

import time
import json
data_dir = "/Users/apple/Desktop/HW3/src/Data/"

#from blaze.compute.tests.test_sparksql import people


def cleanup():
    """
    Deletes previously created information to enable re-running tests.
    :return: None
    """
    cat = CSVCatalog.CSVCatalog()
    cat.drop_table("people")
    cat.drop_table("batting")
    cat.drop_table("teams")

def print_test_separator(msg):
    print("\n")
    lot_of_stars = 20*'*'
    print(lot_of_stars, '  ', msg, '  ', lot_of_stars)
    print("\n")

def __format_odict(l): #For printing tables
    """
    Use to help print a row from the tale.
    :param l: An ordered dictionary representing a row in the table.
    :return: A string with the row printed in equally spaced columns
    """
    result = ""
    temp = list(l)
    template = "{:<" + str(15) + "}"
    for i in range(0, len(temp)):
        result += template.format(temp[i])
    return result

def print_out(result,limit=None):#For printing tables
    
    final=""
    header = list(result[0].keys())
    if limit is None:
        print(__format_odict(header))
        for i in range(len(result)):
            temp_r = list(result[i].values())
            final += __format_odict(temp_r) + "\n"
    else:
        print(__format_odict(header))
        for i in range(limit):
            temp_r = list(result[i].values())
            final += __format_odict(temp_r) + "\n"
        
    print(final)



    
def define_tables():
    cleanup()
    cat = CSVCatalog.CSVCatalog()
    
    cds = []
    cds.append(CSVCatalog.ColumnDefinition("playerID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("nameLast", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("nameFirst", column_type="text"))
    cds.append(CSVCatalog.ColumnDefinition("birthCity", column_type="text"))
    cds.append(CSVCatalog.ColumnDefinition("birthCountry", column_type="text"))
    cds.append(CSVCatalog.ColumnDefinition("throws", column_type="text"))

    
    #DDL:
    t = cat.create_table("people",
                         '/Users/apple/Desktop/HW3/src/Data/People.csv',cds)
    
    cds = []
    cds.append(CSVCatalog.ColumnDefinition("playerID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("H", column_type="number", not_null=True))
    cds.append(CSVCatalog.ColumnDefinition("AB", column_type="number"))
    cds.append(CSVCatalog.ColumnDefinition("teamID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("yearID", column_type="text", not_null=True))
    cds.append(CSVCatalog.ColumnDefinition("stint", column_type="number", not_null=True))
    
    t = cat.create_table("batting",
                         '/Users/apple/Desktop/HW3/src/Data/Batting.csv',cds)

def test_add_index_batting():
    cat = CSVCatalog.CSVCatalog()
    people_def = cat.get_table("batting")
    people_def.define_index("team_year_idx",['teamID','yearID'])
    people_def.define_index("team_H_idx",['teamID','H'])

def test_describe():
    cat = CSVCatalog.CSVCatalog()
    people_t = cat.get_table("people")
    desc = people_t.describe_table()
    print("describe people = \n",json.dumps(desc,indent=2))

   
def test_add_index_batting():
    cat = CSVCatalog.CSVCatalog()
    people_def = cat.get_table("batting")
    people_def.define_index("team_year_idx",['teamID','yearID'])
    people_def.define_index("team_H_idx",['teamID','H'])    

def test_add_index_people():
    cat = CSVCatalog.CSVCatalog()
    people_def = cat.get_table("people")
    people_def.define_index("ln_idx",['nameLast'],'INDEX')

def test_find_by_template_index(tries):
    '''I just tested that with __find_by_template_index__() directly '''
    print_test_separator("Starting test_find_by_template_index")
    people_tbl = CSVTable.CSVTable("people")
    start_time = time.time()
    for i in range(0,tries):
        template = {"nameLast":"Williams","birthCity":"San Diego"}
        result = people_tbl.__find_by_template_index__(template,'ln_idx',fields=["playerID","nameLast","birthCity"])
        if i == 0:
            print("Testing result. Result = \n", json.dumps(result, indent=2))
            print_out(result)
    end_time = time.time()
    print("\nElapsed time to execute", tries, "queries = ", end_time-start_time)
    print_test_separator("Finishing test_find_by_template_index")
      
def test_find_by_template_scan_people(tries):
    print_test_separator("Starting test_find_by_template_scan")
    people_tbl = CSVTable.CSVTable("people")
    #print("rows:",people_tbl.__rows__)
    template = {"nameLast":"Williams","birthCity":"San Diego"}
    start_time = time.time()
    for i in range(0,tries):
        result = people_tbl.__find_by_template_scan__(template,fields=["playerID","nameLast","birthCity"])
        if i == 0:
            print("Testing result. Result = \n", json.dumps(result, indent=2))
            print_out(result)
    end_time = time.time()
    print("\nElapsed time to execute", tries, "queries = ", end_time-start_time)
    print_test_separator("Finishing test_find_by_template_scan")
    
def test_find_by_template(tries):
    print_test_separator("Starting test_find_by_template")
    people_tbl = CSVTable.CSVTable("people")
    #print("rows:",people_tbl.__rows__)
    start_time = time.time()
    for i in range(0,tries):
        template = {"nameLast":"Williams","birthCity":"San Diego"}
        result = people_tbl.find_by_template(template,fields=["playerID","nameLast","birthCity"])
        if i == 0:
            print("Testing result. Result = \n", json.dumps(result, indent=2))
            print_out(result)
    end_time = time.time()
    print("\nElapsed time to execute", tries, "queries = ", end_time-start_time)
    print_test_separator("Finishing test_find_by_template")

def test_find_by_template_scan_batting():
    '''run this function without running test_add_index_batting'''
    print_test_separator("Starting test_find_by_template_scan_batting")
    cleanup()

    cat = CSVCatalog.CSVCatalog()

    cds = []
    cds.append(CSVCatalog.ColumnDefinition("playerID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("teamID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("yearID", column_type="text", not_null=True))
    cds.append(CSVCatalog.ColumnDefinition("stint", column_type="number", not_null=True))
    cds.append(CSVCatalog.ColumnDefinition("H", column_type="number", not_null=False))
    cds.append(CSVCatalog.ColumnDefinition("AB", column_type="number", not_null=False))

    t = cat.create_table("batting",
                         '/Users/apple/Desktop/HW3/src/Data/Batting.csv',
                         cds)
    
    batting_tbl = CSVTable.CSVTable("batting")

    start_time = time.time()

    tmp = {'playerID':'willite01','yearID':'1939','stint': '1','teamID':'BOS'}
    fields = ['playerID','yearID','stint','teamID']

    result = batting_tbl.find_by_template(tmp,fields)

    end_time = time.time()
    print("Result = \n",json.dumps(result,indent=2))
    print_out(result)
    elapsed_time = end_time - start_time
    print("\nElapsed time to execute queries = ",elapsed_time)
    print_test_separator("Finishing test_find_by_template_scan_batting")

def test_find_by_template_index_batting():
    print_test_separator("Starting test_find_by_template_index_batting")

    cleanup()

    cat = CSVCatalog.CSVCatalog()

    cds = []
    cds.append(CSVCatalog.ColumnDefinition("playerID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("teamID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("yearID", column_type="text", not_null=True))
    cds.append(CSVCatalog.ColumnDefinition("stint", column_type="number", not_null=True))
    cds.append(CSVCatalog.ColumnDefinition("H", column_type="number", not_null=False))
    cds.append(CSVCatalog.ColumnDefinition("AB", column_type="number", not_null=False))

    t = cat.create_table("batting",
                         '/Users/apple/Desktop/HW3/src/Data/Batting.csv',
                         cds)
    t.define_index("team_year_idx", ['teamID', 'yearID'], "INDEX")
    batting_tbl = CSVTable.CSVTable("batting")

    start_time = time.time()

    tmp = {'playerID':'willite01','yearID':'1939','stint': '1','teamID':'BOS'}
    fields = ['playerID','yearID','stint','teamID']

    result = batting_tbl.find_by_template(tmp,fields)
    print("Result = \n",json.dumps(result,indent=2))
    print("table:\n")
    print_out(result)
    end_time = time.time()

    elapsed_time = end_time - start_time
    print("\nElapsed time to execute queries = ",elapsed_time)
    print_test_separator("Starting test_find_by_template_index_batting")
    
def test_join_not_optimized(optimize=False):
    """

    :return:
    """

    print_test_separator("Starting test_optimizable_1, optimize = " + str(optimize))
    print("\n\nDude. This takes 30 minutes. Trust me.\n\n")
    return

    cleanup()
    print_test_separator("Starting test_optimizable_1, optimize = " + str(optimize))

    cat = CSVCatalog.CSVCatalog()
    cds = []

    cds = []
    cds.append(CSVCatalog.ColumnDefinition("playerID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("nameLast", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("nameFirst", column_type="text"))
    cds.append(CSVCatalog.ColumnDefinition("birthCity", "text"))
    cds.append(CSVCatalog.ColumnDefinition("birthCountry", "text"))
    cds.append(CSVCatalog.ColumnDefinition("throws", column_type="text"))

    t = cat.create_table(
        "people",
        data_dir + "People.csv",
        cds)
    print("People table metadata = \n", json.dumps(t.describe_table(), indent=2))

    cds = []
    cds.append(CSVCatalog.ColumnDefinition("playerID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("H", "number", True))
    cds.append(CSVCatalog.ColumnDefinition("AB", column_type="number"))
    cds.append(CSVCatalog.ColumnDefinition("teamID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("yearID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("stint", column_type="number", not_null=True))

    t = cat.create_table(
        "batting",
        data_dir + "Batting.csv",
        cds)
    print("Batting table metadata = \n", json.dumps(t.describe_table(), indent=2))

    people_tbl = CSVTable.CSVTable("people")
    batting_tbl = CSVTable.CSVTable("batting")

    start_time = time.time()

    tmp = { "playerID": "willite01"}
    join_result = people_tbl.join(batting_tbl,['playerID'], tmp, optimize=optimize)

    end_time = time.time()

    print("Result = \n", json.dumps(join_result,indent=2))
    elapsed_time = end_time - start_time
    print("\n\nElapsed time = ", elapsed_time)

    print_test_separator("Complete test_join_optimizable")

def test_join_optimizable_2_op(optimize=False):
    '''
    I used limit=15 here, because I just want to show the first 15 records and the whole table is so big
    '''
    
    cleanup()
    print_test_separator("Starting test_join_optimizable_2_op, optimize = " + str(optimize))

    cat = CSVCatalog.CSVCatalog()
    cds = []

    cds = []
    cds.append(CSVCatalog.ColumnDefinition("playerID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("nameLast", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("nameFirst", column_type="text"))
    cds.append(CSVCatalog.ColumnDefinition("birthCity", "text"))
    cds.append(CSVCatalog.ColumnDefinition("birthCountry", "text"))
    cds.append(CSVCatalog.ColumnDefinition("throws", column_type="text"))

    t = cat.create_table(
        "people",
        data_dir + "People.csv",
        cds)
    
    print("People table metadata = \n", json.dumps(t.describe_table(), indent=2))
    t.define_index("pid_idx", ['playerID'],"INDEX")
    cds = []
    cds.append(CSVCatalog.ColumnDefinition("playerID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("H", "number", True))
    cds.append(CSVCatalog.ColumnDefinition("AB", column_type="number"))
    cds.append(CSVCatalog.ColumnDefinition("teamID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("yearID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("stint", column_type="number", not_null=True))

    t = cat.create_table(
        "batting",
        data_dir + "Batting.csv",
        cds)
    print("Batting table metadata = \n", json.dumps(t.describe_table(), indent=2))
    t.define_index("pid_idx", ['playerID'],"INDEX")

    people_tbl = CSVTable.CSVTable("people")
    batting_tbl = CSVTable.CSVTable("batting")


    start_time = time.time()

    tmp = {"playerID": "willite01"}
    join_result = people_tbl.join(batting_tbl,['playerID'], None, optimize=optimize)

    end_time = time.time()
    
    print('table = \n')
    print_out(join_result,limit=15)
    elapsed_time = end_time - start_time
    print("\n\nElapsed time = ", elapsed_time)

    print_test_separator("Complete test_join_optimizable_2_op")
    
def test_join_optimizable_3_t(optimize=False):
    """

    :return:
    """
    cleanup()
    print_test_separator("Starting test_optimizable_3_t, optimize = " + str(optimize))

    cat = CSVCatalog.CSVCatalog()
    cds = []

    cds = []
    cds.append(CSVCatalog.ColumnDefinition("playerID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("nameLast", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("nameFirst", column_type="text"))
    cds.append(CSVCatalog.ColumnDefinition("birthCity", "text"))
    cds.append(CSVCatalog.ColumnDefinition("birthCountry", "text"))
    cds.append(CSVCatalog.ColumnDefinition("throws", column_type="text"))

    t = cat.create_table(
        "people",
        data_dir + "People.csv",
        cds)
    
    print("People table metadata = \n", json.dumps(t.describe_table(), indent=2))
    t.define_index("pid_idx", ['playerID'],"INDEX")
    cds = []
    cds.append(CSVCatalog.ColumnDefinition("playerID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("H", "number", True))
    cds.append(CSVCatalog.ColumnDefinition("AB", column_type="number"))
    cds.append(CSVCatalog.ColumnDefinition("teamID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("yearID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("stint", column_type="number", not_null=True))

    t = cat.create_table(
        "batting",
        data_dir + "Batting.csv",
        cds)
    print("Batting table metadata = \n", json.dumps(t.describe_table(), indent=2))
    t.define_index("pid_idx", ['playerID'],"INDEX")

    people_tbl = CSVTable.CSVTable("people")
    batting_tbl = CSVTable.CSVTable("batting")



    start_time = time.time()

    tmp = {"playerID": "willite01"}
    join_result = people_tbl.join(batting_tbl,['playerID'], tmp, optimize=optimize)

    end_time = time.time()
    
    print('table = \n')
    print_out(join_result)
    elapsed_time = end_time - start_time
    print("\n\nElapsed time = ", elapsed_time)

    print_test_separator("Complete test_join_optimizable_3_t")
    

#define_tables()
#test_add_index_batting()
#test_add_index_people()
#test_describe()
#test_find_by_template_index(1000)
#test_find_by_template_scan_people(1000)
#test_find_by_template(1000)
#test_find_by_template_scan_batting()
#test_find_by_template_index_batting()
#test_join_not_optimized()
#test_join_optimizable_2_op(optimize=True)
test_join_optimizable_3_t(optimize=True)


