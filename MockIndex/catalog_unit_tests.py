from src import CSVCatalog

import time
import json
def test():
    b = []
    #a =  CSVCatalog.ColumnDefinition("playerID", "text", True)
    b.append(CSVCatalog.ColumnDefinition("playerID", "text", True))
    b.append(CSVCatalog.ColumnDefinition("playerID", "number", True))
    #CSVCatalog.ColumnDefinition("playerID", "text", True).to_json()
    #print(type(b[0])) 
    return b

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
def test1():
    a = CSVCatalog.TableDefinition()
    a.define_primary_key(['playerID', 'teamID', 'yearID', 'HR'])

def test2():
    a = CSVCatalog.TableDefinition()
    a.define_primary_key(['playerID', 'teamID', 'yearID', 'HR'])
    a.define_index("team_year_idx",['teamID', 'yearID'],"INDEX")
#create table
def test3():
    print_test_separator("Starting test_create_table_1")
    cat = CSVCatalog.CSVCatalog()
    t = cat.create_table(
        "people",
        "/Users/Jinyang_Li/Dropbox/ColumbiaCourse/Courses/Fall2018/W4111/Data/People.csv")
def test4_drop():
    cat = CSVCatalog.CSVCatalog()
    t = cat.get_table("people")
    #print("t: ",t.describe_table())
    print("Initial status of table = \n", json.dumps(t.describe_table_test(), indent=2))
    t = cat.drop_table('people')
    print("Initial status of table = \n", json.dumps(t.describe_table_test(), indent=2))
def test_create_table_1():
    cleanup()
    print_test_separator("Starting test_create_table_1")
    cat = CSVCatalog.CSVCatalog()
    t = cat.create_table(
        "people",
        "/Users/apple/Desktop/HW3/src/Data/People.csv")
    print("People table", json.dumps(t.describe_table()))
    print_test_separator("Complete test_create_table_1")
    #t = cat.get_table("people")
    
def test_create_table_2_fail(): #failed
    print_test_separator("Starting test_create_table_2_fail")
    cleanup()
    cat = CSVCatalog.CSVCatalog()
    t = cat.create_table("people",
     "/Users/donaldferguson/Dropbox/ColumbiaCourse/Courses/Fall2018/W4111-Projects/CSVDB/Data/core/People.csv")

    try:
        t = cat.create_table("people",
             "/Users/donaldferguson/Dropbox/ColumbiaCourse/Courses/Fall2018/W4111-Projects/CSVDB/Data/core/People.csv")
    except Exception as e:
        print("Second created failed with e = ", e)
        print("Second create should fail.")
        print_test_separator("Successful end for  test_create_table_2_fail")
        

def test_create_table_3():
    """
    Creates a table that includes several column definitions.
    :return:
    """
    print_test_separator("Starting test_create_table_3")
    cleanup()
    cat = CSVCatalog.CSVCatalog()

    cds = []
    cds.append(CSVCatalog.ColumnDefinition("playerID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("nameLast", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("nameFirst", column_type="text"))

    t = cat.create_table("people",
     "/Users/donaldferguson/Dropbox/ColumbiaCourse/Courses/Fall2018/W4111-Projects/CSVDB/Data/core/People.csv",
                     cds)
    print("People table", json.dumps(t.describe_table(), indent=2))
    print_test_separator("Complete test_create_table_3")
    
def test_create_table_3_fail():
    """
    Creates a table that includes several column definitions. This test should fail because one of the defined
    columns is not in the underlying CSV file.
    :return:
    """
    print_test_separator("Starting test_create_table_3_fail")
    cleanup()
    cat = CSVCatalog.CSVCatalog()

    cds = []
    cds.append(CSVCatalog.ColumnDefinition("playerID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("nameLast", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("nameFirst", column_type="text"))
    cds.append(CSVCatalog.ColumnDefinition("canary"))

    try:
        t = cat.create_table("people",
            "/Users/donaldferguson/Dropbox/ColumbiaCourse/Courses/Fall2018/W4111-Projects/CSVDB/Data/core/People.csv",
                     cds)
        print_test_separator("FAILURE test_create_table_3")
        print("People table", json.dumps(t.describe_table(), indent=2))
    except Exception as e:
        print("Exception e = ", e)
        print_test_separator("Complete test_create_table_3_fail successfully")
        

    
def test_create_table_4():
    """
        Creates a table that includes several column definitions.
        :return:
        """
    print_test_separator("Starting test_create_table_4")
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
                         "/Users/donaldferguson/Dropbox/ColumbiaCourse/Courses/Fall2018/W4111-Projects/CSVDB/Data/core/Batting.csv",
                         cds)

    t.define_primary_key(['playerID', 'teamID', 'yearID', 'stint'])
    print("People table", json.dumps(t.describe_table(), indent=2))
    print_test_separator("Complete test_create_table_4")
    
def test_create_table_4_fail():
    """
    Creates a table that includes several column definitions and a primary key.
    The primary key references an undefined column, which is an error.

    NOTE: You should check for other errors. You do not need to check in the CSV file for uniqueness but
    should test other possible failures.
    :return:
    """
    print_test_separator("Starting test_create_table_4_fail")
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
                         "/Users/donaldferguson/Dropbox/ColumbiaCourse/Courses/Fall2018/W4111-Projects/CSVDB/Data/core/Batting.csv",
                         cds)
    try:
        t.define_primary_key(['playerID', 'teamID', 'yearID', 'HR'])
        print("Batting table", json.dumps(t.describe_table(), indent=2))
        print_test_separator("FAILURES test_create_table_4_fail")
    except Exception as e:
        print("Exception e = ", e)
        print_test_separator("SUCCESS test_create_table_4_fail should fail.")



def test_create_table_5_prep():
    """
    Creates a table that includes several column definitions and a primary key.
    :return:
    """
    print_test_separator("Starting test_create_table_5_prep")
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
                         "/Users/donaldferguson/Dropbox/ColumbiaCourse/Courses/Fall2018/W4111-Projects/CSVDB/Data/core/Batting.csv",
                         cds)

    t.define_primary_key(['playerID', 'teamID', 'yearID', 'stint'])
    print("Batting table", json.dumps(t.describe_table(), indent=2))

    print_test_separator("Completed test_create_table_5_prep")
    


def test_create_table_5():
    """
    Modifies a preexisting/precreated table definition.
    :return:
    """
    print_test_separator("Starting test_create_table_5")

    # DO NOT CALL CLEANUP. Want to access preexisting table.
    cat = CSVCatalog.CSVCatalog()
    t = cat.get_table("batting")
    print("Initial status of table = \n", json.dumps(t.describe_table(), indent=2))
    t.add_column_definition(CSVCatalog.ColumnDefinition("HR","number"))
    t.add_column_definition(CSVCatalog.ColumnDefinition("G","number"))
    t.define_index("team_year_idx",['teamID', 'yearID'],"INDEX")
    print("Modified status of table = \n", json.dumps(t.describe_table(), indent=2))
    print_test_separator("Success test_create_table_5")
def test_drop_column_definition():
    print_test_separator("Starting test_drop_column_definition_5")

    # DO NOT CALL CLEANUP. Want to access preexisting table.
    cat = CSVCatalog.CSVCatalog()
    t = cat.get_table("batting")
    print("Initial status of table = \n", json.dumps(t.describe_table(), indent=2))
    
    t.drop_column_(CSVCatalog.ColumnDefinition("G","number"))
    print("Modified status of table = \n", json.dumps(t.describe_table(), indent=2))
    print_test_separator("Success test_drop_column_definition_5")

def test_drop_index():
    print_test_separator("Starting test_drop_index_5")

    # DO NOT CALL CLEANUP. Want to access preexisting table.
    cat = CSVCatalog.CSVCatalog()
    t = cat.get_table("batting")
    print("Initial status of table = \n", json.dumps(t.describe_table(), indent=2))
    
    t.drop_column_(CSVCatalog.ColumnDefinition("G","number"))
    print("Modified status of table = \n", json.dumps(t.describe_table(), indent=2))
    print_test_separator("Success test_drop_index_5")
    

        
def testt():
    s = []
    a = CSVCatalog.get_header_people()
    print(a)
    f = a.replace("bbrefID\n","bbrefID")
    f = f.split(",")
    print(f)

def header():
    people = CSVCatalog.get_header('/Users/apple/Desktop/HW3/src/Data/People.csv')
    batting = CSVCatalog.get_header('/Users/apple/Desktop/HW3/src/Data/Batting.csv')
    Teams = CSVCatalog.get_header('/Users/apple/Desktop/HW3/src/Data/Teams.csv')
    print(people)
    print(batting)
    print(Teams)
#testt()
#header()
#test()
#test1()
#test2()
#test3()
#test4_drop()
#test_create_table_1() #success
#test_create_table_2_fail() #success
#test_create_table_3() #success
#test_create_table_3_fail() #success
#test_create_table_4() #success
#test_create_table_4_fail() #success
#test_create_table_5_prep() #success
#test_create_table_5()  #success
test_drop_column_definition()

