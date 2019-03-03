
from src import CSVCatalog
from src import CSVTable

import time
import json

data_dir = "/Users/apple/Desktop/HW3/src/Data/"

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

def __format_odict(l):
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

def print_out(result):
    
    final=""
    header = list(result[0].keys())
    print(__format_odict(header))
    for i in range(len(result)):
        temp_r = list(result[i].values())
        final += __format_odict(temp_r) + "\n"
    print(final)
    
def print_out2(result):
    
    final=""
    header = list(result[0].keys())
    print(__format_odict(header))
    for i in range(len(result)):
        temp_r = list(result[i].values())
        final += __format_odict(temp_r) + "\n"
    print(final)

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


def test_join_optimizable_2(optimize=False):
    """
    Calling this with optimize=True turns on optimizations in the JOIN code.
    :return:
    """
    cleanup()
    print_test_separator("Starting test_optimizable_2, optimize = " + str(optimize))

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
    t.define_index("pid_idx", ['playerID'], "INDEX")

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

    join_result = people_tbl.join(batting_tbl,['playerID'], None, optimize=optimize)

    end_time = time.time()

    print("Result = \n", json.dumps(join_result,indent=2))
    elapsed_time = end_time - start_time
    print("\n\nElapsed time = ", elapsed_time)

    print_test_separator("Complete test_join_optimizable_2")
    


def test_join_optimizable_3(optimize=False):
    """

    :return:
    """
    cleanup()
    print_test_separator("Starting test_optimizable_3, optimize = " + str(optimize))

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
    t.define_index("pid_idx", ['playerID'], "INDEX")
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
    t.define_index("pid_idx", ['playerID'], "INDEX")

    people_tbl = CSVTable.CSVTable("people")
    batting_tbl = CSVTable.CSVTable("batting")

    start_time = time.time()

    tmp = {"playerID": "willite01"}
    join_result = people_tbl.join(batting_tbl,['playerID'], tmp, optimize=optimize)

    end_time = time.time()

    print("Result = \n", json.dumps(join_result,indent=2))
    print(print_out(join_result))
    elapsed_time = end_time - start_time
    print("\n\nElapsed time = ", elapsed_time)

    print_test_separator("Complete test_join_optimizable_3")
    
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

    print("Loaded people table = \n", people_tbl)
    print("Loaded batting table = \n", batting_tbl)

    start_time = time.time()

    tmp = {"playerID": "willite01"}
    join_result = people_tbl.join(batting_tbl,['playerID'], tmp, optimize=optimize)

    end_time = time.time()

    print(print_out(join_result))
    elapsed_time = end_time - start_time
    print("\n\nElapsed time = ", elapsed_time)

    print_test_separator("Complete test_join_optimizable_3_t")
    

#check()
#test_join_not_optimized(optimize=False)
#test_join_optimizable_2(optimize=True)
#test_join_optimizable_2_standard(optimize=True)
#test_join_optimizable_3(optimize=True)
test_join_optimizable_3_t(optimize=True)
#test_join_optimizable_2_teacher(optimize=True)

