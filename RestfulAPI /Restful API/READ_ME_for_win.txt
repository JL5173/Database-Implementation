Introduction: 
Conditions: lahman2017raw.sql (6 tables), python, workbench, postman
Notice: I add primary key and foreign key with alter table clicking originally.
        For convenience of your examination, I also wrote queries in CONSTRAIN.sql
1.After observation and analysis, I found that all tables except people and teams have ¡®playerID¡¯,¡¯yearID¡¯ and ¡®teamID¡¯, and ¡®playerID¡¯ is unique in people, combination of ¡®yearID¡¯ and 
¡®teamID¡¯ is unique in teams. Also, in those 4 tables( batting,appearances,managers,fielding), we could find responding people and teams in each record. Therefore, I gave each of those
4 tables Foreign keys, which can help us to find who they are or which team they are from in each record.

2. For convenience of your examination, I derivate six tables (people,batting,appearances,fielding,managers,teams),which are tables that you want us to modify.
Their primary keys and Foreign keys are (in order):
People: ¡®playerID¡¯       no foreign keys
Batting: ¡®playerID¡¯, ¡®yearID¡¯, ¡®stint¡¯, ¡®teamID¡¯   battingtopeople:¡¯playerID¡¯,¡¯battingtoteams¡¯:¡¯yearID¡¯,¡¯teamID¡¯
Appearances: ¡®yearID¡¯,¡¯teamID¡¯,¡¯playerID¡¯     ¡®apptopeople¡¯: ¡®playerID¡¯,¡¯apptoteams¡¯:¡¯yearID¡¯,¡¯teamID¡¯
Fielding: ¡®playerID¡¯,¡¯yearID¡¯,¡¯stint¡¯, ¡®teamID¡¯, ¡®POS¡¯,¡¯InnOuts¡¯, ¡®PO¡¯, ¡¯E¡¯  ¡®fieldingtopeople¡¯:¡¯playerID¡¯,¡¯fieldingtoteams¡¯:¡¯yearID¡¯,¡¯teamID¡¯
Teams: ¡®yearID¡¯, ¡®teamID¡¯ no foreign keys
Managers:¡¯playerID¡¯,¡¯yearID¡¯,¡¯teamID¡¯,¡¯inseason¡¯ ¡®managertopeople¡¯:¡¯playerID¡¯, ¡®managertoteams¡¯:¡¯yearID¡¯,¡¯teamID¡¯

3.Design Ideas:
a) For Path:
Base Resource: GET: we get parameters from request.args and transfer them to a dictionary. Then we can get results through query combined with function templateToWhereClause.
 			 POST: we give a new record in the ¡®body¡¯ through postman, and our function can use json.load() to get new record and transfer them in to dictionary that we can deal with
Specific Resources: GET: we get primary keys from specific table first by query. And I matched values with primary keys and combine them into dictionaries. Then use find_by_template
			       UPDATE: I give an expecting record into ¡®body¡¯, and my function(Updating) can get modify the record
			       Delete: From my perspective, I think you mean that you want to delete some specific records found by its primary key. In this case, I don¡¯t need to delete records through ¡®body¡¯. 
			       For example, I will delete this record: api/people/jl5173.
Dependent Resources: GET: Because people and teams don¡¯t have foreign keys, I divided this function into two different situations:
						1>.people(or teams)/primary_key/batting(appearances¡­.): Since we can get primary keys and values from them through path first, and add them into template from
						    related tables, and use new template(dictionary) to get results through find_by_template
						2>batting(¡­.)/primary_key/people(or teams): we can get record through ¡¯batting(¡­.)/primary_key¡¯, but program only can return one result. And then we use 
						    foreign keys(battingtopeople) as primary key to get information in the related tables. So in this situation, we can only find one record.
				   POST: Insert a new record in the related table.
(primary_key and foreign key queries:
PK:"select COLUMN_NAME from (select * from INFORMATION_SCHEMA.KEY_COLUMN_USAGE  \
    WHERE TABLE_NAME = "+"'"+table+"'"+" ) as a \
    where a.CONSTRAINT_SCHEMA = 'lahman2017raw_pk' and CONSTRAINT_NAME = 'PRIMARY'"
FK:"select column_name from (select * from INFORMATION_SCHEMA.KEY_COLUMN_USAGE  \
        WHERE TABLE_NAME =" +"'" + table + "'" +") as a \
        where a.CONSTRAINT_SCHEMA = 'lahman2017raw_pk' and REFERENCED_TABLE_NAME =" + "'" +related_table+ "'"
b) Customer queries:
I designed three queries and only hold one or two variables in each query. And run their query with parameters. My original queries are:
For roster:
"select c.playerid, d.nameLast, d.nameFirst,c.teamid,c.yearid,c.G_all,c.H,c.AB from \
        (select distinct a.playerid,a.teamid,a.yearid,a.G_all,b.H,b.AB from \
            (select playerid,teamid,yearid,G_all from appearances "+ WhereClause + " " +" \
                 ) as a \
            join \
            (select playerid,teamid,yearid,H,AB from batting " + WhereClause + " " +"  \
                 ) as b \
        on \
            a.playerid = b.playerid) as c \
     join \
         people as d \
     on d.playerid = c.playerid"
For career_stats:
"select c.playerID,teamID,yearID,g_all,d.hits,d.ABs,d.A,d.E from appearances as c \
        join (select a.playerID,A,E,b.H as hits,b.AB as ABs from \
             (select playerID,A,E from Fielding) as a \
        join batting as b \
            on a.playerID = b.playerID) as d \
            on c.playerID = d.playerID " + WhereClause
For teammates:
"select e.playerID,e.teammate,e.first_year,e.last_year,count(*) as times \
from (select c.playerID,c.teammate,c.yearID,c.teamID,d.first_year,d.last_year \
from (select a.playerID,b.playerID as teammate,a.yearID,a.teamID \
from (select * from batting where playerID = " + " '" + playerid +"'" +" ) as a \
join batting as b \
on a.teamID = b.teamID and a.yearID = b.yearID) as c \
join (select playerid,min(yearID) as first_year,max(yearID) as last_year from batting group by playerid) as d \
on c.teammate = d.playerID) as e \
group by e.playerID,e.teammate,e.first_year,e.last_year"

d) For Links:
I designed three possible links: previous, current, next. If len(result) is not more than 10(I set that ten records per page) there is only one link: current
Besides, I designed a function ( generate_links) to finish this mission. My original thinking is to transfer them into a string with a lot of determines,i,e, api/people?¡­.&limit=10 & offset=variables
In this case, for convenience, I default ¡®&limit=10&offset=xxx¡¯ or ¡®?limit=10&offset=xxxx¡¯ (no [?queries][&fields], we should begin with ¡®?¡¯) as the end of url.





For my assignment, I provide you with 3 python files: SimpBO.py, Flask.py, customer.py.
I run Path and Customer Queries separately 


For SimpleBO:

Introduction: I put all basic functions into this file, which can make codes in Flask.py more clear

Functions:
a). Basic functions( including 
def run_q(q, args, fetch=False):
I used this function to connect my database, and run the queries to get what we want.

def templateToWhereClause(t):
Transfer the dictionaries collected from request.args into WhereClause to queries
Notice: this function for request.args(i.e{¡®playerID¡¯:[¡®willite01¡¯]}), so if we want the value, we should use ¡®value[0]¡¯

def template_To_SETClause(t):
Transfer the dictionaries collected from request.args into SETClause to queries
In ¡®POST¡¯,¡¯UPDATE¡¯ operations, we should use ¡¯SET¡¯ key words to execute queries.

def template_To_Where_Clause(t):
Transfer the dictionaries collected from ¡®body¡¯ in postman into whereclause to queries.
Notice: this is different between with first function. Because the dictionaries we get from ¡®body¡¯ is {¡®playerID¡¯:¡¯willite01¡¯},so if we want the value,

def find_by_template(table,template,fields=None,limit=None,offset=None):
This function can get results by parameters collected from i.e.(¡®/people?playerID=willite&¡­.)

def args_to_str(in_args,offset=None):
Transfer ¡®in_args¡¯ into string type.

def fields_to_str(fields):
Transfer fields into to string ( for generate links)

def generate_links(resource,result,args,url,url_root,limit=None,offset=None,fields=None):
Generate links with ¡®previous¡¯(if exists), ¡®next¡¯(if exists), ¡®current¡¯

def insert(table,s):
Insert new record

def Updating(table,s,primary_key):
Modify records

def Delete1(table,primary_key):
Delete records with primary_key in specific resources

def pri_to_dic(table,primary_key):
Get primary_key from each table with query, and transfer them into dictionary

def get_PK(table,primary_key,fields=None,limit=None,offset=None):
Use ¡®pri_to_dic¡¯ to transfer PK to dictionary and use ¡®find_by_template¡¯ to get result

def get_FK(table,primary_key,related_table,template=None,fields=None,limit=None,offset=None):
Get Foreign keys and transfer them to dictionary in order to get result by ¡®find_by_template¡¯
Notice: if some tables don¡¯t have FK ( I.e.people/<primary_key>/batting), I just derive their primary key into a template and add this template into related resource¡¯s template
Then use the new template to get result

def career_stats(playerid,limit,offset):
Customer Queries: search for records of one player.

def roster(s,limit=None,offset=None):
Given teamID,yearID, we can get results about roster in each year about each team

def find_teammate(playerid,limit,offset):
Given a playerid, we can find their teammates and data about ¡®first year¡¯ and ¡®last year¡¯ about his teammates. And also can calculate times when they were teammates.

http://35.231.116.243:5000/?token=3314543794666d575c8071d2ccc0f36194fd73313bd5c284


Flask.py:

Introduction: This file can get parameters from url, and we can use them with functions in SimpleBO to create resources and get results.


Customer.py:

Introduction: For convenience: I put three missions about queries into a new python file.