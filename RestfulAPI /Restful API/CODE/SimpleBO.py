import pymysql
import json
import copy
from pymysql.constants.ER import REQUIRES_PRIMARY_KEY

cnx = pymysql.connect(host='localhost',
                              user='dbuser',
                              password='dbuser',
                              db='lahman2017raw',
                              charset='utf8mb4',
                              cursorclass=pymysql.cursors.DictCursor)


def run_q(q, args, fetch=False):
    cursor = cnx.cursor()
    cursor.execute(q, args)
    if fetch:
        result = cursor.fetchall()
    else:
        result = None
    cnx.commit()
    print('Query: ',q)
    return result


        
    
def templateToWhereClause(t):
    s = ""
    if t is None:
        return s
    for (key,value) in t.items():
        if s != "":
            s += " AND "
        s += key + "='" + value[0] + "'"

    if s != "":
        s = "WHERE " + s;

    return s

def template_To_SETClause(t):
    s = ""
    if t is None:
        return s
    for (key,value) in t.items():
        if s != "":
            s += ","
        s += key + "='" + str(value) + "'"

    if s != "":
        s = "SET " + s;

    return s

def template_To_Where_Clause(t):
    s = ""
    if t is None:
        return s
    for (key,value) in t.items():
        if s != "":
            s += "AND "
        s += key + "='" + value + "'"

    if s != "":
        s = "WHERE " + s;

    return s

def find_by_template(table,template,fields=None,limit=None,offset=None):
    WC = templateToWhereClause(template)
    limit_default = str(10)
    offset_default = str(0)
    if limit is None and offset is None:
        if fields is not None:
            q = "select " + fields[0] + " from "+table + " " +WC+" limit "+limit_default+" offset "+offset_default
        else:
            q = "select * from "+table + " " +WC+" limit "+limit_default+" offset "+offset_default
        print('Query= ',q)
        result = run_q(q, None, True)
        return result
    elif limit is not None and offset is not None:
        print(limit,type(limit))
        if fields is not None:
            q = "select " + fields[0] + " from "+table + " " +WC+" limit "+limit[0]+" offset "+offset[0]
        else:
            q = "select * from "+table + " " +WC+" limit "+limit[0]+" offset "+offset[0]
        print('Query= ',q)
        result = run_q(q, None, True)
        return result

def args_to_str(in_args,offset=None):
    f = "?"
    for k,v in in_args.items():
        f += k+"="+v[0]+"&"
    return f[:-1]

def fields_to_str(fields):
    fields_0 = '&fields='
    s = ''
    for i in fields:
        s += i+','
    s = s.strip(',')
    result = fields_0 + s
    return result

def generate_links(resource,result,args,url,url_root,limit=None,offset=None,fields=None):
    Link_lst =[]
    limit_default = 10
    offset_default = 0
    limit_str = "&limit="
    limit_str1 = "?limit="
    offset_str = "&offset="
    limit_value = int(limit[0])
    offset_value = int(offset[0])
    current_link = {"rel":"current","href":url}
    print("current: ",current_link)
    if fields == None:
        if args:
             previous_url = url_root+'api/'+resource+args+limit_str+str(limit_value)+offset_str+str(offset_value-limit_default)
             next_url = url_root+'api/'+resource+args+limit_str+str(limit_value)+offset_str+str(offset_value+limit_default)
             print("p: ",previous_url)
             print("next_:",next_url)
        else:
            previous_url = url_root+'api/'+resource+args+limit_str1+str(limit_value)+offset_str+str(offset_value-limit_default)
            next_url = url_root+'api/'+resource+args+limit_str1+str(limit_value)+offset_str+str(offset_value+limit_default)
            
    elif fields is not None:
        f = {}
        f['fields'] = fields
        f_s = fields_to_str(fields)
        print('f_s:',f_s)
        if args:
            previous_url = url_root+'api/'+resource+args+f_s+limit_str+str(limit_value)+offset_str+str(offset_value-limit_default)
            next_url = url_root+'api/'+resource+args+f_s+limit_str+str(limit_value)+offset_str+str(offset_value+limit_default)
        else:
            f_s = f_s.replace('&','?')
            previous_url = url_root+'api/'+resource+args+f_s+limit_str+str(limit_value)+offset_str+str(offset_value-limit_default)
            next_url = url_root+'api/'+resource+args+f_s+limit_str+str(limit_value)+offset_str+str(offset_value+limit_default)
    if offset == ['0'] and len(result) < 10:
        Link_lst = [current_link]
        return Link_lst
    else:
        if len(result) == 10:
            if int(offset[0]) >= 10:
                previous_link = {"rel":"previous","href":previous_url}
                next_link = {"rel":"next","href":next_url}
                Link_lst = [previous_link,current_link,next_link]
                return Link_lst
            else:
                next_link = {"rel":"next","href":next_url}
                Link_lst = [current_link,next_link]
                return Link_lst
        elif len(result) < 10:
            previous_link = {"rel":"previous","href":previous_url}
            Link_lst = [previous_link,current_link]
            return Link_lst
        
    
    
    
   


def insert(table,s):
    BODY = template_To_SETClause(s)
    q = "INSERT INTO " + table +" "+ BODY
    print(q)
    run_q(q, None, True)
    return 'Inserted successfully'



# UPDATE runoob_tbl SET runoob_title='xxx' WHERE runoob_id=3;
def Updating(table,s,primary_key):
    BODY = template_To_SETClause(s)
    pri = pri_to_dic(table,primary_key)
    WC = templateToWhereClause(pri)
    q = "UPDATE " + table +" "+ BODY + " " + WC
    print("Query: ",q)
    result = run_q(q, None, True)
    print('Updated successful!')
    return result,'Updated successful!'
    

# Deleting rows can be through (find by template, not primary_key)
# DELETE FROM table WHERE Clause

def Delete1(table,primary_key):
    s = pri_to_dic(table,primary_key)
    BODY = templateToWhereClause(s)
    q = "DELETE FROM " + table +" "+ BODY
    run_q(q, None, True)
    return 'Delete successfully'
    
# want to make sure the table's primary key. and find them by template

def pri_to_dic(table,primary_key):
    print('table: ',table)
    print('primary_key: ',primary_key)
    k = primary_key.split('_')
    new = []
    for i in k:
        new.append([i])
    q = "select COLUMN_NAME from (select * from INFORMATION_SCHEMA.KEY_COLUMN_USAGE  \
    WHERE TABLE_NAME = "+"'"+table+"'"+" ) as a \
    where a.CONSTRAINT_SCHEMA = 'lahman2017raw_pk' and CONSTRAINT_NAME = 'PRIMARY'"
    pk_lst=[]
    pk = run_q(q,None,True)
    print("primary_key is ",pk)
    for i in pk:
        pk_lst.append(i.get('COLUMN_NAME'))
    
    
    ### Combine pk and primary_key to a template
    d = dict(zip(pk_lst,new))
    print(d)
    return d
# Because request.args will define the type of value as list, not str. so this function
# So this function was defined in order to transform the normal dict into the same type of request.args
# for example: from {'yearID': '1960', 'teamID': 'BOS'} into {'yearID': ['1960'], 'teamID': ['BOS']}
"""
def pri_to_dic_for_template(table,primary_key):
    k = primary_key.split('_')
    together=[]
    for z in k:
        together.append([z])
    q = "select COLUMN_NAME from (select * from INFORMATION_SCHEMA.KEY_COLUMN_USAGE  \
    WHERE TABLE_NAME = "+"'"+table+"'"+" ) as a \
    where a.CONSTRAINT_SCHEMA = 'lahman2017raw_pk' and CONSTRAINT_NAME = 'PRIMARY'"
    pk_lst=[]
    pk = run_q(q,None,True)
    for i in pk:
        pk_lst.append(i.get('COLUMN_NAME'))
    ### Combine pk and primary_key to a template
    d = dict(zip(pk_lst,together))
    return d
"""
def get_PK(table,primary_key,fields=None,limit=None,offset=None):
    d = pri_to_dic(table,primary_key)
    print('d:',d)
    result = find_by_template(table, d, fields, limit, offset)
    return result

def get_FK(table,primary_key,related_table,template=None,fields=None,limit=None,offset=None):
    if table == 'people' or table == 'teams': #they shouldn't have foreign keys 
        s = dict(copy.copy(pri_to_dic(table,primary_key)))
        print('s: ',s)
        template_new = dict(template,**s)
        print(template_new)
        print('new: ',template_new)
        result = find_by_template(related_table,template_new,fields,limit,offset)
        return result

        
    fk_lst = [] #to receive the foreign keys
    value_lst = [] #in order to record values of each foreign key, and make them to the template
    record_get = get_PK(table, primary_key)
    q1 = "select column_name from (select * from INFORMATION_SCHEMA.KEY_COLUMN_USAGE  \
        WHERE TABLE_NAME =" +"'" + table + "'" +") as a \
        where a.CONSTRAINT_SCHEMA = 'lahman2017raw_pk' and REFERENCED_TABLE_NAME =" + "'" +related_table+ "'"
    print(q1)
    Foreign_K = run_q(q1,None,True)
    for i in Foreign_K:
        fk_lst.append(i.get('COLUMN_NAME'))
    for i in fk_lst:
        value_lst.append([record_get[0].get(i)])
    d = dict(zip(fk_lst,value_lst))
    print('template= ',d)
    result = find_by_template(related_table,d,fields,limit,offset)
    return result
    '''
    wc = template_To_Where_Clause(d)
    if fields is not None:
        q = "select " + fields[0] + " from "+related_table + " " +wc
    else:
        q = "select * from "+related_table + " " +wc
    print(q)
    result = run_q(q, None, True)
    return result
    '''
def career_stats(playerid,limit,offset):
    limit_default = str(10)
    offset_default = str(0)
    wc = "WHERE c.playerID = " + "'"+ playerid +"'"
    q = "select c.playerID,teamID,yearID,g_all,d.hits,d.ABs,d.A,d.E from appearances as c \
        join (select a.playerID,A,E,b.H as hits,b.AB as ABs from \
             (select playerID,A,E from Fielding) as a \
        join batting as b \
            on a.playerID = b.playerID) as d \
            on c.playerID = d.playerID " + wc 
    q += " limit "+limit[0]+" offset "+offset[0]
    print(q)
    result = run_q(q,None,True)
    return result


def roster(s,limit=None,offset=None):
    wc = templateToWhereClause(s)
    q = "select c.playerid, d.nameLast, d.nameFirst,c.teamid,c.yearid,c.G_all,c.H,c.AB from \
        (select distinct a.playerid,a.teamid,a.yearid,a.G_all,b.H,b.AB from \
            (select playerid,teamid,yearid,G_all from appearances "+ wc + " " +" \
                 ) as a \
            join \
            (select playerid,teamid,yearid,H,AB from batting " + wc + " " +"  \
                 ) as b \
        on \
            a.playerid = b.playerid) as c \
     join \
         people as d \
     on d.playerid = c.playerid"
    # result = run_q(q, (teamid,yearid,teamid,yearid), True)
    q += " limit "+limit[0]+" offset "+offset[0]
    print(q)
    result = run_q(q, None, True)
    return result

def find_teammate(playerid,limit,offset):
    q = "select e.playerID,e.teammate,e.first_year,e.last_year,count(*) as times \
from (select c.playerID,c.teammate,c.yearID,c.teamID,d.first_year,d.last_year \
from (select a.playerID,b.playerID as teammate,a.yearID,a.teamID \
from (select * from batting where playerID = " + " '" + playerid +"'" +" ) as a \
join batting as b \
on a.teamID = b.teamID and a.yearID = b.yearID) as c \
join (select playerid,min(yearID) as first_year,max(yearID) as last_year from batting group by playerid) as d \
on c.teammate = d.playerID) as e \
group by e.playerID,e.teammate,e.first_year,e.last_year"
    q += " limit "+limit[0]+" offset "+offset[0]
    print(q)
    result = run_q(q, None, True)
    return result


        

