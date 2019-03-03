#from social_graph import fan_comment
#from utils import utils as ut

import fan_comment_template as fct
#import utils.utils as ut
import utils as ut


import json
import py2neo
import pymysql

cnx = pymysql.connect(host='localhost',
                             user='dbuser',
                             password='dbuser',
                             db='lahman2017raw_pk',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

fg = fct.FanGraph(auth=('neo4j','Ljy250318'),
                              host="localhost",
                              port=7687,
                              secure=False)

ut.set_debug_mode(True)
def print_test_separator(msg):
    print("\n")
    lot_of_stars = 20*'*'
    print(lot_of_stars, '  ', msg, '  ', lot_of_stars)
    print("\n")

def load_players():
    print_test_separator("Starting test_load_players")

    q = "SELECT playerID, nameLast, nameFirst FROM People where  " + \
        "exists (select * from appearances where appearances.playerID = people.playerID and yearID >= 2017)"

    curs = cnx.cursor()
    curs.execute(q)

    r = curs.fetchone()
    cnt = 0
    while r is not None:
        print(r)
        cnt += 1
        r = curs.fetchone()
        if r is not None:
            p = fg.create_player(player_id=r['playerID'], last_name=r['nameLast'], first_name=r['nameFirst'])
            print("Created player = ", p)

    print("Loaded ", cnt, "records.")
    print_test_separator("Finishing test_load_players")

def test_load_teams():
    print_test_separator("Starting test_load_teams")

    q = "SELECT teamid, name from teams where yearid >= 2017"

    curs = cnx.cursor()
    curs.execute(q)

    r = curs.fetchone()
    cnt = 0
    while r is not None:
        print(r)
        cnt += 1
        r = curs.fetchone()
        if r is not None:
            p = fg.create_team(team_id=r['teamid'], team_name=r['name'])
            print("Created team = ", p)

    print("Loaded ", cnt, "records.")
    
    print_test_separator("Finishing test_load_teams")


def load_appearances():
    
    print_test_separator("Starting load_appearances")
    q = "SELECT distinct playerid, teamid, g_all as games from appearances where yearid >= 2017"

    curs = cnx.cursor()
    curs.execute(q)

    r = curs.fetchone()
    cnt = 0
    while r is not None:
        print(r)
        cnt += 1
        r = curs.fetchone()
        if r is not None:
            try:
                p = fg.create_appearance(team_id=r['teamid'], player_id=r['playerid'])
                print("Created appearances = ", p)
            except Exception as e:
                print("Could not create.")

    print("Loaded ", cnt, "records.")
    print_test_separator("Finishing load_appearances")


def load_follows_fans():
    print_test_separator("Starting load_follows_fans")
    fg.create_fan(uni="js1", last_name="Smith", first_name="John")
    fg.create_fan(uni="ja1", last_name="Adams", first_name="John")
    fg.create_fan(uni="tj1", last_name="Jefferson", first_name="Thomas")
    fg.create_fan(uni="gw1", last_name="Washing", first_name="George")
    fg.create_fan(uni="jm1", last_name="Monroe", first_name="James")
    fg.create_fan(uni="al1", last_name="Lincoln", first_name="Abraham")

    fg.create_follows(follower="gw1", followed="js1")
    fg.create_follows(follower="tj1", followed="gw1")
    fg.create_follows(follower="ja1", followed="gw1")
    fg.create_follows(follower="jm1", followed="gw1")
    fg.create_follows(follower="tj1", followed="gw1")
    fg.create_follows(follower="al1", followed="jm1")
    print('Load fans Successfully')
    
    print_test_separator("Finishing load_follows_fans")


def create_supports():
    print_test_separator("Starting create_supports")
    
    fg.create_supports("gw1", "WAS")
    fg.create_supports("ja1", "BOS")
    fg.create_supports("tj1", "WAS")
    fg.create_supports("jm1", "NYA")
    fg.create_supports("al1", "CHA")
    fg.create_supports("al1", "CHN")
    
    print('Created supports successful')
    
    
    print_test_separator("Starting create_supports")


#load_players()
#test_load_teams()
#load_appearances()
#load_follows_fans()
#create_supports()


def test_create_comment():
    
    t = fg.get_team('BOS')
    f = fg.get_fan('al1')
    p = fg.get_player('pedrodu01')
    c = "Best Player!"
    pid = p['player_id']
    tid = t['team_id']
    fid = f['uni']
    c = fg.create_comment(fid, c, tid, pid)
    print('c:',c)

#test_create_comment()

def test_create_comment_fail():
    print_test_separator("Starting test_create_comment_fail")
    try:
        t = fg.get_team('BOS')
        f = fg.get_fan('jm1')
        p = fg.get_player('cashnan01')
        pid = p['player_id']
        tid = t['team_id']
        fid = f['uni']
        c = ""
        c = fg.create_comment(fid, c, tid, pid)
    
    except Exception as e:
        print(e)
        print("Comment created badly! Please check it again!")
        print_test_separator("Finishing test_create_comment_fail")
    
    
    #print_test_separator("Finishing test_create_comment_fail")
#test_create_comment_fail()
''' just for fun in order to simulate the real database'''
def test_create_comment_dirtyWords_clean():
    print_test_separator("Starting test_create_comment_dirtyWords_clean")
    try:
        t = fg.get_team('NYA')
        f = fg.get_fan('jm1')
        p = fg.get_player('cashnan01')
        pid = p['player_id']
        tid = t['team_id']
        fid = f['uni']
        c = "It sucks"     
        '''Because this is dirty words, I will notice my client or engineer to delete or use **** 
        to replace it'''
        c = fg.create_comment(fid, c, tid, pid)
        print('Has cleaned a dirty comment successfully! ')
    
    except Exception as e:
        print(e)
        print("Comment created badly! Please check it again!")
        #print_test_separator("Finishing test_create_comment_fail_dirtyWords")
    
    
    print_test_separator("Finishing test_create_comment_dirtyWords_clean")
    
#test_create_comment_dirtyWords_clean()

def test_create_sub_comment():
    print_test_separator("Starting test_create_sub_comment")
    try:
        c = fg.get_comment("63db5904-97ef-40dc-b35b-339b630a31ac")
        m = "Agree, but I also think Ted is always the best!"
        r = fg.create_sub_comment('jm1', "63db5904-97ef-40dc-b35b-339b630a31ac", m)
        print('Sub_comment created successfully')
        #print('c:',c)
        print('sub_c:',r)
    except Exception as e:
        print(e)
        print("Comment created badly! Please check it again!")
        #print_test_separator("Finishing test_create_comment_fail")
    
    print_test_separator("Finishing test_create_sub_comment")
    
test_create_sub_comment()


def test_create_sub_comment_fail():
    print_test_separator("Starting test_create_sub_comment_fail")
    try:
        #c = fg.get_comment("9e3a9a9b-04d7-4650-b1f7-31e687d3fd52")
        m = "Yeah!"
        r = fg.create_sub_comment('tj1', "Yeah_Yeah_it's_close_to_final", m)
        print('Sub_comment_fail created not successfully')
        print('c:',c)
    except Exception as e:
        print(e)
        print("Sub Comment created badly! Please check it again!")
        #print_test_separator("Finishing test_create_comment_fail")
    
    print_test_separator("Finishing test_create_sub_comment_fail")

#test_create_sub_comment_fail()

def test_create_sub_comment_fail_2():
    print_test_separator("Starting test_create_sub_comment_fail_2")
    try:
        #c = fg.get_comment("9e3a9a9b-04d7-4650-b1f7-31e687d3fd52")
        m = "Yeah!"
        r = fg.create_sub_comment('ppp', "2df85cf5-5d88-41cc-b49e-36ae3dcf396a", m)
        print('Sub_comment_fail_2 created not successfully')
        print('c:',c)
    except Exception as e:
        print(e)
        print("Sub Comment created badly! Please check it again!")
        #print_test_separator("Finishing test_create_comment_fail")
    
    print_test_separator("Finishing test_create_sub_comment_fail_2")

#test_create_sub_comment_fail_2()


def test_get_player_comments(player_id):
    print_test_separator("Starting test_get_player_comments")
    
    try:
        result = fg.get_player_comments(player_id)
        print('result:',json.dumps(result,indent=2))
        
    except Exception as e:
        print(e)
        
    print_test_separator("Finishing test_get_player_comments")
    
#test_get_player_comments("pedrodu01")
    
def test_get_team_comments(team_id):
    print_test_separator("Starting test_get_team_comments")
    
    try:
        result = fg.get_team_comments(team_id)
        print('result:',json.dumps(result,indent=2))
        
    except Exception as e:
        print(e)   
    
    print_test_separator("Finishing test_get_team_comments")

#test_get_team_comments("BOS")
    
#test_create_sub_comment1()
#test_get_player_comments("pedrodu01")
#test_get_team_comments("BOS")








