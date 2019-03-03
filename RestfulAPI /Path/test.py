import json
import SimpleBO
import requests
from odo.backends.sql import discover_foreign_key_relationship

def test1():
    
    
    result = SimpleBO.generate_links('people',['result'],'?birthMonth=8&birthyear=1989', 
                'http://127.0.0.1:5000/api/people?birthMonth=8&birthyear=1989&limit=10&offset=20',
                'http://127.0.0.1:5000/',['10'],['20'])
    return result
    #s = {"playerID":"JL510","birthMonth":"7"}
    #result = SimpleBO.Delete('people',s)
    #result = SimpleBO.find_by_template("people",t,None)
    #print(json.dumps(result,indent=2))
    #print(SimpleBO.templateToWhereClause({"nameLast":"JL", "nameFirst":"LJ"}))

test1()