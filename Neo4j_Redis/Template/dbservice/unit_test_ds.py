import dataservice
import utils.utils as ut
from redis_cache.utils import utils as ut
import json

#ut.set_debug_mode(True)
dataservice.set_config()

template = {
    "nameLast": "Williams",
    "nameFirst": "Ted"
}

fields = ['playerID', 'nameFirst', 'bats', 'birthCity']

'''************************ Second Testing Data ***************************'''
#dataservice.set_config()

template1 = {
    "nameLast": "Abreu",
    "nameFirst": "Jose"
}

fields1 = ['playerID', 'nameLast', 'bats', 'birthCountry']


def test_get_resource():
    i = 3
    while i:
        result = dataservice.retrieve_by_template("people", template, fields)
        print("Result = ", json.dumps(result, indent=2))
        i -= 1

def test_get_resource1():
    i = 3
    while i:
        result = dataservice.retrieve_by_template("people", template1, fields1)
        print("Result = ", json.dumps(result, indent=2))
        i -= 1

#test_get_resource()
test_get_resource1()


