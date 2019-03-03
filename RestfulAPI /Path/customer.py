# Lahman.py

# Convert to/from web native JSON and Python/RDB types.
import json

# Include Flask packages
from flask import Flask
from flask import request
import copy

import SimpleBO

# The main program that executes. This call creates an instance of a
# class and the constructor starts the runtime.
app = Flask(__name__)

def parse_and_print_args():
    fields = None
    in_args = None
    
    if request.args is not None:
        print('say: ',request.args)
        in_args = dict(copy.copy(request.args))
        #s = SimpleBO.args_to_str(in_args)
        #print("s: ",s)
        fields = copy.copy(in_args.get('fields',None))
        print('fields is : ',fields)
        offset = copy.copy(in_args.get('offset',None))
        limit = copy.copy(in_args.get('limit',None))
        if fields:
            del(in_args['fields'])
        if offset:
            del(in_args['offset'])
        if limit:
            del(in_args['limit'])
    
    try:
        if request.data:
            body = json.loads(request.data)
        else:
            body = None
    except Exception as e:
        print("exception here is: ", e)
        body = None
    
    
    
    print("Request.args : ", json.dumps(in_args))
    return in_args,fields,body,limit,offset



@app.route('/api/people/<playerid>/career_stats', methods=['GET'])
def get_career_stats(playerid):
    if request.method == 'GET':
        result = SimpleBO.career_stats(playerid)
        return json.dumps(result), 200, \
               {"content-type": "application/json; charset: utf-8"}

@app.route('/api/roster', methods=['GET'])
def get_roster():
    if request.args:
        in_args = dict(copy.copy(request.args))
    if request.method == 'GET':
        result = SimpleBO.roster(in_args)
        return json.dumps(result), 200, \
               {"content-type": "application/json; charset: utf-8"}

@app.route('/api/teammates/<playerid>', methods=['GET'])
def get_teamates(playerid):
    if request.method == 'GET':
        result = SimpleBO.find_teamate(playerid)
        return json.dumps(result), 200, \
               {"content-type": "application/json; charset: utf-8"}

if __name__ == '__main__':
    app.run()

