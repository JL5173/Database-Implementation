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
    in_args,fields,body,limit,offset = parse_and_print_args()
    resource_name = 'people/'+playerid+'/career_stats'
    print("r: ",resource_name)
    args = SimpleBO.args_to_str(in_args)
    if request.method == 'GET':
        if limit is None and offset is None:
            limit_default = ['10']
            offset_default = ['0']
            if not in_args and fields is None:
                url = request.url+'?limit=10&offset=0'
            else:
                url = request.url+'&limit=10&offset=0'
            url_root = request.url_root
            result = SimpleBO.career_stats(playerid,limit_default,offset_default)
            put_links=SimpleBO.generate_links(resource_name,result,args,url,url_root,limit_default,offset_default,fields)
            
            output=[{"data":result,
                       "links":put_links}]
            
            return json.dumps(output), 200, \
                {"content-type": "application/json; charset:utf-8"}
        elif limit is not None and offset is not None: 
            url = request.url
            url_root = request.url_root
            result = SimpleBO.career_stats(playerid,limit,offset)
            links=SimpleBO.generate_links(resource_name,result,args,url,url_root,limit,offset,fields)
            print('result: ',len(result))
            output=[{"data":result,
                       "links":links}]
            
            return json.dumps(output), 200, \
                {"content-type": "application/json; charset:utf-8"}

@app.route('/api/roster', methods=['GET'])
def get_roster():
    in_args,fields,body,limit,offset = parse_and_print_args()
    print(in_args)
    resource_name = 'roster'
    args = SimpleBO.args_to_str(in_args)
    if request.method == 'GET':
        if limit is None and offset is None:
            limit_default = ['10']
            offset_default = ['0']
            if not in_args and fields is None:
                url = request.url+'?limit=10&offset=0'
            else:
                url = request.url+'&limit=10&offset=0'
            url_root = request.url_root
            result = SimpleBO.roster(in_args,limit_default,offset_default)
            put_links=SimpleBO.generate_links(resource_name,result,args,url,url_root,limit_default,offset_default,fields)
            
            output=[{"data":result,
                       "links":put_links}]
            
            return json.dumps(output), 200, \
                {"content-type": "application/json; charset:utf-8"}
        elif limit is not None and offset is not None: 
            url = request.url
            url_root = request.url_root
            result = SimpleBO.roster(in_args,limit,offset)
            links=SimpleBO.generate_links(resource_name,result,args,url,url_root,limit,offset,fields)
            print('result: ',len(result))
            output=[{"data":result,
                       "links":links}]
            
            return json.dumps(output), 200, \
                {"content-type": "application/json; charset:utf-8"}
               
               
    

    
@app.route('/api/teammates/<playerid>', methods=['GET'])
def get_teamates(playerid):
    in_args,fields,body,limit,offset = parse_and_print_args()
    print(in_args)
    resource_name = 'teammates/'+playerid
    args = SimpleBO.args_to_str(in_args)
    if request.method == 'GET':
        if limit is None and offset is None:
            limit_default = ['10']
            offset_default = ['0']
            if not in_args and fields is None:
                url = request.url+'?limit=10&offset=0'
            else:
                url = request.url+'&limit=10&offset=0'
            url_root = request.url_root
            result = SimpleBO.find_teammate(playerid,limit_default,offset_default)
            put_links=SimpleBO.generate_links(resource_name,result,args,url,url_root,limit_default,offset_default,fields)
            
            output=[{"data":result,
                       "links":put_links}]
            
            return json.dumps(output), 200, \
                {"content-type": "application/json; charset:utf-8"}
        elif limit is not None and offset is not None: 
            url = request.url
            url_root = request.url_root
            result = SimpleBO.find_teammate(playerid,limit,offset)
            links=SimpleBO.generate_links(resource_name,result,args,url,url_root,limit,offset,fields)
            print('result: ',len(result))
            output=[{"data":result,
                       "links":links}]
            
            return json.dumps(output), 200, \
                {"content-type": "application/json; charset:utf-8"}
    
    


if __name__ == '__main__':
    app.run()

