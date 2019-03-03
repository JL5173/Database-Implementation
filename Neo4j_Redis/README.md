
# Neo4j and Redis
- Neo4j is a popular graph database from facebook, this document will implement a very easy Neo4j project with dataset for lahman 2017
- Redis is very important structure in the real projects. Because of its effeciency of query, it's always be asked about in each interview.

### Part 1: neo4j

### modified functions: 

#### create_comment(self,uni,comment,team_id,player_id):

* examine1: Because Professor suggest not to use team_id and player_id to be both None, I gave a 'if' to determine that first
* examine2: Because I similated 'Shielding dirty words' in the comment, I also write a simple 'if' to determine the input comment contains dirty words or not (I know it's not simple in the real system, but just for fun)
* function: I used uuid.uuid4() to create a random strings to be consider as code of a comment
* relationships: in this function, there's some relationships I created to achieve the expecting results:1."COMMENT_ON", 2."COMMENT_BY" 3. if comment is empty, I will return raise error!

#### examine_sub_comment(self, uni, origin_comment_id, comment):
* For convenience to examine validness of input about original comment, new comment and fans in the next function (create_sub_comment), I designed additional function here to determine whether the input is valid or not. If not, I will raise NameError to show the reasons why the input is not acceptable in the console!

#### create_sub_comment(self, uni, origin_comment_id, comment)
* After exmining the validness and dirty words of input, I get_comment first in order to find the original comment
* Relationship: I created 'RESPONSE_TO'and "RESPONSE_BY" here.

#### get_player_comments(self, player_id)：
* I wrote a long neo4j query to show all comments and subcomments and fans included in these relationships.
* If I receive these data with list, it will not return some column ( like on:,response_by ).
* In this case, I used .data() from Google to show  and get that. I also give a screenshot about text from neo4j console

#### get_team_comments(self, team_id)：
* Most of thoughts and codes are similar with get_player_comments()

### Part 2: Tests With Neo4j ( unit_tests_local_social.py ):

#### test_create_comment()：
- Just create a new comment

#### test_create_comment_fail():
- If users or programmer create an empty comment, which is always considered as not meaningful or no purpose. So if our system detect that, will return a message to remind them

#### test_create_comment_fail_2()
- Professor noticed that team_id and player_id shouldn't be BOTH None in the function "create_comment"
- Therefore, I designed this function to verify that.

#### test_create_comment_dirtyWords_clean()
- I designed this interesting funtion to simulate the real situation that if developer or administriter in some big Internet Platform find some bad or too negative dirty words, they will shield or even delete them.
- However, I know the real function of codes will be more complicated and more comprehensive than my simple codes. Because of tight schedule of final, I think implementing my function successfully in this simple test will be ok,thanks!
- As you can see, if some fans comment with bad words, their words will be replaced with "****"

#### test_create_sub_comment():
- Test the function create_sub_comment()
- It can comment on some existed comment, and I showed results in the screenshot.

#### test_create_sub_comment_fail():
- Because if we want the function create_sub_comment work, we should make sure that the number of existed_comment should be valid.
- In this function, I used a wrong number about existed comment, then my function will return the reason why this implementation is bad and failed. I showed the details and explanation in the screenshot.

#### test_create_sub_comment_fail_2():
- In this function, I gave a wrong uni of fans, and my function will detect that and return the message about error.
- I showed the details and explanation in the screenshot.

#### test_get_player_comments():
- This function can retrieve all the information and relationships about the specific players

#### test_get_team_comments():
- This function can retrieve all the information and relationships about the specific teams 

### Part 3: Redis With Data Service

### 1.data_cache.py : modified functions: 

#### add_to_cache(key, value):
* Use "set key value" syntax to add new record into cache if there's no previous record about that.

#### get_from_cache(key):
* I made some changes here: I used get instead of hashget, and transfer the results into list of dictionaries.

#### check_query_cache(resource, template, fields):
* This function was designed to check whether there's any result in the cache or not. If there exists a result that matches the template, it will get matching result with function "get_from_cache(key)". 
* If this function cannot find any results with template, it will return None, and print "catch miss" in the function "retrieve_by_template" in the dataservice.py

#### add_to_query_cache(resource, template, fields, query_result):
* This function was designed to compute a key that is very clean and easy for us to recognize
* Then we can use function add_to_cache(key,value) to set a new key,value pair from the mysql into cache

### 2.dataservice.py : modified functions: 

#### retrieve_by_template(table, t, fields=None, limit=None, offset=None, orderBy=None):
* In this function, we should check whether there exists a matched record in the cache first with "check_query_cache"
* If there exists matched record in the cache, we can get results from the cache more efficiently. And print:"cache hit"
* If there's not a matched record in the cache, we can only search and get results from the mysql. And for more convenience in the future, we also add this record into the cache. Finally, print a message:"catch miss"
* Also, I think the original version of this function has something wrong, and I changed the 'fields' (in my 79-84 line) into 'field', then the program goes succussfully

### Part 4: Test With Redis

#### test1() and test2():
* Designed just for examination of correctness of most main functions from data_cache.py

### Part 5: Test With dataservice.py

#### Main Idea:
- According to the lecture, and from my perspective, I think the main idea about cache is that :
  - Our check_query_cache() will check whether there exists a matched record or not in the cache when we are searching for some records.
  -  If there have existed matched records in the cache, my system will return the expecting results more quickly through cache instead of searching records in the database. Print message: "cache hit"
  -  If there're not matched records in the cache, print('cache miss') first, then my 'engine' will search them in the database( mysql ) and put them into our cache for more convenience of finding them again in the future.
  
#### test_get_resource():
- In this function, my idea is to verify above thoughts from 'Main idea', I run function"retrieve_by_template" 3 times.
- First: Result will print "cache miss" and get results from database, then put our record into the cache
- Second: Result will print "cache hit" and get results from the cache. That means we input our data successful last time. And also means that it can find results from cache.
- Third: This result is same with the second time.

#### test_get_resource1():
- To make sure, I just replaced with another template and test function "retrieve_by_template" again.

## Notice and Conclusion: 

- My database name is not lahman2017, which is lahman2017raw_pk instead. Because the original database has been inserted and deleted due to previous midterm exam or homework. Therefore, if you want to run my code and examine, please be careful about this. Sorry for your inconvenience.

- For convenience of your examining my HW4, I gave a file called "Screenshot_complete", which includes all images about part1 and part2. And I also put each part of images into each python file with the same directory as announced by professor.

- Because of tight schedule of final exams of four courses, I just provided there above test cases. However, I have a lot of thoughts about test cases, such as create_topics, which can let fans to follow the different and hot topics like "Vote for your favoriate player" or "What do you think of xxx's performance last night." In the topic part, I could also use create_comment, create_sub_comment and etc.

- In my test cases, I also very focus on the failed cases. Because I think a good engineer should forcast the possible errors in the future and how to deal with that.

- Conlcusion:
    - 1. Cache can be considered a middle station that can help users or developers retrieve data more efficiently. And since cache utilizes HashMap to search for results ( O(1) time complexity), this is a very helpful tools in the data science
    - 2. Neo4j is a very good and Intuitive database, which can define and show relationships very easily. From my perspection, I think this kind of Neo4j is very useful in the social app, like facebook, twitter. 
