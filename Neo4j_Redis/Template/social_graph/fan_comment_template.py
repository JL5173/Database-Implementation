from py2neo import Graph, NodeMatcher, Node, Relationship
import json
#from HW4Template.utils import utils as ut
import utils as ut
import uuid


class FanGraph(object):
    """
    This object provides a set of helper methods for creating and retrieving Nodes and relationship from
    a Neo4j database.
    """

    # Connects to the DB and sets a Graph instance variable.
    # Also creates a NodeMatcher, which is a py2neo class.
    def __init__(self,  auth, host, port, secure=False, ):
        self._graph = Graph(secure=secure,
                            bolt=True,
                            auth=auth,
                            host=host,
                            port=port)
        self._node_matcher = NodeMatcher(self._graph)

    def run_match(self, labels=None, properties=None):
        """
        Uses a NodeMatcher to find a node matching a "template."
        :param labels: A list of labels that the node must have.
        :param properties: A parameter list of the form prop1=value1, prop2=value2, ...
        :return: An array of Node objects matching the pattern.
        """
        #ut.debug_message("Labels = ", labels)
        #ut.debug_message("Properties = ", json.dumps(properties))

        if labels is not None and properties is not None:
            result = self._node_matcher.match(labels, **properties)
        elif labels is not None and properties is None:
            result = self._node_matcher.match(labels)
        elif labels is None and properties is not None:
            result = self._node_matcher.match(**properties)
        else:
            raise ValueError("Invalid request. Labels and properties cannot both be None.")

        # Convert NodeMatch data into a simple list of Nodes.
        full_result = []
        for r in result:
            full_result.append(r)

        return full_result

    def find_nodes_by_template(self, tmp):
        """

        :param tmp: A template defining the label and properties for Nodes to return. An
         example is { "label": "Fan", "template" { "last_name": "Ferguson", "first_name": "Donald" }}
        :return: A list of Nodes matching the template.
        """
        labels = tmp.get('label')
        props = tmp.get("template")
        result = self.run_match(labels=labels, properties=props)
        return result

    # Create and save a new node for  a 'Fan.'
    def create_fan(self, uni, last_name, first_name):
        n = Node("Fan", uni=uni, last_name=last_name, first_name=first_name)
        tx = self._graph.begin(autocommit=True)
        tx.create(n)

    # Given a UNI, return the node for the Fan.
    def get_fan(self, uni):
        n = self.find_nodes_by_template({"label": "Fan", "template": {"uni": uni}})
        if n is not None and len(n) > 0:
            n = n[0]
        else:
            n = None

        return n

    def create_player(self, player_id, last_name, first_name):
        n = Node("Player", player_id=player_id, last_name=last_name, first_name=first_name)
        tx = self._graph.begin(autocommit=True)
        tx.create(n)
        return n

    def get_player(self, player_id):
        n = self.find_nodes_by_template({"label": "Player", "template": {"player_id": player_id}})
        if n is not None and len(n) > 0:
            n = n[0]
        else:
            n = None

        return n

    def create_team(self, team_id, team_name):
        n = Node("Team", team_id=team_id, team_name=team_name)
        tx = self._graph.begin(autocommit=True)
        tx.create(n)
        return n

    def get_team(self, team_id):
        n = self.find_nodes_by_template({"label": "Team", "template": {"team_id": team_id}})
        if n is not None and len(n) > 0:
            n = n[0]
        else:
            n = None

        return n

    def create_supports(self, uni, team_id):
        """
        Create a SUPPORTS relationship from a Fan to a Team.
        :param uni: The UNI for a fan.
        :param team_id: An ID for a team.
        :return: The created SUPPORTS relationship from the Fan to the Team
        """
        f = self.get_fan(uni)
        t = self.get_team(team_id)
        r = Relationship(f, "SUPPORTS", t)
        tx = self._graph.begin(autocommit=True)
        tx.create(r)
        return r

    # Create an APPEARED relationship from a player to a Team
    def create_appearance(self, player_id, team_id):
        try:
            f = self.get_player(player_id)
            t = self.get_team(team_id)
            r = Relationship(f, "APPEARED", t)
            tx = self._graph.begin(autocommit=True)
            tx.create(r)
        except Exception as e:
            print("create_appearances: exception = ", e)

    # Create a FOLLOWS relationship from a Fan to another Fan.
    def create_follows(self, follower, followed):
        f = self.get_fan(follower)
        t = self.get_fan(followed)
        r = Relationship(f, "FOLLOWS", t)
        tx = self._graph.begin(autocommit=True)
        tx.create(r)

    def get_comment(self, comment_id):
        n = self.find_nodes_by_template({"label": "Comment", "template": {"comment_id": comment_id}})
        if n is not None and len(n) > 0:
            n = n[0]
        else:
            n = None

        return n

    def create_comment(self, uni, comment, team_id=None, player_id=None):
        """
        Creates a comment
        :param uni: The UNI for the Fan making the comment.
        :param comment: A simple string.
        :param team_id: A valid team ID or None. team_id and player_id cannot BOTH be None.
        :param player_id: A valid player ID or None
        :return: The Node representing the comment.
        
        """
        
        if comment:
            if 'sucks' in comment or 'fuc*' in comment or 'sh*t' in comment:
                comment_id = str(uuid.uuid4())
                com = Node("Comment",comment_id=comment_id,team_id=team_id,player_id=player_id,comment='****')
            else:
                comment_id = str(uuid.uuid4())
                com = Node("Comment",comment_id=comment_id,team_id=team_id,player_id=player_id,comment=comment)

            tx = self._graph.begin(autocommit=True)
            tx.create(com)
            fan = self.get_fan(uni)
            
            # fan and comment are necessary here
            # define relation shape with COMMENT_BY:
            r_fan_com = Relationship(fan, "COMMENT_BY", com)
            tx = self._graph.begin(autocommit=True)
            tx.create(r_fan_com)
            # Then define two kinds of COMMENT_ON
            if team_id is not None:
                tm = self.get_team(team_id)
                r_com_tm = Relationship(com, "COMMENT_ON", tm)
                tx = self._graph.begin(autocommit=True)
                tx.create(r_com_tm)
            if player_id is not None:
                plr = self.get_player(player_id)
                r_com_plr = Relationship(com, "COMMENT_ON", plr)
                tx = self._graph.begin(autocommit=True)
                tx.create(r_com_plr)
            return com
        else:
            raise NameError('There is no valid comments,please check it again')
        
    def examine_sub_comment(self, uni, origin_comment_id, comment):
        '''
        This function designed to examine whether our input is valid or not
        '''
        examine_ori = self.get_comment(origin_comment_id)
        examine_fan = self.get_fan(uni)
        if not comment:
            raise NameError('There are some problems about new_comment')
        if examine_ori is None:
            raise NameError('There are some problems about origin_comment_id')
        if examine_fan is None:
            raise NameError('There are some problems about fans')
        

    def create_sub_comment(self, uni, origin_comment_id, comment):
        """
        Create a sub-comment (response to a comment or response) and links with parent in thread.
        :param uni: ID of the Fan making the comment.
        :param origin_comment_id: Id of the comment to which this is a response.
        :param comment: Comment string
        :return: Created comment.
        """
        #examine the correctness of my function and input:
        self.examine_sub_comment(uni, origin_comment_id, comment)
        if 'sucks' in comment or 'fuc*' in comment or 'sh*t' in comment:
            comment_id = str(uuid.uuid4())
            com = Node("Comment",comment_id=comment_id,comment='****')
        else:
            comment_id = str(uuid.uuid4())
            com = Node("Comment",comment_id=comment_id,comment=comment)
        existed_comment = self.get_comment(origin_comment_id)
        tx = self._graph.begin(autocommit=True)
        tx.create(com)
        
        fan = self.get_fan(uni)
        # Response to :
        #r_fan_com = Relationship(fan, "RESPONSE_TO", existed_comment)
        r_com_ori = Relationship(com, "RESPONSE_TO", existed_comment)
        tx = self._graph.begin(autocommit=True)
        tx.create(r_com_ori)
        #comments From fan to 
        '''
        r_fan_ori = Relationship(fan, "COMMENT_BY", existed_comment)
        tx = self._graph.begin(autocommit=True)
        tx.create(r_fan_ori)
        '''
        # Response by ( from new to fan to new
        r_fan_com = Relationship(fan, "RESPONSE_BY", com)
        tx = self._graph.begin(autocommit=True)
        tx.create(r_fan_com)
    
        return com
        
        
        
        


    def get_player_comments(self, player_id):
        """
        Gets all of the comments associated with a player, all of the comments on the comment and comments
        on the comments, etc. Also returns the Nodes for people making the comments.
        :param player_id: ID of the player.
        :return: Graph containing comment, comment streams and commenters.
        """
        
        q = 'match (player:Player{player_id:' +"'" +player_id +"'"+'})-[on:COMMENT_ON*]-(comment:Comment)- \
        [response_to:RESPONSE_TO*]-(sub_comment:Comment)-[response_by:RESPONSE_BY*]-(fan2:Fan) with \
        player,on,comment,response_to,sub_comment,response_by,fan2 match (comment)-[comment_by:COMMENT_BY*] \
        -(fan:Fan) return player,on,comment,comment_by,fan,response_to,sub_comment,response_by,fan2'
        
        res = self._graph.run(q)
        res = res.data() #Checking through google and apply in this way can help me to collect complete data
        #res = list(res)
        #print('q:',q)
        return res
        

    def get_team_comments(self, team_id):
        """
        Gets all of the comments associated with a teams, all of the comments on the comment and comments
        on the comments, etc. Also returns the Nodes for people making the comments.
        :param player_id: ID of the team.
        :return: Graph containing comment, comment streams and commenters.
        """
        q = 'match (team:Team{team_id:' + "'"+team_id+"'"+'})-[on:COMMENT_ON*]-(comment:Comment)- \
        [response_to:RESPONSE_TO*]-(sub_comment:Comment)-[response_by:RESPONSE_BY*]-(fan2:Fan) with \
        team,on,comment,response_to,sub_comment,response_by,fan2 match (comment)-[comment_by:COMMENT_BY*] \
        -(fan:Fan) return team,on,comment,comment_by,fan,response_to,sub_comment,response_by,fan2' 
        
        res = self._graph.run(q)
        res = res.data() #Checking through google and apply in this way can help me to collect complete data
        #res = list(res)
        print('q:',q)
        return res














"""
bryankr01   CHN
scherma01   WAS
abreujo02   CHA
ortizda01   BOS
jeterde01   NYA
"""
