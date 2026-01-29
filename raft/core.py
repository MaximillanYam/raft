from .timer import RaftTimer
from .filestorage import FileStorage 
from enum import Enum
import asyncio
import message

class Status(Enum):
    follower = "follower"
    candidate = "candidate" 
    leader = "leader"

class CoreRaft:
    def __init__(self, node_id, peers, database_path): 
        min_timeout = .150 
        max_timeout = .300
        self.raft_timer = RaftTimer(self.on_election_timeout, min_timeout, max_timeout)
        self.storage = FileStorage(database_path)
        self.node_id = node_id 
        self.peers = peers
        self.status = Status.follower
        self.term, self.voted_for = self.storage.load_metadata()
        self.outgoing_messages = asyncio.Queue()
        
        self.commit_index = 0 
        self.last_applied = 0

        self.next_index = {} 
        self.match_index = {}

    async def on_election_timeout(self):
        self.votes = {self.node_id}
        self.term += 1 
        self.voted_for = self.node_id  
        self.storage.save_metadata(self.term, self.voted_for)    
        self.status = Status.candidate    
        self.raft_timer.reset()

        last_log_index = self.storage.last_index()
        last_log_term = self.storage.last_term()

        await self.request_vote(last_log_index, last_log_term) 

    # request vote rpc ? 
    async def request_vote(self, last_log_index, last_log_term): 
        for peer in self.peers: 
            request_vote_message = message.RequestVoteMessage(self.term, self.node_id, last_log_index, last_log_term) 
            await self.outgoing_messages.put((peer, request_vote_message)) 

    # append entries rpc ? 
    async def append_entries(self, log_entries):
        for peer in self.peers: 
            append_entries_message = message.AppendEntriesMessage(self.term, self.node_id, self.storage.last_index(), self.storage.last_termªº, self.commit_index, log_entries)
            await self.outgoing_messages.put((peer, append_entries_message))

    # handling response from request vote rpc 
    def handle_request_vote(self):
        pass 

    # handling response from append entries rpc
    def handle_append_entries(self): 
        pass



    


    
