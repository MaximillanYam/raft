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
        self.request_vote_queue = asyncio.Queue()
        
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

        for peer in self.peers: 
            request_vote_message = message.RequestVoteMessage(self.term, self.node_id, last_log_index, last_log_term)
            await self.request_vote_queue.put((peer, request_vote_message)) 

    
