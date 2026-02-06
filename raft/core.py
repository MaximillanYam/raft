from .timer import RaftTimer
from .filestorage import FileStorage 
from enum import Enum
import asyncio
import message

class CoreFailure(Exception): 
    pass

class Status(Enum):
    follower = "follower"
    candidate = "candidate" 
    leader = "leader"

class CoreRaft:
    def __init__(self, node_id, peers, database_path): 
        min_timeout = .150 
        max_timeout = .300
        heartbeat_interval = .05
        self.raft_timer = RaftTimer(self.on_election_timeout, min_timeout, max_timeout)
        self.heartbeat_timer = RaftTimer(self.send_heartbeats, heartbeat_interval, heartbeat_interval)
        self.storage = FileStorage(database_path)
        self.node_id = node_id 
        self.peers = peers
        self.status = Status.follower
        self.term, self.voted_for = self.storage.load_metadata()

        self.outgoing_messages = asyncio.Queue()
        self.inbound_messages = asyncio.Queue()

        self.votes = set()
        
        self.commit_index = 0 
        self.last_applied = 0

        self.next_index = {} 
        self.match_index = {}

    # Timer call backs
    async def on_election_timeout(self):
        self.votes.clear()
        self.term += 1 
        self.voted_for = self.node_id  
        self.storage.save_metadata(self.term, self.voted_for)    
        self.status = Status.candidate    
        self.raft_timer.reset()

        last_log_index = self.storage.last_index()
        last_log_term = self.storage.last_term()

        await self.request_vote(last_log_index, last_log_term) 

    async def send_heartbeats(self):
        if self.status == Status.leader: 
            await self.append_entries([])

    # Helper functions 
    def validate_term(self, incoming_term): 
        if incoming_term > self.term: 
            self.term = incoming_term 
            self.voted_for = None 
            self.storage.save_metadata(self.term, self.voted_for) 

            if self.status != Status.follower: 
                self.status = Status.follower
                self.raft_timer.reset()
            return True 
        return False
    
    def transition_to_leader(self):
        pass

    # request vote rpc 
    async def request_vote(self, last_log_index, last_log_term): 
        for peer in self.peers: 
            request_vote_message = message.RequestVoteMessage(self.term, self.node_id, last_log_index, last_log_term) 
            await self.outgoing_messages.put((peer, request_vote_message)) 

    # append entries rpc 
    async def append_entries(self, log_entries):
        if self.status == Status.leader: 
            for peer in self.peers: 
                prev_log_index = self.next_index[peer] - 1 

                if prev_log_index == 0: 
                    prev_log_term = 0 
                else: 
                    entry = self.storage.get_entry(prev_log_index) 
                    if entry is None: 
                        raise CoreFailure(f'prev_log_index {prev_log_index} beyond our log')
                    prev_log_term = entry.term

                append_entries_message = message.AppendEntriesMessage(self.term, self.node_id, prev_log_index, prev_log_term, self.commit_index, log_entries)
                await self.outgoing_messages.put((peer, append_entries_message))
    
    # handle incoming request vote rpc
    def handle_request_vote(self, incoming_request_vote_message):
        pass

    # handle incoming append entry rpc
    def handle_append_entries(self): 
        pass

    # handle response to request vote rpc 
    def handle_request_vote_response(self): 
        pass 

    # handle response to append entry rpc 
    def handle_append_entries_response(self): 
        pass

    


    


    
