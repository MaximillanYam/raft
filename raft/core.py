from .timer import RaftTimer
from .filestorage import FileStorage 
from enum import Enum
import asyncio
import message

class CoreFailure(Exception): 
    pass

class SubmitError(Exception):
    pass

class Status(Enum):
    follower = "follower"
    candidate = "candidate" 
    leader = "leader"

class CoreRaft:
    def __init__(self, node_id, peers, database_path, apply_function): 
        min_timeout = .150 
        max_timeout = .300
        heartbeat_interval = .05
        self.raft_timer = RaftTimer(self.on_election_timeout, min_timeout, max_timeout)
        self.heartbeat_timer = RaftTimer(self.send_heartbeats, heartbeat_interval, heartbeat_interval)
        self.storage = FileStorage(database_path)
        self.apply_function = apply_function
        self.node_id = node_id 
        self.peers = peers
        self.status = Status.follower
        self.term, self.voted_for = self.storage.load_metadata()

        if len(peers) != 0:
            self.leader_id = None 
        else:
            self.leader_id = node_id

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
        self.votes.add(self.node_id)
        self.term += 1 
        self.leader_id = None
        self.voted_for = self.node_id  
        self.storage.save_metadata(self.term, self.voted_for)    
        self.status = Status.candidate    
        self.raft_timer.reset()

        last_log_index = self.storage.last_index()
        last_log_term = self.storage.last_term()

        await self.request_vote(last_log_index, last_log_term) 

    async def send_heartbeats(self):
        if self.status == Status.leader: 
            await self.append_entries()

    # Helper functions 
    def valid_term(self, incoming_term): 
        if incoming_term > self.term: 
            self.term = incoming_term 
            self.voted_for = None 
            self.storage.save_metadata(self.term, self.voted_for) 

            if self.status != Status.follower: 
                self.status = Status.follower
                self.leader_id = None
                self.raft_timer.reset() 
            return True 
        return False
    
    def transition_to_leader(self):
        self.status = Status.leader 
        self.leader_id = self.node_id
        self.raft_timer.stop() 
        self.next_index = {peer : self.storage.last_index() + 1 for peer in self.peers}
        self.match_index = {peer : 0 for peer in self.peers} 
        self.heartbeat_timer.start()

    # request vote rpc 
    async def request_vote(self, last_log_index, last_log_term): 
        for peer in self.peers: 
            request_vote_message = message.RequestVoteMessage(self.term, self.node_id, last_log_index, last_log_term) 
            await self.outgoing_messages.put((peer, request_vote_message)) 

    # append entries rpc 
    async def append_entries(self):
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

                entries_for_peer = self.storage.get_entries_from(self.next_index[peer])

                append_entries_message = message.AppendEntriesMessage(self.term, self.node_id, prev_log_index, prev_log_term, self.commit_index, entries_for_peer)
                await self.outgoing_messages.put((peer, append_entries_message))
    
    # handle incoming request vote rpc
    def handle_request_vote(self, incoming_request_vote_message):
        self.valid_term(incoming_request_vote_message.term)
        
        if incoming_request_vote_message.term < self.term:
            return message.RequestVoteResponse(self.term, False)
        
        if self.voted_for is not None and self.voted_for != incoming_request_vote_message.candidate_id:
            return message.RequestVoteResponse(self.term, False)
        
        node_last_log_index = self.storage.last_index()
        node_last_log_term = self.storage.last_term()
        
        candidate_log_outdated = (
            incoming_request_vote_message.last_log_term < node_last_log_term or
            (incoming_request_vote_message.last_log_term == node_last_log_term and 
            incoming_request_vote_message.last_log_index < node_last_log_index)
        )
        
        if candidate_log_outdated:
            return message.RequestVoteResponse(self.term, False)
        
        self.voted_for = incoming_request_vote_message.candidate_id
        self.storage.save_metadata(self.term, self.voted_for)
        self.raft_timer.reset()
        return message.RequestVoteResponse(self.term, True)

    # handle incoming append entry rpc
    def handle_append_entries(self, incoming_append_entries_message): 
        self.valid_term(incoming_append_entries_message.term) 

        if incoming_append_entries_message.term < self.term:
            return message.AppendEntriesResponse(self.term, False, 0) 

        if self.status == Status.candidate: 
            self.status = Status.follower

        self.raft_timer.reset()
        self.leader_id = incoming_append_entries_message.leader_id

        if incoming_append_entries_message.prev_log_index != 0: 
            node_entry = self.storage.get_entry(incoming_append_entries_message.prev_log_index) 
            if node_entry is None: 
                return message.AppendEntriesResponse(self.term, False, 0) 
            if node_entry.term != incoming_append_entries_message.prev_log_term:
                return message.AppendEntriesResponse(self.term, False, 0)
        
        for index, entry in enumerate(incoming_append_entries_message.entries):
            node_entry = self.storage.get_entry(entry.index)
            if node_entry is None: 
                self.storage.append_entries(incoming_append_entries_message.entries[index:])
                break 
            else:
                if node_entry.term == entry.term:
                    continue
                else:
                    self.storage.delete_from_index(entry.index)
                    self.storage.append_entries(incoming_append_entries_message.entries[index:])
                    break

        if incoming_append_entries_message.leader_commit > self.commit_index:
            last_entry_index = self.storage.last_index()
            self.commit_index = min(incoming_append_entries_message.leader_commit, last_entry_index)

        match_index = incoming_append_entries_message.prev_log_index + len(incoming_append_entries_message.entries)

        return message.AppendEntriesResponse(self.term, True, match_index)

    # handle response to request vote rpc 
    def handle_request_vote_response(self, incoming_request_vote_response, peer_id): 
        self.valid_term(incoming_request_vote_response.term)
        
        if self.status != Status.candidate: 
            return 
        
        if incoming_request_vote_response.term != self.term:
            return 
        
        self.votes.add(peer_id)

        if len(self.votes) >= (len(self.peers) + 1) // 2 + 1: 
            self.transition_to_leader()

    # handle response to append entry rpc 
    def handle_append_entries_response(self, incoming_append_entries_response, peer_id): 
        self.valid_term(incoming_append_entries_response.term)

        if self.status != Status.leader: 
            return 
        
        if incoming_append_entries_response.success: 
            self.match_index[peer_id] = incoming_append_entries_response.match_index 
            self.next_index[peer_id] = self.match_index[peer_id] + 1

            for i in range(self.storage.last_index(), self.commit_index, -1):
                entry = self.storage.get_entry(i)
                if entry.term != self.term:
                    continue 
                count = 1 
                for peer in self.peers: 
                    if self.match_index[peer] >= i:
                        count += 1 
                if count >= (len(self.peers) + 1) // 2 + 1:
                    self.commit_index = i
                    break 
        else: 
            self.next_index[peer_id] -= 1
    
    async def message_router(self):
        while True: 
            sender_id, inbound_message = await self.inbound_messages.get()
            try: 
                if isinstance(inbound_message, message.AppendEntriesMessage):
                    result = self.handle_append_entries(inbound_message)
                    await self.outgoing_messages.put((sender_id, result)) 
                elif isinstance(inbound_message, message.AppendEntriesResponse):
                    self.handle_append_entries_response(inbound_message, sender_id)
                elif isinstance(inbound_message, message.RequestVoteMessage):
                    result = self.handle_request_vote(inbound_message)
                    await self.outgoing_messages.put((sender_id, result))
                elif isinstance(inbound_message, message.RequestVoteResponse):
                    self.handle_request_vote_response(inbound_message, sender_id)
            finally:
                self.inbound_messages.task_done()

    async def apply_committed_entries(self): 
        while True: 
            while self.last_applied < self.commit_index: 
                self.last_applied += 1 
                entry = self.storage.get_entry(self.last_applied) 
                self.apply_function(entry.command) 
            await asyncio.sleep(0.01)

    async def submit_command(self, command: bytes):
        if self.status != Status.leader:
            return self.leader_id 
        index = self.storage.last_index() + 1
        entry = message.LogEntry(index=index, term=self.term, command=command)
        self.storage.append_entries([entry])
        await self.append_entries()

    async def start(self):
        self.raft_timer.start()
        await asyncio.gather(
            self.message_router(),
            self.apply_committed_entries()
        )