from dataclasses import dataclass, field

@dataclass
class LogEntry:
    index : int 
    term : int 
    command : bytes

@dataclass 
class RequestVoteMessage: 
    term : int 
    candidate_id : int 
    last_log_index : int 
    last_log_term : int 

@dataclass 
class RequestVoteResponse:
    term : int 
    vote_granted : bool

@dataclass 
class AppendEntriesMessage:
    term: int 
    leader_id : int 
    prev_log_index : int 
    prev_log_term : int 
    leader_commit : int
    entries : list[LogEntry] = field(default_factory = list)

@dataclass 
class AppendEntriesResponse: 
    term : int
    success : bool
    match_index : int 

def main(): 
    log_entry_1 = LogEntry(0, 0, 'Insert into database1')
    log_entry_2 = LogEntry(0, 0, 'Insert into database2')
    request_vote_message = RequestVoteMessage(0, 0, 0, 0) 
    append_entries_message = AppendEntriesMessage(0, 0, 0, 0, 0, [log_entry_1, log_entry_2]) 
    print(log_entry_1, log_entry_2, request_vote_message, append_entries_message, sep = '\n', end = '\n')

if __name__ == '__main__': 
    main()