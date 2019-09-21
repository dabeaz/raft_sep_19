# raftcore.py
#

import logging

from .raftlog import RaftLog, LogEntry

class RaftEntry:
    '''
    Special log entry type for Raft-internal operations. Entries of this type
    do not get applied to the application state machine.
    '''
    def __init__(self, term, value):
        self.term = term
        self.value = value
        
# --- Messages exchanged between the Raft servers

class Message:
    pass

class AppendEntries(Message):
    def __init__(self, source, dest, term, prev_log_index,
                 prev_log_term, entries, leader_commit_index):
        self.source = source
        self.dest = dest
        self.term = term
        self.prev_log_index = prev_log_index
        self.prev_log_term = prev_log_term
        self.entries = entries
        self.leader_commit_index = leader_commit_index

class AppendEntriesResponse(Message):
    def __init__(self, source, dest, term, success, match_index):
        self.source = source
        self.dest = dest
        self.term = term
        self.success = success
        self.match_index = match_index

class RequestVote(Message):
    def __init__(self, source, dest, term, 
                 last_log_index, last_log_term):
        self.source = source
        self.dest = dest
        self.term = term
        self.last_log_index = last_log_index
        self.last_log_term = last_log_term

class RequestVoteResponse(Message):
    def __init__(self, source, dest, term, vote_granted):
        self.source = source
        self.dest = dest
        self.term = term
        self.vote_granted = vote_granted

# --- State machine

class RaftMachine:
    '''
    An instance of the state machine executed by a single Raft server.
    '''
    def __init__(self, control, apply=None):
        self.debug_log = logging.getLogger(f'machine.{control.address}')
        self.control = control
        self.state = 'FOLLOWER'
             
        # Persistent state
        self.log = RaftLog()
        self._current_term = 0
        self._voted_for = None

        # Volatile state
        self.commit_index = -1
        self.next_index = None
        self.match_index = None
        self.votes_granted = 0

        # Application function (called to apply log entries to the state machine)
        self.apply = apply

    @property
    def current_term(self):
        return self._current_term

    @current_term.setter
    def current_term(self, value):
        self.control.persist(('set', '_current_term', value))
        self._current_term = value

    @property
    def voted_for(self):
        return self._voted_for

    @voted_for.setter
    def voted_for(self, value):
        if value is not None:
            self.control.persist(('set', '_voted_for', value))
        self._voted_for = value
        
    def restore(self, transaction):
        '''
        Function to playback a transaction from the persistent storage log.
        '''
        op, *args = transaction
        if op == 'set':
            setattr(self, *args)
        elif op == 'append_entries':
            self.log.append_entries(*args)
                
    # Actions.  These functions change the machine state and perform
    # the actions that are required upon entry to a given state.  These functions
    # may be performed by code within the state machine itself (i.e., election
    # to leader by a candidate).  They could also be triggered externally by
    # events in the controller (e.g., election timeout).
    
    def become_follower(self):
        self.debug_log.info('Becoming Follower')
        self.state = 'FOLLOWER'
        
    def become_leader(self):
        self.debug_log.info('Becoming Leader')
        assert self.state == 'CANDIDATE'
        self.state = 'LEADER'
        self.next_index = [ len(self.log) for n in range(self.control.numservers) ]
        self.match_index = [ -1 ] * len(self.next_index)
        self.append_entries([None], EntryType=RaftEntry)  # Assert our dominance

    def become_candidate(self):
        self.debug_log.info('Becoming Candidate')
        self.state = 'CANDIDATE'
        self.current_term += 1
        self.votes_granted = 1
        self.voted_for = self.control.address
        
        for i in range(self.control.numservers):
            if i != self.control.address:
                self.control.send_message(
                    RequestVote(
                        dest=i,
                        source=self.control.address,
                        term=self.current_term,
                        last_log_index=len(self.log) - 1,
                        last_log_term=self.log[-1].term if self.log else -1
                    )
                )
        
    def append_entries(self, values, *, EntryType=LogEntry):
        '''
        Normal function to append more entries to the log.  It causes an AppendEntries
        message to be sent to all of the other servers.  Only valid on a leader.
        '''
        if self.state != 'LEADER':
            return
        
        self.control.leader_alive()
        entries = [ EntryType(self.current_term, v) for v in values ]
        args = (len(self.log),
                self.log[-1].term if self.log else 0,
                entries)
        if entries:
            self.log.append_entries(*args)
            self.control.persist(('append_entries', *args))

        self.match_index[self.control.address] = len(self.log) - 1
        for i in range(self.control.numservers):
            if i != self.control.address:
                self.send_append_entries(i)
                
    def send_append_entries(self, i):
        '''
        Helper function to send an append entries message to a specified server
        '''
        self.control.send_message(
            AppendEntries(
                dest = i,
                source = self.control.address,
                term = self.current_term,
                prev_log_index = self.next_index[i] - 1,
                prev_log_term = self.log[self.next_index[i]-1].term if self.next_index[i] > 0 else 0,
                entries = self.log[self.next_index[i]:],
                leader_commit_index = self.commit_index,
            )
        )

    def handle_Message(self, msg):
        if msg.term > self.current_term:
            self.current_term = msg.term
            self.voted_for = None
            self.become_follower()

        if msg.term < self.current_term:
            # Drop the message entirely (from a historic term)
            return

        # Dispatch to appropriate method
        getattr(self, f'handle_{type(msg).__name__}')(msg)
            
    def handle_AppendEntries(self, msg):
        if self.state == 'CANDIDATE' and msg.term == self.current_term:
            self.become_follower()
            
        if self.state == 'FOLLOWER':
            self.control.leader_alive()
            success = self.log.append_entries(msg.prev_log_index+1,
                                              msg.prev_log_term,
                                              msg.entries)
            if success and msg.entries:
                self.control.persist(('append_entries',
                                      msg.prev_log_index+1,
                                      msg.prev_log_term,
                                      msg.entries))
                            
            new_commit_index = min(len(self.log)-1, msg.leader_commit_index)
            while self.commit_index < new_commit_index:
                self.commit_index += 1
                if self.apply and isinstance(self.log[self.commit_index], LogEntry):
                    self.debug_log.info("Applying %r", self.log[self.commit_index].value)
                    self.apply(self.log[self.commit_index].value)

            self.control.send_message(            
                    AppendEntriesResponse(
                        dest = msg.source,
                        source = msg.dest,
                        term = self.current_term,
                        success = success,
                        match_index = len(self.log) - 1,                        
                    )
                    )
            

    def handle_AppendEntriesResponse(self, msg):
        if self.state == 'LEADER':
            if not msg.success:
                self.next_index[msg.source] -= 1
                self.send_append_entries(msg.source)
            else:
                self.match_index[msg.source] = msg.match_index
                self.next_index[msg.source] = msg.match_index + 1

                # Must determine the highest log index for which there is
                # a quorum
    
                min_commit = sorted(self.match_index)[self.control.quorum-1]
                if (min_commit > self.commit_index and 
                       self.log[min_commit].term == self.current_term):
                    self.debug_log.info("Committing: %d", min_commit)

                    while self.commit_index < min_commit:
                        self.commit_index += 1
                        if self.apply and isinstance(self.log[self.commit_index], LogEntry):
                            self.debug_log.info("Applying %r", self.log[self.commit_index].value)
                            self.apply(self.log[self.commit_index].value)

    def handle_RequestVote(self, msg):
        if (self.voted_for is None or self.voted_for == msg.source):
            if not self.log:
                vote_granted = True
            else:
                if msg.last_log_term > self.log[-1].term:
                    vote_granted = True
                elif (msg.last_log_term == self.log[-1].term) and \
                     msg.last_log_index >= (len(self.log) - 1):
                    vote_granted = True
                else:
                    vote_granted = False
        else:
            vote_granted = False

        if vote_granted:
            self.voted_for = msg.source
            
        self.control.send_message(
            RequestVoteResponse(
                dest=msg.source,
                source=msg.dest,
                term=self.current_term,
                vote_granted=vote_granted
                )
            )

    def handle_RequestVoteResponse(self, msg):
        if self.state == 'CANDIDATE':
            if msg.vote_granted:
                self.votes_granted += 1
            if self.votes_granted >= self.control.quorum:
                self.become_leader()
        

