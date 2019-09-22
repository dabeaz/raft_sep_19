# raftlog.py
#
# This is an implementation of the Raft log as a list-like object that's
# completely decoupled from remote procedure call, events, and other parts
# of Raft.   The idea is to have something small and self-contained that
# can be tested and debugged more easily.
#

from collections import namedtuple
LogEntry = namedtuple('LogEntry', ['term', 'value'])

class RaftLog:
    def __init__(self):
        self.log = []
        
    def __len__(self):
        return len(self.log)

    def __repr__(self):
        return f'RaftLog({self.log})'

    def __getitem__(self, index):
        return self.log[index]
        
    def append_entries(self, index, prior_term, entries):
        '''
        Inserts new entries at position index.  If this is not possible, return False.
        No holes in the log are allowed.  If placing items at index would result in
        undefined entries, it fails.  Also, you must specify the term of the log
        entry immediately prior to index.  If this doesn't match, the append operation
        will fail.
        '''
        # No holes in the log are allowed
        if index > len(self.log):
            return False

        # The term of the prior log entry has to match (#2, Receiver Implementation).
        if index > 0 and self.log[index-1].term != prior_term:
            return False
        
        # All entries at index (and beyond) are replaced by the new entries
        self.log[index:] = entries
        return True

    


    
        
        


    
