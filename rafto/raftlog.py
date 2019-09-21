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
    
def test_log():
    log = RaftLog()

    assert len(log) == 0

    # This would create a hole. Should return false
    assert not log.append_entries(1, 1, [ LogEntry(1, 'x') ])

    # This should work
    assert log.append_entries(0, 1, [ LogEntry(1, 'x') ])

    # This should work
    assert log.append_entries(1, 1, [ LogEntry(1, 'y') ])

    # This should not work (prior term doesn't match)
    assert not log.append_entries(2, 0, [ LogEntry(1, 'z') ])

    # This should replace the last entry.  The prior term matches
    # up so it should work
    assert log.append_entries(1, 1, [ LogEntry(2, 'z') ])
    assert len(log) == 2

    # Make sure indexing works
    assert log[1] == LogEntry(2, 'z')

test_log()

def test_scenarios():
    # Test the scenarios shown in Figure 7
    scenarios = [
        ('a', [ 1, 1, 1, 4, 4, 5, 5, 6, 6 ]),
        ('b', [ 1, 1, 1, 4 ]),
        ('c', [ 1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 6]),
        ('d', [ 1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 7, 7]),
        ('e', [ 1, 1, 1, 4, 4, 4, 4]),
        ('f', [ 1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3])
        ]
    leader_log = [ LogEntry(n, None) for n in [1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 8] ]

    for name, entries in scenarios:
        print("Trying:", name, entries)
        log = RaftLog()
        log.log = [ LogEntry(e, None) for e in entries ]
        last_index = len(leader_log) - 1
        while True:
            print("Trying", last_index, leader_log[last_index:])
            if log.append_entries(last_index,
                                  leader_log[last_index].term,
                                  leader_log[last_index:]):
                break
            else:
                last_index -= 1

        print(log.log)
        print(leader_log)
        assert log.log == leader_log

if __name__ == '__main__':
    # test_scenarios()
    pass

    


    
        
        


    
