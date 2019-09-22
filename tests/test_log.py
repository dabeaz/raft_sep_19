# testlog.py
#
# RaftLog implements the low-level append semantics of the Raft
# transaction log in the form of a "pythonic" list object.

from rafto.raftlog import RaftLog, LogEntry
    
def test_log():
    log = RaftLog()

    assert len(log) == 0

    # Adding empty entries should work
    assert log.append_entries(0, -1, [])

    # This would create a hole. Should return false
    assert not log.append_entries(1, 1, [ LogEntry(1, 'x') ])

    # This should work (replaces existing entries)
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

def test_figure7():
    # Test the scenarios shown in Figure 7 of the Raft paper
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
        log = RaftLog()
        log.log = [ LogEntry(e, None) for e in entries ]
        last_index = len(leader_log) - 1
        while True:
            if log.append_entries(last_index,
                                  leader_log[last_index].term,
                                  leader_log[last_index:]):
                break
            else:
                last_index -= 1

        # The whole point is that the leader log should be replicated
        assert log.log == leader_log, f'{entries} failed'
