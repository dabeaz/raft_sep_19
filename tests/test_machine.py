# Basic tests of the Raft state machine.
#
# These only illustrate a potential process of testing. They do not aim to
# cover every possible corner case (that would be need to be fleshed out later).

from rafto import raftcore

# The state machine requires interaction with the controller.  This
# object provides the minimal API for purpose of testing

class MockControl:
    def __init__(self, address, numservers=5):
        # --- These attributes are required by the machine
        self.address = address
        self.numservers = numservers
        self.quorum = numservers // 2 + 1

        # --- internal
        self.messages = [ ]
        self.persist_log = [ ]
        self.alive = False

    def persist(self, entry):
        self.persist_log.append(entry)

    def send_message(self, msg):
        self.messages.append(msg)

    def leader_alive(self):
        self.alive = True

def test_init():
    # Assert some assumptions about newly created machines
    m = raftcore.RaftMachine(MockControl(0))
    assert m.state == 'FOLLOWER'
    assert m.current_term == 0
    assert m.voted_for == None
    assert len(m.log) == 0

def test_append_entries():
    m = raftcore.RaftMachine(MockControl(0))

    # Fake leader state
    m.state = 'LEADER'
    m.match_index = [-1]*m.control.numservers
    m.next_index = [0]*m.control.numservers

    assert not m.control.alive
    m.append_entries(['x'])

    # Appending entries indicates that the leader is alive
    assert m.control.alive

    # The entry should be in the log
    assert len(m.log) == 1
    assert m.log[0].value == 'x'

    # There should be outgoing AppendEntry messages to all of the other servers
    assert len(m.control.messages) == 4
    assert all(type(_) is raftcore.AppendEntries for _ in m.control.messages)
    assert { _.dest for _ in m.control.messages} == { 1, 2, 3, 4 }

def test_become_candidate():
    m = raftcore.RaftMachine(MockControl(0))

    prior_term = m.current_term

    m.become_candidate()
    
    assert m.state == 'CANDIDATE'
    assert m.current_term == prior_term + 1      
    assert m.voted_for == 0
    assert len(m.control.messages) == 4
    assert all(type(_) is raftcore.RequestVote for _ in m.control.messages)
    assert { _.dest for _ in m.control.messages} == { 1, 2, 3, 4 }
    return m

def test_become_leader():
    m = test_become_candidate()
    m.control.messages.clear()

    # Nothing should be in log yet
    assert len(m.log) == 0
    m.become_leader()
    assert m.state == 'LEADER'

    assert m.next_index == [0] * 5

    # Note: Leader added an entry to its log, so it's one ahead of followers
    assert m.match_index == [0, -1, -1, -1, -1]

    # On becoming leader, AppendEntries should be sent
    assert len(m.control.messages) == 4
    assert all(type(_) is raftcore.AppendEntries for _ in m.control.messages)
    assert { _.dest for _ in m.control.messages} == { 1, 2, 3, 4 }

    # A special RaftEntry should go in the log
    assert len(m.log) == 1
    return m

def test_vote_response():
    m = test_become_candidate()
    # Upon becoming a candidate you should have voted for yourself
    assert m.voted_for == 0
    assert m.votes_granted == 1

    # Now, let's send in a successful vote response message
    m.handle_Message(
        raftcore.RequestVoteResponse(
            dest=0,
            source=1,
            term=m.current_term,
            vote_granted=True
            )
        )
    assert m.votes_granted == 2

    # Now an unsuccessful message
    m.handle_Message(
        raftcore.RequestVoteResponse(
            dest=0,
            source=2,
            term=m.current_term,
            vote_granted=False
            )
        )

    assert m.votes_granted == 2

    # Now, another message to make a quorum
    m.handle_Message(
        raftcore.RequestVoteResponse(
            dest=0,
            source=3,
            term=m.current_term,
            vote_granted=True
            )
        )
    assert m.votes_granted == 3

    # The machine should have gone to leader state
    assert m.state == 'LEADER'

def test_append_entries_response():
    m = test_become_leader()
    m.control.messages.clear()

    # Receive a response on one follow
    m.handle_Message(
        raftcore.AppendEntriesResponse(
            source=1,
            dest=0,
            term=m.current_term,
            success=True,
            match_index=0,
            )
        )

    assert m.next_index[1] == 1
    assert m.match_index[1] == 0

    # Add a log entry
    m.append_entries(['x'])
    
    # Should be two entries on leader log (leader change plus new item)
    assert len(m.log) == 2      

    # Should be append entry messages for followers
    msgs = { msg.dest: msg for msg in m.control.messages }
    assert len(msgs) == 4

    # Follower 1 only has one entry because it already responded
    assert len(msgs[1].entries) == 1

    # Follower 2 has two entries because it did not respond
    assert len(msgs[2].entries) == 2

    # Simulate a false response.
    m.next_index[3] = 1       
    m.match_index[3] = -1
    m.handle_Message(
        raftcore.AppendEntriesResponse(
            source=3,
            dest=0,
            term=m.current_term,
            success=False,
            match_index=0,
            )
        )

    # Should decrement the next_index value.
    assert m.next_index[3] == 0
    assert m.match_index[3] == -1


    




    

    




    
    


