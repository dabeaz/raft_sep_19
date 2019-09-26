# config.py

# These are endpoints used for the Raft cluster itself
SERVERS = {
    0: ('localhost', 15000),
    1: ('localhost', 15001),
    2: ('localhost', 15002),
    3: ('localhost', 15003),
    4: ('localhost', 15004),
    }

# These are endpoints for the Application client/server
APPSERVERS = {
    0: ('localhost', 16000),
    1: ('localhost', 16001),
    2: ('localhost', 16002),
    3: ('localhost', 16003),
    4: ('localhost', 16004),
    }

# Number of seconds between AppendEntries messages on leader
LEADER_HEARTBEAT = 1

# Election timeout.  Time before an election is called (randomized)
ELECTION_TIMEOUT_BASE = 3
ELECTION_TIMEOUT_SPREAD = 2

