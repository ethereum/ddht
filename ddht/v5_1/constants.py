from ddht.constants import DISCOVERY_MAX_PACKET_SIZE

SESSION_IDLE_TIMEOUT = 60

REQUEST_RESPONSE_TIMEOUT = 10

# safe upper bound on the size of the ENR list in a nodes message
FOUND_NODES_MAX_PAYLOAD_SIZE = DISCOVERY_MAX_PACKET_SIZE - 200