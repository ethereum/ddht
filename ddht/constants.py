import enum
import ipaddress

# Default port number
DEFAULT_PORT: int = 30303

# Default listen address
DEFAULT_LISTEN: ipaddress.IPv4Address = ipaddress.ip_address("0.0.0.0")

# Max size of discovery packets.
DISCOVERY_MAX_PACKET_SIZE = 1280

# Buffer size used for inbound discovery UDP datagrams (must be larger than
# DISCOVERY_MAX_PACKET_SIZE)
DISCOVERY_DATAGRAM_BUFFER_SIZE = DISCOVERY_MAX_PACKET_SIZE * 2

NEIGHBOURS_RESPONSE_ITEMS = 16
AES128_KEY_SIZE = 16  # size of an AES218 key
HKDF_INFO = b"discovery v5 key agreement"
ID_NONCE_SIGNATURE_PREFIX = b"discovery-id-nonce"
ENR_REPR_PREFIX = "enr:"  # prefix used when printing an ENR
MAX_ENR_SIZE = 300  # maximum allowed size of an ENR
IP_V4_ADDRESS_ENR_KEY = b"ip"
UDP_PORT_ENR_KEY = b"udp"
TCP_PORT_ENR_KEY = b"tcp"
IP_V4_SIZE = 4  # size of an IPv4 address
IP_V6_SIZE = 16  # size of an IPv6 address
NUM_ROUTING_TABLE_BUCKETS = 256  # number of buckets in the routing table
ROUTING_TABLE_BUCKET_SIZE = 16


class ProtocolVersion(enum.Enum):
    v5 = "v5"
    v5_1 = "v5.1"
