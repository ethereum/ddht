import ipaddress
from typing import Tuple

# Default bootnodes
DEFAULT_BOOTNODES: Tuple[str, ...] = (
    "enr:-LK4QHAlBrRpcx9d6JTRA5kVnTNSPwVs-v_QIwBE8wfZxIqPWqqMDGGKpZDXI2lhbbnO66cmGK3eEzot3D_P_MGbcUAhh2F0dG5ldHOIgRebSXZucWmEZXRoMpCA4XabAAAAAP__________gmlkgnY0gmlwhBLDX_-Jc2VjcDI1NmsxoQOnyC60XGPSxv86ncxxezh0khFdgu7E3Cqr4imui_h_6oN0Y3CCIyiDdWRwgiMo",  # noqa: E501
    # https://github.com/goerli/medalla/blob/cd5c2042f6249de86bfad10d0cd141c988a42089/medalla/bootnodes.txt
    "enr:-LK4QKWk9yZo258PQouLshTOEEGWVHH7GhKwpYmB5tmKE4eHeSfman0PZvM2Rpp54RWgoOagAsOfKoXgZSbiCYzERWABh2F0dG5ldHOIAAAAAAAAAACEZXRoMpAAAAAAAAAAAAAAAAAAAAAAgmlkgnY0gmlwhDQlA5CJc2VjcDI1NmsxoQOYiWqrQtQksTEtS3qY6idxJE5wkm0t9wKqpzv2gCR21oN0Y3CCIyiDdWRwgiMo",  # noqa: E501
    "enr:-LK4QEnIS-PIxxLCadJdnp83VXuJqgKvC9ZTIWaJpWqdKlUFCiup2sHxWihF9EYGlMrQLs0mq_2IyarhNq38eoaOHUoBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpAAAAAAAAAAAAAAAAAAAAAAgmlkgnY0gmlwhA37LMaJc2VjcDI1NmsxoQJ7k0mKtTd_kdEq251flOjD1HKpqgMmIETDoD-Msy_O-4N0Y3CCIyiDdWRwgiMo",  # noqa: E501
    "enr:-KG4QIOJRu0BBlcXJcn3lI34Ub1aBLYipbnDaxBnr2uf2q6nE1TWnKY5OAajg3eG6mHheQSfRhXLuy-a8V5rqXKSoUEChGV0aDKQGK5MywAAAAH__________4JpZIJ2NIJpcIQKAAFhiXNlY3AyNTZrMaEDESplmV9c2k73v0DjxVXJ6__2bWyP-tK28_80lf7dUhqDdGNwgiMog3VkcIIjKA",  # noqa: E501
)

# Default port number
DEFAULT_PORT: int = 30303

# Default listen address
DEFAULT_LISTEN: ipaddress.IPv4Address = ipaddress.ip_address("0.0.0.0")

# Max size of discovery packets.
DISCOVERY_MAX_PACKET_SIZE = 1280

# Buffer size used for incoming discovery UDP datagrams (must be larger than
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
