from typing import Tuple

from ddht.constants import DISCOVERY_MAX_PACKET_SIZE

SESSION_IDLE_TIMEOUT = 60

ROUTING_TABLE_KEEP_ALIVE = 300

REQUEST_RESPONSE_TIMEOUT = 10

# safe upper bound on the size of the ENR list in a nodes message
FOUND_NODES_MAX_PAYLOAD_SIZE = DISCOVERY_MAX_PACKET_SIZE - 200


DEFAULT_BOOTNODES: Tuple[str, ...] = (
    "enr:-IS4QAHCaxRnea2ypKUsy-Ldotp6pYtpqUfq0DfGBEeBhuiKDZg2lm12Bjt6KPUAccmPyFA5zkpT-ciVjt6zcxjue8cDgmlkgnY0gmlwhK3mkMaJc2VjcDI1NmsxoQJTqyNYyHmn7ibmkepLZVEuznjQeTGyAyH-xQLyL-6dZ4N1ZHCCdl8",  # noqa: E501
)
