import hashlib

from eth_typing import NodeID

from .typing import ContentID, ContentKey


def content_key_to_content_id(key: ContentKey) -> ContentID:
    return ContentID(hashlib.sha256(key).digest())


def compute_content_distance(node_id: NodeID, content_id: ContentID) -> int:
    node_id_int = int.from_bytes(node_id, "big")
    content_id_int = int.from_bytes(content_id, "big")
    return node_id_int ^ content_id_int


def compute_content_log_distance(node_id: NodeID, content_id: ContentID) -> int:
    return compute_content_distance(node_id, content_id).bit_length()
