import functools
import hashlib


@functools.lru_cache(maxsize=512)
def ContentFactory(length: int = 2048) -> bytes:
    base_content = b"".join((_hash_idx(idx) for idx in range((length + 31) // 32)))
    return base_content[:length]


@functools.lru_cache(maxsize=4096)
def _hash_idx(idx: int) -> bytes:
    return hashlib.sha256(idx.to_bytes(32, "big")).digest()
