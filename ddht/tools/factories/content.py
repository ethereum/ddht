import functools
import hashlib


@functools.lru_cache(maxsize=512)
def ContentFactory(length: int) -> bytes:
    base_content = b"".join(
        (
            hashlib.sha256(i.to_bytes(32, "big")).digest()
            for i in range((length + 31) // 32)
        )
    )
    return base_content[:length]
