import enum
from typing import ClassVar, Dict

from eth_typing import Hash32
from eth_utils import ValidationError, keccak
from lru import LRU
from pyethash import EPOCH_LENGTH, hashimoto_light, mkcache_bytes
import rlp
from rlp.exceptions import DecodingError
import trio

from ddht.v5_1.alexandria.abc import AlexandriaNetworkAPI, ContentValidatorAPI
from ddht.v5_1.alexandria.rlp_sedes import BlockHeader
from ddht.v5_1.alexandria.typing import ContentKey


class ContentFlag(enum.IntEnum):
    TEST = 255
    HEADER = 1


class ContentValidator(ContentValidatorAPI):
    is_test_enabled: ClassVar[bool] = False

    def __init__(self, network: AlexandriaNetworkAPI) -> None:
        self._network = network

    async def validate_content(self, content_key: ContentKey, content: bytes) -> None:
        try:
            flag = ContentFlag(content_key[0])
        except IndexError:
            raise ValidationError(f"Invalid: content_key={content_key.hex()}")
        except ValueError:
            raise ValidationError(f"Unsupported: content_key={content_key.hex()}")

        if flag is ContentFlag.HEADER:
            await self.validate_header(content_key, content)
        elif flag is ContentFlag.TEST:
            # test content is always valid
            pass
        else:
            raise ValidationError(
                f"Unsupported content key: content_key={content_key.hex()}"
            )

    async def validate_header(self, content_key: ContentKey, content: bytes) -> None:
        # 1. key well formed
        # 2. content decodes as header
        # 3. passes proof of work check
        # 4. TODO: canonical...
        flag = ContentFlag(content_key[0])
        if flag is not ContentFlag.HEADER:
            raise ValidationError(f"Invalid flag: content_key={content_key.hex()}")

        block_hash = Hash32(content_key[1:])

        if len(block_hash) != 32:
            raise ValidationError(f"Invalid block hash: value={block_hash.hex()}")

        try:
            header: BlockHeader = rlp.decode(content, sedes=BlockHeader)
        except DecodingError as err:
            raise ValidationError(f"Invalid RLP: {err}") from err

        if header.hash != block_hash:
            raise ValidationError(
                f"Block hash mismatch: expected={block_hash.hex()}  "
                f"actual={header.hash.hex()}"
            )

        # We run this in a thread to avoid blocking since it is CPU intensive
        await trio.to_thread.run_sync(
            check_pow,
            *(
                header.block_number,
                header.mining_hash,
                header.mix_hash,
                header.nonce,
                header.difficulty,
            ),
        )


cache_by_epoch: Dict[int, bytes] = LRU(16)


def get_cache(block_number: int) -> bytes:
    epoch_index = block_number // EPOCH_LENGTH

    # doing explicit caching, because functools.lru_cache is 70% slower in the tests

    # Get the cache if already generated, marking it as recently used
    if epoch_index in cache_by_epoch:
        return cache_by_epoch[epoch_index]

    # Generate the cache if it was not already in memory
    # Simulate requesting mkcache by block number: multiply index by epoch length
    c = mkcache_bytes(epoch_index * EPOCH_LENGTH)
    cache_by_epoch[epoch_index] = c

    return c  # type: ignore


def check_pow(
    block_number: int,
    mining_hash: Hash32,
    mix_hash: Hash32,
    nonce: bytes,
    difficulty: int,
) -> None:
    if len(mix_hash) != 32:
        raise ValidationError(f"Wrong length: mix_hash={mix_hash.hex()}")
    elif len(mining_hash) != 32:
        raise ValidationError(f"Wrong length: mining_hash={mining_hash.hex()}")
    elif len(nonce) != 8:
        raise ValidationError(f"Wrong length: nonce={nonce.hex()}")

    cache = get_cache(block_number)
    mining_output = hashimoto_light(
        block_number, cache, mining_hash, int.from_bytes(nonce, "big")
    )

    mix_digest = mining_output[b"mix digest"]

    if mix_digest != mix_hash:
        raise ValidationError(
            f"Invalid POW: expected={mix_digest.hex()}  "
            f"actual={mix_hash.hex()}. "
            f"Mix hash calculated from block #{block_number}, "
            f"mine hash {mining_hash.hex()}, nonce {nonce.hex()}"
            f", difficulty {difficulty}, cache hash {keccak(cache).hex()}"
        )
    result = int.from_bytes(mining_output[b"result"], "big")

    if result > 2 ** 256 // difficulty:
        raise ValidationError(
            f"Insufficient difficulty: actual={result}  required={difficulty}"
        )
