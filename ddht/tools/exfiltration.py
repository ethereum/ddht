import argparse
import itertools
import logging
from typing import NamedTuple, Optional, Tuple

from eth_typing import Hash32
from eth_utils import big_endian_to_int, encode_hex, to_canonical_address
import rlp
import ssz
from ssz import sedes as ssz_sedes
import trio
from web3 import Web3

from ddht.logging import setup_stderr_logging
from ddht.v5_1.alexandria.content_storage import (
    ContentNotFound,
    FileSystemContentStorage,
)
from ddht.v5_1.alexandria.rlp_sedes import BlockHeader
from ddht.v5_1.alexandria.typing import ContentKey
from ddht.xdg import get_xdg_ddht_root

logger = logging.getLogger("ddht.tools.exfiltration")


class AccumulatorLeaf(NamedTuple):
    block_hash: Hash32
    total_difficulty: int


# This makes each accumulator
EPOCH_SIZE = 2048


AccumulatorLeafSedes = ssz_sedes.Container(
    field_sedes=(ssz_sedes.bytes32, ssz_sedes.uint256)
)
EpochAccumulatorSedes = ssz_sedes.List(AccumulatorLeafSedes, max_length=EPOCH_SIZE)

# This gives room for ~2 billion headers in the accumulator.
MasterAccumulatorSedes = ssz_sedes.List(ssz_sedes.bytes32, max_length=2 ** 24)


def header_key(block_hash: Hash32) -> ContentKey:
    return ContentKey(b"\x01" + block_hash)


def epoch_accumulator_key(epoch: int) -> ContentKey:
    return ContentKey(b"\x02" + epoch.to_bytes(3, "big"))


def master_accumulator_key(epoch: int) -> ContentKey:
    return ContentKey(b"\x03" + epoch.to_bytes(3, "big"))


LATEST_MASTER_ACCUMULATOR_KEY = ContentKey(b"meta:master-accumulator:latest")


def compute_epoch_accumulator(
    headers: Tuple[BlockHeader, ...], previous_total_difficulty: int
) -> Tuple[AccumulatorLeaf, ...]:
    if len(headers) != EPOCH_SIZE:
        raise ValueError(f"Insufficient headers: need={EPOCH_SIZE}  got={len(headers)}")

    def accumulate_total_difficulties(
        previous: AccumulatorLeaf, header: BlockHeader
    ) -> AccumulatorLeaf:
        return AccumulatorLeaf(
            block_hash=header.hash,
            total_difficulty=previous.total_difficulty + header.difficulty,
        )

    first_leaf = AccumulatorLeaf(
        block_hash=headers[0].hash,
        total_difficulty=previous_total_difficulty + headers[0].difficulty,
    )
    accumulator = tuple(
        itertools.accumulate(
            headers[1:], accumulate_total_difficulties, initial=first_leaf,
        )
    )
    return accumulator


async def exfiltrate_header_chain(
    w3: Web3,
    epoch_start_at: Optional[int],
    epoch_end_at: Optional[int],
    truncate: bool = False,
) -> None:
    ddht_xdg_root = get_xdg_ddht_root()
    storage_dir = ddht_xdg_root / "alexandria" / "storage"
    storage_dir.mkdir(parents=True, exist_ok=True)

    content_storage = FileSystemContentStorage(storage_dir)

    try:
        master_accumulator_data = content_storage.get_content(
            LATEST_MASTER_ACCUMULATOR_KEY
        )
    except ContentNotFound:
        master_accumulator = []
    else:
        master_accumulator = ssz.decode(
            master_accumulator_data, sedes=MasterAccumulatorSedes
        )

    if epoch_start_at is None:
        epoch_start_at = len(master_accumulator)
    elif epoch_start_at > len(master_accumulator):
        raise Exception(
            f"Start epoch after master accumulator: "
            f"start-epoch={epoch_start_at}  master={len(master_accumulator)}"
        )
    elif len(master_accumulator) > epoch_start_at:
        if truncate is False:
            num_to_truncate = len(master_accumulator) - epoch_start_at
            raise Exception(
                f"Pass `--truncate` to overwrite history.  This will erase "
                f"{num_to_truncate}"
            )
        logger.info(
            "Deleting %d previous accumulators from epoch #%d - %d",
            len(master_accumulator) - epoch_start_at,
            epoch_start_at,
            len(master_accumulator) - 1,
        )
        for epoch in range(epoch_start_at, len(master_accumulator)):
            try:
                content_storage.delete_content(epoch_accumulator_key(epoch))
            except ContentNotFound:
                logger.debug("missing epoch accumulator for deletion: epoch={epoch}")
            else:
                logger.debug("deleted epoch accumulator: epoch={epoch}")

            try:
                content_storage.delete_content(master_accumulator_key(epoch))
            except ContentNotFound:
                logger.debug("missing master accumulator for deletion: epoch={epoch}")
            else:
                logger.debug("deleted master accumulator: epoch={epoch}")

        if len(master_accumulator) > 1:
            latest_master_epoch = len(master_accumulator) - 1
            master_accumulator_data = content_storage.get_content(
                master_accumulator_key(latest_master_epoch)
            )
            master_accumulator = ssz.decode(
                master_accumulator_data, sedes=MasterAccumulatorSedes,
            )
            content_storage.set_content(
                LATEST_MASTER_ACCUMULATOR_KEY, master_accumulator_data, exists_ok=True,
            )
        else:
            assert epoch_start_at == 0
            master_accumulator = []

    if epoch_end_at is None:
        latest_block_number = w3.eth.blockNumber
        epoch_end_at = latest_block_number // EPOCH_SIZE

    logger.info(
        "Starting exfiltration of epochs #%d -> %d", epoch_start_at, epoch_end_at
    )

    if epoch_start_at == 0:
        master_accumulator = []
        previous_total_difficulty = 0
    elif epoch_start_at == len(master_accumulator):
        prevous_epoch_accumulator_data = content_storage.get_content(
            epoch_accumulator_key(epoch_start_at - 1)
        )
        prevous_epoch_accumulator = ssz.decode(
            prevous_epoch_accumulator_data, sedes=EpochAccumulatorSedes,
        )
        previous_total_difficulty = prevous_epoch_accumulator[-1][1]
    else:
        raise Exception("Invariant")

    for epoch in range(epoch_start_at, epoch_end_at):
        logger.debug("starting: epoch=%d", epoch)

        headers = await retrieve_epoch_headers(w3, epoch)
        for header in headers:
            content_storage.set_content(
                header_key(header.hash), rlp.encode(header),
            )

        epoch_accumulator = compute_epoch_accumulator(
            headers, previous_total_difficulty
        )
        assert len(epoch_accumulator) == EPOCH_SIZE
        epoch_accumulator_root = ssz.get_hash_tree_root(
            epoch_accumulator, sedes=EpochAccumulatorSedes,
        )
        content_storage.set_content(
            epoch_accumulator_key(epoch),
            ssz.encode(epoch_accumulator, sedes=EpochAccumulatorSedes),
        )

        master_accumulator.append(epoch_accumulator_root)
        master_accumulator_root = ssz.get_hash_tree_root(
            master_accumulator, sedes=MasterAccumulatorSedes,
        )

        # set the accumulator for the epoch
        master_accumulator_data = ssz.encode(
            master_accumulator, sedes=MasterAccumulatorSedes
        )
        content_storage.set_content(
            master_accumulator_key(epoch), master_accumulator_data
        )
        content_storage.set_content(
            LATEST_MASTER_ACCUMULATOR_KEY, master_accumulator_data, exists_ok=True,
        )

        logger.info(
            "completed: epoch=%d blocks=#%d-%d  epoch_root=%s  master_root=%s",
            epoch,
            epoch * EPOCH_SIZE,
            epoch * EPOCH_SIZE + EPOCH_SIZE,
            epoch_accumulator_root.hex(),
            master_accumulator_root.hex(),
        )
        previous_total_difficulty = epoch_accumulator[-1].total_difficulty

    logger.info("Starting exfiltration")


async def retrieve_epoch_headers(w3: Web3, epoch: int) -> Tuple[BlockHeader, ...]:
    start_at = epoch * EPOCH_SIZE
    end_at = (epoch + 1) * EPOCH_SIZE

    (send_channel, receive_channel,) = trio.open_memory_channel[BlockHeader](32)

    async with trio.open_nursery() as nursery:
        nursery.start_soon(retrieve_headers, w3, start_at, end_at, send_channel)
        async with receive_channel:
            headers = tuple(sorted([header async for header in receive_channel]))
            logger.debug("Got headers for epoch #%d", epoch)
            return headers


async def retrieve_headers(
    w3: Web3,
    start_at: int,
    end_at: int,
    send_channel: trio.abc.SendChannel[BlockHeader],
    request_rate: int = 3,
) -> None:
    semaphor = trio.Semaphore(request_rate, max_value=request_rate)

    async def _fetch(block_number: int) -> None:
        header = await retrieve_header(w3, block_number)
        semaphor.release()
        await send_channel.send(header)

    async with send_channel:
        async with trio.open_nursery() as nursery:
            logger.debug("Starting retrieval of headers %d-%d", start_at, end_at)
            for block_number in range(start_at, end_at):
                await semaphor.acquire()
                nursery.start_soon(_fetch, block_number)


async def retrieve_header(w3: Web3, block_number: int) -> BlockHeader:
    logger.debug("Retrieving header #%d", block_number)
    w3_header = await trio.to_thread.run_sync(w3.eth.getBlock, block_number)
    header = BlockHeader(
        difficulty=w3_header["difficulty"],
        block_number=w3_header["number"],
        gas_limit=w3_header["gasLimit"],
        timestamp=w3_header["timestamp"],
        coinbase=to_canonical_address(w3_header["miner"]),
        parent_hash=Hash32(w3_header["parentHash"]),
        uncles_hash=Hash32(w3_header["sha3Uncles"]),
        state_root=Hash32(w3_header["stateRoot"]),
        transaction_root=Hash32(w3_header["transactionsRoot"]),
        receipt_root=Hash32(w3_header["receiptsRoot"]),
        bloom=big_endian_to_int(bytes(w3_header["logsBloom"])),
        gas_used=w3_header["gasUsed"],
        extra_data=bytes(w3_header["extraData"]),
        mix_hash=Hash32(w3_header["mixHash"]),
        nonce=bytes(w3_header["nonce"]),
    )
    if header.hash != Hash32(w3_header["hash"]):
        raise ValueError(
            f"Reconstructed header hash does not match expected: "
            f"expected={encode_hex(w3_header['hash'])}  actual={header.hex_hash}"
        )
    return header


parser = argparse.ArgumentParser(description="Block Header Exfiltration")
parser.add_argument("--start-epoch", type=int)
parser.add_argument("--end-epoch", type=int)
parser.add_argument("--log-level", type=int, default=logging.INFO)


LOGGERS_TO_MAKE_QUIET = (
    ("web3.providers.IPCProvider", logging.INFO),
    ("web3.RequestManager", logging.INFO),
)


if __name__ == "__main__":
    args = parser.parse_args()

    logger = logging.getLogger()
    logger.setLevel(logging.INFO if args.log_level is None else args.log_level)

    for logger_path, level in LOGGERS_TO_MAKE_QUIET:
        logger = logging.getLogger(logger_path)
        logger.setLevel(level)

    setup_stderr_logging(args.log_level)
    from web3.auto.ipc import w3

    trio.run(
        exfiltrate_header_chain, w3, args.start_epoch, args.end_epoch,
    )
