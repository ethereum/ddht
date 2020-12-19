import argparse
import logging
import pathlib
import tempfile
from typing import Callable, Optional, Sequence, Tuple

from eth_typing import Hash32
from eth_utils import big_endian_to_int, encode_hex, to_canonical_address
import rlp
import trio
from web3 import IPCProvider, Web3

from ddht.logging import setup_stderr_logging
from ddht.tools.w3_alexandria import AlexandriaModule
from ddht.v5_1.alexandria.content_storage import FileSystemContentStorage
from ddht.v5_1.alexandria.rlp_sedes import BlockHeader
from ddht.v5_1.alexandria.typing import ContentKey

logger = logging.getLogger("ddht.tools.exfiltration")


EPOCH_SIZE = 2048


def header_key(block_hash: Hash32) -> ContentKey:
    return ContentKey(b"\x01" + block_hash)


def get_content_storage_export_fn(
    storage_dir: pathlib.Path,
) -> Callable[[Sequence[BlockHeader]], None]:
    storage_dir.mkdir(parents=True, exist_ok=True)
    content_storage = FileSystemContentStorage(storage_dir)
    logger.info("ContentStorage: %s", storage_dir)

    def export_content_storage(headers: Sequence[BlockHeader]) -> None:
        for header in headers:
            content_storage.set_content(
                header_key(header.hash), rlp.encode(header),
            )

    return export_content_storage


def get_direct_w3_export_fn(
    ipc_path: pathlib.Path,
) -> Callable[[Sequence[BlockHeader]], None]:
    w3 = Web3(
        IPCProvider(ipc_path, timeout=30), modules={"alexandria": (AlexandriaModule,)}
    )

    def export_alexandria_w3(headers: Sequence[BlockHeader]) -> None:
        for header in headers:
            w3.alexandria.add_commons_content(
                header_key(header.hash), rlp.encode(header),
            )

    return export_alexandria_w3


async def exfiltrate_header_chain(
    w3: Web3,
    epoch_start_at: Optional[int],
    epoch_end_at: Optional[int],
    export_fn: Callable[[Sequence[BlockHeader]], None],
    truncate: bool = False,
) -> None:
    if epoch_start_at is None:
        epoch_start_at = 0

    if epoch_end_at is None:
        latest_block_number = w3.eth.blockNumber
        epoch_end_at = latest_block_number // EPOCH_SIZE

    logger.info(
        "Starting exfiltration of epochs #%d -> %d", epoch_start_at, epoch_end_at
    )

    for epoch in range(epoch_start_at, epoch_end_at):
        logger.debug("starting: epoch=%d", epoch)
        start_at = trio.current_time()

        headers = await retrieve_epoch_headers(w3, epoch)
        retrieval_end_at = trio.current_time()

        export_fn(headers)

        export_end_at = trio.current_time()

        elapsed_total = export_end_at - start_at
        elapsed_retrieve = retrieval_end_at - start_at
        elapsed_export = export_end_at - retrieval_end_at

        logger.info(
            "completed: epoch=%d blocks=#%d-%d  bps=%.1f (ret=%.1f  exp=%.2f)  "
            "elapsed=%.1fs (%.1f / %.1f)",
            epoch,
            epoch * EPOCH_SIZE,
            epoch * EPOCH_SIZE + EPOCH_SIZE,
            EPOCH_SIZE / elapsed_total,
            EPOCH_SIZE / elapsed_retrieve,
            EPOCH_SIZE / elapsed_export,
            elapsed_total,
            100 * elapsed_retrieve / elapsed_total,
            100 * elapsed_export / elapsed_total,
        )

    logger.info("Finished exfiltration")


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

export_parser = parser.add_mutually_exclusive_group()
export_parser.add_argument("--storage-dir", type=pathlib.Path)
export_parser.add_argument("--export-w3-ipc", type=pathlib.Path)


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

    export_fn: Callable[[Sequence[BlockHeader]], None]

    if args.storage_dir is not None:
        export_fn = get_content_storage_export_fn(args.storage_dir)
    elif args.export_w3_ipc is not None:
        export_fn = get_direct_w3_export_fn(args.export_w3_ipc)
    else:
        temp_storage_dir = pathlib.Path(tempfile.mkdtemp())
        export_fn = get_content_storage_export_fn(temp_storage_dir)

    trio.run(
        exfiltrate_header_chain, w3, args.start_epoch, args.end_epoch, export_fn,
    )
