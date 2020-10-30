import hashlib
import logging
import sys
import time
from typing import List, Tuple

from ddht.v5_1.alexandria.partials.proof import compute_proof
from ddht.v5_1.alexandria.sedes import content_sedes

try:
    import texttable
except ImportError as err:
    raise ImportError(
        "The `texttable` python library is required to use the benchmarking "
        "suite.  Installable usin `pip install ddht[benchmark]`"
    ) from err


MB = 1024 * 1024


logger = logging.getLogger("ddht.benchmarks")


SEGMENTS = (
    ("32b-first", (0, 32)),
    ("32b-offset-1c", (32, 32)),
    ("32b-offset-4c", (128, 32)),
    ("64b-first", (0, 64)),
    ("64b-offset-1c", (32, 64)),
    ("64b-offset-4c", (128, 64)),
    ("128b-first", (0, 128)),
    ("128b-offset-1c", (32, 128)),
    ("128b-offset-4c", (128, 128)),
    ("256b-first", (0, 256)),
    ("256b-offset-1c", (32, 256)),
    ("256b-offset-4c", (128, 256)),
    ("512b-first", (0, 512)),
    ("512b-offset-1c", (32, 512)),
    ("512b-offset-4c", (128, 512)),
    ("768b-first", (0, 768)),
    ("768b-offset-1c", (32, 768)),
    ("768b-offset-4c", (128, 768)),
    ("1kb-first", (0, 1024)),
    ("1kb-offset-1c", (32, 1024)),
    ("1kb-offset-4c", (128, 1024)),
)


def benchmark_serialization(
    benchmark_name: str,
    content: bytes,
    segments: Tuple[Tuple[str, Tuple[int, int]], ...],
) -> None:
    full_proof = compute_proof(content, sedes=content_sedes)

    proofs = tuple(
        full_proof.to_partial(
            start_at=start_at, partial_data_length=partial_data_length
        )
        for (name, (start_at, partial_data_length)) in segments
    )
    serialized_proofs = tuple(proof.serialize() for proof in proofs)

    sizes = tuple(len(serialized_proof) for serialized_proof in serialized_proofs)

    table_header = ("name", "start", "end", "length", "size")
    table_rows = tuple(
        (name, start_at, start_at + partial_data_length, partial_data_length, size)
        for ((name, (start_at, partial_data_length)), size) in zip(segments, sizes)
    )

    table = texttable.Texttable()
    table.set_cols_align(("l", "r", "r", "r", "r"))
    table.header(table_header)
    table.add_rows(table_rows, header=False)

    logger.info("\n##########################")
    logger.info(f"benchmark: {benchmark_name}")
    logger.info("##########################\n")
    logger.info(table.draw())


CONTENT_SIZES = (
    ("0b", 0, 1000),
    ("128b", 128, 1000),
    ("256b", 256, 1000),
    ("512b", 512, 1000),
    ("1kb", 1024, 500),
    ("2kb", 2048, 300),
    ("4kb", 4096, 200),
    ("10kb", 10240, 100),
    ("20kb", 20480, 70),
    ("100kb", 102400, 20),
)


def benchmark_full_construction(sizes: Tuple[Tuple[str, int, int], ...],) -> None:
    base_content_length = max(size for name, size, iteration_count in sizes)
    base_content = b"".join(
        (
            hashlib.sha256(i.to_bytes(32, "big")).digest()
            for i in range((base_content_length + 31) // 32)
        )
    )

    proof_performances: List[Tuple[float, int]] = []
    for name, size, iteration_count in sizes:
        content = base_content[:size]
        start_at = time.monotonic()
        for _ in range(iteration_count):
            compute_proof(content, sedes=content_sedes)
        end_at = time.monotonic()
        proof_performances.append((end_at - start_at, iteration_count))

    proofs_per_second = tuple(
        iteration_count / elapsed for (elapsed, iteration_count) in proof_performances
    )

    table_header = ("name", "size", "elapsed", "proofs/sec", "iterations")
    table_rows = tuple(
        (name, size, elapsed, rate, iteration_count)
        for ((name, size, iteration_count), (elapsed, _), rate) in zip(
            sizes, proof_performances, proofs_per_second
        )
    )

    table = texttable.Texttable()
    table.set_cols_align(("l", "r", "r", "r", "r"))
    table.header(table_header)
    table.add_rows(table_rows, header=False)

    logger.info("\n######################################")
    logger.info("benchmark: Proof Generation Speed")
    logger.info("######################################\n")
    logger.info(table.draw())


def do_benchmarks() -> None:
    content_1mb = b"".join(
        (hashlib.sha256(i.to_bytes(32, "big")).digest() for i in range(MB // 32))
    )
    benchmark_serialization("1MB", content_1mb, SEGMENTS)

    benchmark_full_construction(CONTENT_SIZES)


if __name__ == "__main__":
    handler_stream = logging.StreamHandler(sys.stderr)
    handler_stream.setLevel(logging.INFO)

    logger.setLevel(logging.INFO)
    logger.addHandler(handler_stream)

    do_benchmarks()
