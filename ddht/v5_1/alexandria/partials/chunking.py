import itertools
from typing import Iterable, List, Sequence, Tuple

from eth_typing import Hash32
from eth_utils import to_tuple
from eth_utils.toolz import sliding_window
from ssz.constants import CHUNK_SIZE, ZERO_BYTES32

from ddht.v5_1.alexandria.constants import GB, POWERS_OF_TWO
from ddht.v5_1.alexandria.partials.typing import TreePath


@to_tuple
def compute_chunks(data: bytes) -> Iterable[Hash32]:
    """
    An optimized version of SSZ chunking specifically for byte
    strings.

    Takes the data and splits it into chunks of length 32.  The last chunk is
    padded out to 32 bytes with null bytes `0x00` if it is not full.
    """
    if not data:
        yield ZERO_BYTES32
        return
    elif len(data) > GB:
        raise Exception("too big")
    data_length = len(data)
    if data_length % CHUNK_SIZE == 0:
        padded_data = data
    else:
        padding_byte_count = CHUNK_SIZE - data_length % CHUNK_SIZE
        padded_data = data + b"\x00" * padding_byte_count

    padded_length = len(padded_data)
    for left_boundary, right_boundary in sliding_window(
        2, range(0, padded_length + 1, CHUNK_SIZE)
    ):
        yield Hash32(padded_data[left_boundary:right_boundary])


@to_tuple
def chunk_index_to_path(index: int, path_bit_size: int) -> Iterable[bool]:
    """
    Given a chunk index, convert it to the path into the binary tree where the
    chunk is located.
    """
    for power_of_two in reversed(POWERS_OF_TWO[:path_bit_size]):
        yield bool(index & power_of_two)


def path_to_left_chunk_index(path: TreePath, path_bit_size: int) -> int:
    """
    Given a path, convert it to a chunk index.  In the case where the path is
    to an intermediate tree node, return the chunk index on the leftmost branch
    of the subtree.
    """
    return sum(
        power_of_two
        for path_bit, power_of_two in itertools.zip_longest(
            path, reversed(POWERS_OF_TWO[:path_bit_size]), fillvalue=False,
        )
        if path_bit
    )


@to_tuple
def group_by_subtree(
    first_chunk_index: int, num_chunks: int
) -> Iterable[Tuple[int, ...]]:
    r"""
    Group the paths into groups that belong to the same subtree.

    This helper function is used when constructing partial proofs. After
    setting aside the leaf nodes that correspond to the data we wish to prove,
    the remaining leaf nodes can be replaced by the hashes of the largest
    subtrees that contains them.

    Given a 4-bit tree like this

    0:                           0
                                / \
                              /     \
                            /         \
                          /             \
                        /                 \
                      /                     \
                    /                         \
    1:             0                           1
                 /   \                       /   \
               /       \                   /       \
             /           \               /           \
    2:      0             1             0             1
          /   \         /   \         /   \         /   \
    3:   0     1       0     1       0     1       0     1
        / \   / \     / \   / \     / \   / \     / \   / \
    4: A   B C   D   E   F G   H   I   J K   L   M   N O   P

                |-------------------------|

    If we are given the chunks D-K which map to indices 3-10 we want them
    divided up into the largest subgroups that all belong in the same subtree.

    2:      0             1             0             1
          /   \         /   \         /   \         /   \
    3:   0     1       0     1       0     1       0     1
        / \   / \     / \   / \     / \   / \     / \   / \
    4: A   B C   D   E   F G   H   I   J K   L   M   N O   P
                |-| |-----------| |-----|-|

    These groups are

    - D
    - E, F, G, H
    - I, J
    - K

    The algorithm for doing this is as follows:

    1) Find the largest power of 2 that can fit into the chunk range.  This is
    referred to as the `group_size`.  For this example the number is 8 since we
    have a span of 8 items.  Divide the range up into groups aligned with this
    value.


    3:   0     1       0     1       0     1       0     1
        / \   / \     / \   / \     / \   / \     / \   / \
    4: A   B C   D   E   F G   H   I   J K   L   M   N O   P

                |-------------------------|
                (D, E, F, G, H)    (I, J, K)
       <=========(0-7)=========>   <=========8-16==========>

    2) Any group that is the full lenght (in this case 8) is final.  All groups
    that are not full move onto the next round. In this case none of the groups
    are final.

    3) Now we change our group size to the previous power of two which is 4 in
    this case. Again we divide the range up into groups of this size.


    3:   0     1       0     1       0     1       0     1
        / \   / \     / \   / \     / \   / \     / \   / \
    4: A   B C   D   E   F G   H   I   J K   L   M   N O   P

                |-------------------------|
                (D)  (E, F, G, H)  (I, J, K)
       <==(0-3)==>   <==(4-7)==>   <==(8-11)=>   <=(12-15)=>

    4) In this case the group `(E, F, G, H)` is full so it moves into the
    *final* category, leaving the range (D,) and (I, J, K) for the next round
    which uses 2 as the group size.

    3:   0     1       0     1       0     1       0     1
        / \   / \     / \   / \     / \   / \     / \   / \
    4: A   B C   D   E   F G   H   I   J K   L   M   N O   P

                |-------------------------|
                (D)                (I, J) (K)
       <0-1> <2-3>   <4-5> <6-7>   <8-9> <10-11> ...

    5) At group size 2 we finalize (I, J). All remaining groups will finalize
    at group size 1.

    """
    last_chunk_index = first_chunk_index + num_chunks

    chunk_indices = tuple(range(first_chunk_index, last_chunk_index))
    chunks_to_process: Sequence[Tuple[int, ...]] = (chunk_indices,)

    # The largest power of two that could fit between the chunk range.
    max_bucket_bit_size = num_chunks.bit_length() - 1

    final_groups: List[Tuple[int, ...]] = []

    # Now we iterate downwards through the powers of two, splitting each group
    # up by the bucket boundaries at that level.  Any full buckets are
    # considered final, leaving the remaining ranges for smaller buckets.  The
    # final bucket size of `1` acts as a catch all for any ranges that cannot
    # be grouped.
    for bucket_bit_size in range(max_bucket_bit_size, -1, -1):
        if not chunks_to_process:
            break

        next_chunks_to_process: List[Tuple[int, ...]] = []

        for chunk in chunks_to_process:
            chunk_start_index = chunk[0]
            chunk_end_index = chunk[-1]

            bucket_size = 2 ** bucket_bit_size

            # Compute the start and end indices for the buckets at this bucket size.
            bucket_start_at = chunk_start_index - (chunk_start_index % bucket_size)
            bucket_end_at = chunk_end_index + (
                bucket_size - chunk_end_index % bucket_size
            )

            # Split the chunk up into groups aligned with the buckets at this
            # level.
            group_candidates = tuple(
                chunk[
                    max(0, start_at - chunk_start_index) : start_at
                    - chunk_start_index
                    + bucket_size
                ]  # noqa: E501
                for start_at in range(bucket_start_at, bucket_end_at + 1, bucket_size)
            )

            # Any groups that are "full" are final.
            final_groups.extend(
                tuple(group for group in group_candidates if len(group) == bucket_size)
            )

            # All remaining groups move onto the next round (filtering out
            # empty groups that occur due to how they are sliced)
            next_chunks_to_process.extend(
                filter(
                    bool,
                    (group for group in group_candidates if len(group) < bucket_size),
                )
            )
        chunks_to_process = next_chunks_to_process

    return tuple(sorted(final_groups))
