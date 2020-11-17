import datetime
import operator
from typing import Iterable, NamedTuple, Optional, Sequence, Tuple

from eth_keys import keys
from eth_keys.exceptions import BadSignature
from eth_typing import Hash32, NodeID
from eth_utils import keccak, to_tuple
from eth_utils.toolz import accumulate, cons, sliding_window

from ddht.v5_1.alexandria.content import content_key_to_content_id
from ddht.v5_1.alexandria.typing import ContentID, ContentKey

ADVERTISEMENT_DOMAIN = b"alexandria-advertisement"


def create_advertisement_signing_message(
    content_id: ContentID, hash_tree_root: Hash32, expires_at: datetime.datetime,
) -> bytes:
    return b":".join(
        (
            ADVERTISEMENT_DOMAIN,
            content_id,
            hash_tree_root,
            int(expires_at.timestamp()).to_bytes(5, "big"),
        )
    )


def create_advertisement_signature(
    content_id: ContentID,
    hash_tree_root: Hash32,
    expires_at: datetime.datetime,
    private_key: keys.PrivateKey,
) -> keys.Signature:
    signing_message = create_advertisement_signing_message(
        content_id, hash_tree_root, expires_at
    )
    return private_key.sign_msg(signing_message)


def verify_advertisement_signature(
    content_id: ContentID,
    hash_tree_root: Hash32,
    expires_at: datetime.datetime,
    signature: keys.Signature,
    public_key: keys.PublicKey,
) -> None:
    signing_message = create_advertisement_signing_message(
        content_id, hash_tree_root, expires_at
    )
    if not public_key.verify_msg(signing_message, signature):
        raise BadSignature("Signature does not validate")


ONE_DAY = datetime.timedelta(seconds=60 * 60 * 24)


class Advertisement(NamedTuple):
    content_key: ContentKey
    hash_tree_root: Hash32
    expires_at: datetime.datetime

    signature_v: int
    signature_r: int
    signature_s: int

    @property
    def content_id(self) -> ContentID:
        return content_key_to_content_id(self.content_key)

    @property
    def signature(self) -> keys.Signature:
        return keys.Signature(
            vrs=(self.signature_v, self.signature_r, self.signature_s)
        )

    @property
    def signing_msg(self) -> bytes:
        return create_advertisement_signing_message(
            self.content_id, self.hash_tree_root, self.expires_at,
        )

    @property
    def is_expired(self) -> bool:
        return datetime.datetime.utcnow() > self.expires_at

    @property
    def public_key(self) -> keys.PublicKey:
        return self.signature.recover_public_key_from_msg(self.signing_msg)

    @property
    def node_id(self) -> NodeID:
        return NodeID(keccak(self.public_key.to_bytes()))

    @classmethod
    def create(
        cls,
        content_key: ContentKey,
        hash_tree_root: Hash32,
        private_key: keys.PrivateKey,
        expires_at: Optional[datetime.datetime] = None,
    ) -> "Advertisement":
        if expires_at is None:
            expires_at = datetime.datetime.utcnow().replace(microsecond=0) + ONE_DAY
        content_id = content_key_to_content_id(content_key)
        signature = create_advertisement_signature(
            content_id, hash_tree_root, expires_at, private_key,
        )
        return cls(
            content_key=content_key,
            hash_tree_root=hash_tree_root,
            expires_at=expires_at,
            signature_v=signature.v,
            signature_r=signature.r,
            signature_s=signature.s,
        )

    @property
    def is_valid(self) -> bool:
        try:
            self.node_id
        except BadSignature:
            return False
        else:
            return True

    def verify(self) -> None:
        verify_advertisement_signature(
            content_id=self.content_id,
            hash_tree_root=self.hash_tree_root,
            expires_at=self.expires_at,
            signature=self.signature,
            public_key=self.public_key,
        )

    def to_sedes_payload(self) -> Tuple[ContentKey, Hash32, int, int, int, int]:
        return (
            self.content_key,
            self.hash_tree_root,
            int(self.expires_at.timestamp()),
            self.signature_v,
            self.signature_r,
            self.signature_s,
        )

    @classmethod
    def from_sedes_payload(
        cls, payload: Tuple[ContentKey, Hash32, int, int, int, int],
    ) -> "Advertisement":
        (
            content_key,
            hash_tree_root,
            expires_at_timestamp,
            signature_v,
            signature_r,
            signature_s,
        ) = payload
        expires_at = datetime.datetime.fromtimestamp(expires_at_timestamp)
        return cls(
            content_key,
            hash_tree_root,
            expires_at,
            signature_v,
            signature_r,
            signature_s,
        )


ADVERTISEMENT_FIXED_SIZE = sum(
    (
        4,  # length prefix for the whole advertisement
        4,  # length prefix for `content_key`
        65,  # signature
        5,  # expiration
        32,  # hash_tree_root
    )
)


def _get_partition_indices(
    encoded_sizes: Sequence[int], max_payload_size: int
) -> Iterable[int]:
    cumulative_sizes = accumulate(operator.add, encoded_sizes)
    offset = 0
    yield 0
    for idx, (last_size, size) in enumerate(
        sliding_window(2, cons(0, cumulative_sizes))
    ):
        if size - offset > max_payload_size:
            offset = last_size
            yield idx
    yield len(encoded_sizes)


@to_tuple
def partition_advertisements(
    advertisements: Sequence[Advertisement], max_payload_size: int,
) -> Iterable[Tuple[Advertisement, ...]]:
    encoded_sizes = tuple(
        len(advertisement.content_key) + ADVERTISEMENT_FIXED_SIZE
        for advertisement in advertisements
    )
    partition_indices = _get_partition_indices(encoded_sizes, max_payload_size)
    for left, right in sliding_window(2, partition_indices):
        yield tuple(advertisements[left:right])
