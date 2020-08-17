from typing import Any, Dict

from eth_keys import keys
from eth_utils.toolz import merge
import factory

from ddht.enr import ENR, UnsignedENR
from ddht.identity_schemes import V4IdentityScheme

from .kademlia import AddressFactory


class ENRFactory(factory.Factory):  # type: ignore
    class Meta:
        model = ENR

    sequence_number = factory.Faker("pyint", min_value=0, max_value=100)
    kv_pairs = factory.LazyAttribute(
        lambda o: merge(
            {
                b"id": b"v4",
                b"secp256k1": keys.PrivateKey(
                    o.private_key
                ).public_key.to_compressed_bytes(),
                b"ip": o.address.ip_packed,
                b"udp": o.address.udp_port,
                b"tcp": o.address.tcp_port,
            },
            o.custom_kv_pairs,
        )
    )
    signature = factory.LazyAttribute(
        lambda o: UnsignedENR(o.sequence_number, o.kv_pairs)
        .to_signed_enr(o.private_key)
        .signature
    )

    class Params:
        private_key = factory.Faker("binary", length=V4IdentityScheme.private_key_size)
        address = factory.SubFactory(AddressFactory)
        custom_kv_pairs: Dict[bytes, Any] = {}
