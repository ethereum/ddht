import logging
from typing import Mapping, Optional

from eth_keys import keys
from eth_typing import NodeID
from eth_utils.toolz import merge
import trio

from ddht.abc import ENRManagerAPI, NodeDBAPI
from ddht.enr import ENR, UnsignedENR
from ddht.identity_schemes import (
    IdentitySchemeRegistry,
    default_identity_scheme_registry,
)
from ddht.typing import ENR_KV


class ENRManager(ENRManagerAPI):
    _node_db: NodeDBAPI
    _enr: ENR
    _node_id: NodeID
    _identity_scheme_registry: IdentitySchemeRegistry

    logger = logging.getLogger("ddht.ENRManager")

    def __init__(
        self,
        node_db: NodeDBAPI,
        private_key: keys.PrivateKey,
        kv_pairs: Optional[Mapping[bytes, bytes]] = None,
        identity_scheme_registry: IdentitySchemeRegistry = default_identity_scheme_registry,  # noqa: E501
    ) -> None:
        self._node_db = node_db
        self._identity_scheme_registry = identity_scheme_registry
        self._private_key = private_key
        self._send_channel, self._receive_channel = trio.open_memory_channel[ENR_KV](0)

        if kv_pairs is None:
            kv_pairs = {}

        if b"id" in kv_pairs:
            identity_kv_pairs = {}
        else:
            identity_kv_pairs = {
                b"id": b"v4",
                b"secp256k1": self._private_key.public_key.to_compressed_bytes(),
            }

        minimal_enr = UnsignedENR(
            sequence_number=1,
            kv_pairs=merge(identity_kv_pairs, kv_pairs),
            identity_scheme_registry=self._identity_scheme_registry,
        ).to_signed_enr(self._private_key.to_bytes())
        self._node_id = minimal_enr.node_id

        try:
            base_enr = node_db.get_enr(self._node_id)
        except KeyError:
            self.logger.info(
                "Local ENR created: seq=%d  enr=%r",
                minimal_enr.sequence_number,
                minimal_enr,
            )
            self._enr = minimal_enr
            node_db.set_enr(self._enr)
        else:
            self._enr = base_enr
            self.update(*tuple(dict(minimal_enr).items()))

    @property
    def enr(self) -> ENR:
        return self._enr

    def update(self, *kv_pairs: ENR_KV) -> ENR:
        if any(
            key not in self._enr or self._enr[key] != value for key, value in kv_pairs
        ):
            self._enr = UnsignedENR(
                sequence_number=self._enr.sequence_number + 1,
                kv_pairs=merge(dict(self._enr), dict(kv_pairs)),
                identity_scheme_registry=self._identity_scheme_registry,
            ).to_signed_enr(self._private_key.to_bytes())
            self._node_db.set_enr(self._enr)
            self.logger.info(
                "Local ENR Updated: seq=%d  enr=%r", self.enr.sequence_number, self.enr
            )
        return self._enr
