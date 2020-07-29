import logging
from typing import Optional

from eth.abc import DatabaseAPI
from eth_utils.encoding import big_endian_to_int, int_to_big_endian
import rlp

from ddht.abc import NodeDBAPI
from ddht.enr import ENR
from ddht.exceptions import OldSequenceNumber
from ddht.identity_schemes import IdentitySchemeRegistry
from ddht.typing import NodeID


class NodeDB(NodeDBAPI):
    logger = logging.getLogger("ddht.NodeDB")

    def __init__(
        self, identity_scheme_registry: IdentitySchemeRegistry, db: DatabaseAPI
    ) -> None:
        self.db = db
        self._identity_scheme_registry = identity_scheme_registry

    @property
    def identity_scheme_registry(self) -> IdentitySchemeRegistry:
        return self._identity_scheme_registry

    def validate_identity_scheme(self, enr: ENR) -> None:
        """
        Check that we know the identity scheme of the ENR.

        This check should be performed whenever an ENR is inserted or updated in serialized form to
        make sure retrieving it at a later time will succeed (deserializing the ENR would fail if
        we don't know the identity scheme).
        """
        if enr.identity_scheme.id not in self.identity_scheme_registry:
            raise ValueError(
                f"ENRs identity scheme with id {enr.identity_scheme.id!r} unknown to ENR DBs "
                f"identity scheme registry"
            )

    def set_enr(self, enr: ENR) -> None:
        existing_enr: Optional[ENR]
        self.validate_identity_scheme(enr)
        try:
            existing_enr = self.get_enr(enr.node_id)
        except KeyError:
            existing_enr = None
        if existing_enr and existing_enr.sequence_number > enr.sequence_number:
            raise OldSequenceNumber(
                f"Cannot overwrite existing ENR ({existing_enr.sequence_number}) with old one "
                f"({enr.sequence_number})"
            )
        self.db.set(self._get_enr_key(enr.node_id), rlp.encode(enr))

    def get_enr(self, node_id: NodeID) -> ENR:
        return rlp.decode(self.db[self._get_enr_key(node_id)], sedes=ENR)  # type: ignore

    def delete_enr(self, node_id: NodeID) -> None:
        del self.db[self._get_enr_key(node_id)]

    def set_last_pong_time(self, node_id: NodeID, last_pong: int) -> None:
        self.db.set(self._get_last_pong_time_key(node_id), int_to_big_endian(last_pong))

    def get_last_pong_time(self, node_id: NodeID) -> int:
        return big_endian_to_int(self.db[self._get_last_pong_time_key(node_id)])

    def delete_last_pong_time(self, node_id: NodeID) -> None:
        del self.db[self._get_last_pong_time_key(node_id)]

    def _get_enr_key(self, node_id: NodeID) -> bytes:
        return bytes(node_id) + b":enr"

    def _get_last_pong_time_key(self, node_id: NodeID) -> bytes:
        return bytes(node_id) + b":last_pong_time"
