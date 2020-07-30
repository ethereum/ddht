from async_service import Service

from ddht.abc import NodeDBAPI
from ddht.enr import ENR
from ddht.exceptions import OldSequenceNumber

def get_local_enr(
    boot_info: BootInfo,
    node_db: NodeDBAPI,
    local_private_key: keys.PrivateKey,
    local_ip_address: Optional[AnyIPAddress],
) -> ENR:
    kv_pairs = {
        b"id": b"v4",
        b"secp256k1": local_private_key.public_key.to_compressed_bytes(),
        b"udp": boot_info.port,
    }
    if local_ip_address is not None:
        kv_pairs[IP_V4_ADDRESS_ENR_KEY] = local_ip_address.packed
    minimal_enr = UnsignedENR(
        sequence_number=1,
        kv_pairs=kv_pairs,
        identity_scheme_registry=default_identity_scheme_registry,
    ).to_signed_enr(local_private_key.to_bytes())
    node_id = minimal_enr.node_id

    try:
        base_enr = node_db.get_enr(node_id)
    except KeyError:
        logger.info(f"No Node for {encode_hex(node_id)} found, creating new one")
        return minimal_enr
    else:
        if any(
            key not in base_enr or base_enr[key] != value
            for key, value in minimal_enr.items()
        ):
            logger.debug("Updating local ENR")
            return UnsignedENR(
                sequence_number=base_enr.sequence_number + 1,
                kv_pairs=merge(dict(base_enr), dict(minimal_enr)),
                identity_scheme_registry=default_identity_scheme_registry,
            ).to_signed_enr(local_private_key.to_bytes())
        else:
            return base_enr

class ENRManager(Service):
    _node_db: NodeDBAPI
    _enr: ENR

    def __init__(self,
                 node_db: NodeDBAPI,
                 base_enr: ENR) -> None:
        self._node_db = node_db

        try:
            self._node_db.set_enr(base_enr)

    def update(self, *kv_pair: Tuple[bytes, bytes]) ->
