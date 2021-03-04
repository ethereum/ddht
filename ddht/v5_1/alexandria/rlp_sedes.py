from rlp import sedes

address = sedes.Binary.fixed_length(20, allow_empty=True)
hash32 = sedes.Binary.fixed_length(32)
uint32 = sedes.BigEndianInt(32)
uint256 = sedes.BigEndianInt(256)
