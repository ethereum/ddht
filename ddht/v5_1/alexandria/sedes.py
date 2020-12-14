from ssz.sedes import (
    Container,
    List,
    UInt,
    boolean,
    bytes32,
    uint8,
    uint16,
    uint32,
    uint256,
)

from ddht.v5_1.alexandria.constants import GB


class ByteList(List):  # type: ignore
    def __init__(self, max_length: int) -> None:
        super().__init__(element_sedes=uint8, max_length=max_length)

    def serialize(self, value: bytes) -> bytes:
        return value

    def deserialize(self, value: bytes) -> bytes:
        return value


byte_list = ByteList(max_length=2048)
uint40 = UInt(40)
content_key_sedes = ByteList(max_length=256)


PingSedes = Container(field_sedes=(uint32, uint256))
PongSedes = Container(field_sedes=(uint32, uint256))

FindNodesSedes = Container(field_sedes=(List(uint16, max_length=256),))
FoundNodesSedes = Container(field_sedes=(uint8, List(byte_list, max_length=32)))

GetContentSedes = Container(field_sedes=(byte_list, uint32, uint16))
ContentSedes = Container(field_sedes=(boolean, byte_list,))

AdvertisementSedes = Container(
    field_sedes=(byte_list, bytes32, uint40, uint8, uint256, uint256)
)
AdvertiseSedes = List(AdvertisementSedes, max_length=32)
AckSedes = Container(field_sedes=(uint256, List(boolean, max_length=32)))

LocateSedes = Container(field_sedes=(content_key_sedes,))
LocationsSedes = Container(field_sedes=(uint8, List(AdvertisementSedes, max_length=32)))

# sedes used for encoding alexandria content
content_sedes = ByteList(max_length=GB)
