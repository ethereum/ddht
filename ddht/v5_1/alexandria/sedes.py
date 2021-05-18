from ssz.sedes import Container, List, uint8, uint16, uint64, uint256


class ByteList(List):  # type: ignore
    def __init__(self, max_length: int) -> None:
        super().__init__(element_sedes=uint8, max_length=max_length)

    def serialize(self, value: bytes) -> bytes:
        return value

    def deserialize(self, value: bytes) -> bytes:
        return value


byte_list = ByteList(max_length=2048)


PingSedes = Container(field_sedes=(uint64, uint256))
PongSedes = Container(field_sedes=(uint64, uint256))

FindNodesSedes = Container(field_sedes=(List(uint16, max_length=256),))
FoundNodesSedes = Container(field_sedes=(uint8, List(byte_list, max_length=32)))

FindContentSedes = Container(field_sedes=(byte_list,))
FoundContentSedes = Container(field_sedes=(List(byte_list, max_length=32), byte_list))
