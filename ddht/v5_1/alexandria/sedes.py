from ssz.sedes import Container, uint32

PingSedes = Container(field_sedes=(uint32,))
PongSedes = Container(field_sedes=(uint32,))
