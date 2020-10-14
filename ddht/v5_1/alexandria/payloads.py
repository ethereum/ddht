from typing import NamedTuple


class PingPayload(NamedTuple):
    enr_seq: int


class PongPayload(NamedTuple):
    enr_seq: int
