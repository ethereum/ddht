from ddht.kademlia import Address, check_relayed_addr
from ddht.tools.factories.kademlia import AddressFactory


def test_check_relayed_addr():
    public_host = Address("8.8.8.8", 80, 80)
    local_host = Address("127.0.0.1", 80, 80)
    assert check_relayed_addr(local_host, local_host)
    assert not check_relayed_addr(public_host, local_host)

    private = Address("192.168.1.1", 80, 80)
    assert check_relayed_addr(private, private)
    assert not check_relayed_addr(public_host, private)

    reserved = Address("240.0.0.1", 80, 80)
    assert not check_relayed_addr(local_host, reserved)
    assert not check_relayed_addr(public_host, reserved)

    unspecified = Address("0.0.0.0", 80, 80)
    assert not check_relayed_addr(local_host, unspecified)
    assert not check_relayed_addr(public_host, unspecified)
