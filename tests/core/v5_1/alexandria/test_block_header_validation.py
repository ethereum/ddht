from eth_utils import ValidationError, decode_hex
import pytest
import rlp

from ddht.v5_1.alexandria.rlp_sedes import BlockHeader


@pytest.fixture
async def content_validator(alice_alexandria_network):
    return alice_alexandria_network.content_validator


@pytest.mark.trio
async def test_content_validator_unsupported_or_invalid_content_key(content_validator):
    with pytest.raises(ValidationError, match="Unsupported"):
        await content_validator.validate_content(
            b"\x00unicornsrainbowscupcakessparkles", b"dummy"
        )
    with pytest.raises(ValidationError, match="Invalid"):
        await content_validator.validate_content(b"", b"dummy")


#
# Block Header Validation
#
GENESIS_RLP = decode_hex(
    "f90214a000000000000000000000000000000000000000000000000000000000"
    "00000000a01dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142"
    "fd40d49347940000000000000000000000000000000000000000a0d7f8974fb5"
    "ac78d9ac099b9ad5018bedc2ce0a72dad1827a1709da30580f0544a056e81f17"
    "1bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a056e81f"
    "171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421b90100"
    "0000000000000000000000000000000000000000000000000000000000000000"
    "0000000000000000000000000000000000000000000000000000000000000000"
    "0000000000000000000000000000000000000000000000000000000000000000"
    "0000000000000000000000000000000000000000000000000000000000000000"
    "0000000000000000000000000000000000000000000000000000000000000000"
    "0000000000000000000000000000000000000000000000000000000000000000"
    "0000000000000000000000000000000000000000000000000000000000000000"
    "0000000000000000000000000000000000000000000000000000000000000000"
    "850400000000808213888080a011bbe8db4e347b4e8c937c1c8370e4b5ed33ad"
    "b3db69cbdb7a38e1e50b1b82faa0000000000000000000000000000000000000"
    "0000000000000000000000000000880000000000000042"
)
GENESIS = rlp.decode(GENESIS_RLP, sedes=BlockHeader)


@pytest.mark.trio
async def test_content_validator_block_header_hash_too_short(content_validator):
    with pytest.raises(ValidationError, match="Invalid block hash"):
        await content_validator.validate_content(b"\x01too_short", GENESIS)
    with pytest.raises(ValidationError, match="Invalid block hash"):
        await content_validator.validate_content(
            b"\x01too_long_too_long_too_long_too_long", GENESIS_RLP,
        )


@pytest.mark.trio
async def test_content_validator_block_header_invalid_rlp(content_validator):
    with pytest.raises(ValidationError, match="Invalid RLP"):
        await content_validator.validate_content(
            b"\x01unicornsrainbowscupcakessparkles", b"invalidrlp",
        )


@pytest.mark.trio
async def test_content_validator_block_header_wrong_block_hash(content_validator):
    with pytest.raises(ValidationError, match="Block hash mismatch"):
        await content_validator.validate_content(
            b"\x01unicornsrainbowscupcakessparkles", GENESIS_RLP,
        )


@pytest.mark.trio
async def test_content_validator_block_header_invalid_pow(content_validator):
    BAD_POW_HEADER = BlockHeader(*GENESIS[:-1], nonce=b"\x00" * 8)
    BAD_POW_HEADER_RLP = rlp.encode(BAD_POW_HEADER)
    with pytest.raises(ValidationError, match="Invalid POW"):
        await content_validator.validate_content(
            b"\x01" + BAD_POW_HEADER.hash, BAD_POW_HEADER_RLP,
        )
