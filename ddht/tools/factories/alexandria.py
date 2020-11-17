import datetime
import secrets
from typing import Any

import factory

from ddht.tools.factories.keys import PrivateKeyFactory
from ddht.v5_1.alexandria.advertisements import (
    Advertisement,
    create_advertisement_signature,
)
from ddht.v5_1.alexandria.content import content_key_to_content_id

ONE_HOUR = datetime.timedelta(seconds=60 * 60)


class AdvertisementFactory(factory.Factory):  # type: ignore
    content_key = factory.LazyFunction(
        lambda: secrets.token_bytes(33 + secrets.randbelow(127))
    )
    hash_tree_root = factory.LazyFunction(lambda: secrets.token_bytes(32))
    expires_at = factory.LazyFunction(
        lambda: datetime.datetime.utcnow().replace(microsecond=0) + ONE_HOUR
    )

    signature_v = factory.LazyAttribute(lambda o: o.signature.v)
    signature_r = factory.LazyAttribute(lambda o: o.signature.r)
    signature_s = factory.LazyAttribute(lambda o: o.signature.s)

    class Params:
        private_key = factory.SubFactory(PrivateKeyFactory)
        signature = factory.LazyAttribute(
            lambda o: create_advertisement_signature(
                content_id=content_key_to_content_id(o.content_key),
                hash_tree_root=o.hash_tree_root,
                expires_at=o.expires_at,
                private_key=o.private_key,
            )
        )

    class Meta:
        model = Advertisement

    @classmethod
    def expired(cls, **kwargs: Any) -> "Advertisement":
        assert "expires_at" not in kwargs
        expires_at = datetime.datetime.utcnow().replace(microsecond=0) - ONE_HOUR
        return cls(**kwargs, expires_at=expires_at)
