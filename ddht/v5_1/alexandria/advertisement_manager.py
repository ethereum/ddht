import logging
import secrets

from async_service import Service
from eth_utils import ValidationError
from eth_utils.toolz import take
import ssz
from ssz.constants import CHUNK_SIZE
import trio

from ddht.base_message import InboundMessage
from ddht.event import Event
from ddht.v5_1.alexandria.abc import (
    AdvertisementDatabaseAPI,
    AdvertisementManagerAPI,
    AlexandriaNetworkAPI,
)
from ddht.v5_1.alexandria.content import compute_content_distance
from ddht.v5_1.alexandria.content_storage import ContentNotFound
from ddht.v5_1.alexandria.messages import AdvertiseMessage
from ddht.v5_1.alexandria.payloads import Advertisement
from ddht.v5_1.alexandria.sedes import content_sedes


class AdvertisementManager(Service, AdvertisementManagerAPI):
    logger = logging.getLogger("ddht.AdvertisementManager")

    def __init__(
        self, network: AlexandriaNetworkAPI, advertisement_db: AdvertisementDatabaseAPI,
    ) -> None:
        self._network = network
        self._advertisement_db = advertisement_db
        self._ready = trio.Event()

        self.new_advertisement = Event("new-advertisement")

    async def ready(self) -> None:
        await self._ready.wait()

    async def run(self) -> None:
        self.manager.run_daemon_task(self._periodically_purge_expired)
        self.manager.run_daemon_task(self._handle_advertisement_requests)

        await self.manager.wait_finished()

    async def _periodically_purge_expired(self) -> None:
        while True:
            await trio.lowlevel.checkpoint()

            # Purge in *small* chunks to avoid blocking too long
            to_purge = tuple(take(64, self._advertisement_db.expired()))
            if not to_purge:
                await trio.sleep(30)
                continue

            self.logger.debug("Purging: count=%d", len(to_purge))

            for advertisement in to_purge:
                self._advertisement_db.remove(advertisement)

    async def _handle_advertisement_requests(self) -> None:
        async with trio.open_nursery() as nursery:
            async with self._network.client.subscribe(AdvertiseMessage) as subscription:
                self._ready.set()
                async for request in subscription:
                    if not request.message.payload:
                        continue

                    nursery.start_soon(self._handle_request, request)

    def check_interest(self, advertisement: Advertisement) -> bool:
        if self._advertisement_db.exists(advertisement):
            return False

        distance_from_content = compute_content_distance(
            self._network.local_node_id, advertisement.content_id,
        )
        if distance_from_content > self._network.local_advertisement_radius:
            return False

        return True

    async def validate_advertisement(self, advertisement: Advertisement) -> None:
        if not advertisement.is_valid:
            raise ValidationError(
                f"Advertisement has invalid signature: signature={advertisement.signature}"
            )

        if advertisement.is_expired:
            raise ValidationError(
                f"Advertisement expired: advertisement={advertisement}"
            )

        if self._advertisement_db.exists(advertisement):
            raise ValidationError(
                f"Advertisement already known: advertisement={advertisement}"
            )

        # Verify that the `hash_tree_root` matches the known roots.
        known_hash_tree_roots = set(
            self._advertisement_db.get_hash_tree_roots_for_content_id(
                advertisement.content_id
            )
        )
        if len(known_hash_tree_roots) > 1:
            self.logger.error(
                "Multiple hash tree roots found: content_id=%s  roots=%s",
                advertisement.content_id.hex(),
                known_hash_tree_roots,
            )

        if (
            known_hash_tree_roots
            and advertisement.hash_tree_root not in known_hash_tree_roots
        ):
            # TODO: perform full validation of the content to determine the
            # correct root.
            raise NotImplementedError("TODO: resolve disccrepancy")

        if advertisement.node_id == self._network.local_node_id:
            await self._validate_local_advertisement(advertisement)
        else:
            await self._validate_remote_advertisement(advertisement)

    async def _validate_local_advertisement(self, advertisement: Advertisement) -> None:
        if not advertisement.node_id == self._network.local_node_id:
            raise Exception(
                "Must be called with an advertisement signed by the local node"
            )

        try:
            try:
                # First query "pinned" storage
                content = self._network.pinned_content_storage.get_content(
                    advertisement.content_key
                )
            except ContentNotFound:
                # Fall back to "commons" storage
                content = self._network.commons_content_storage.get_content(
                    advertisement.content_key
                )
        except ContentNotFound as err:
            raise ValidationError(
                f"Content not found: advertisement={advertisement}"
            ) from err

        hash_tree_root = ssz.get_hash_tree_root(content, sedes=content_sedes)

        if hash_tree_root != advertisement.hash_tree_root:
            raise ValidationError(
                f"Mismatched roots: local={hash_tree_root.hex()}  "
                f"advertisement={advertisement.hash_tree_root.hex}"
            )

    async def _validate_remote_advertisement(
        self, advertisement: Advertisement
    ) -> None:
        """
        - valid signature
        - not expired
        - within local advertisement radius
        - node is reachable
        - proof of custody check
        """
        # Verify the liveliness of the node this advertisement is for.
        did_bond = await self._network.bond(advertisement.node_id)
        if not did_bond:
            raise ValidationError(
                f"Advertisement failed liveliness check: node_id={advertisement.node_id.hex()}"
            )

        # Perform a proof of custody check
        try:
            base_proof = await self._network.get_content_proof(
                advertisement.node_id,
                hash_tree_root=advertisement.hash_tree_root,
                content_key=advertisement.content_key,
                start_chunk_index=0,
                max_chunks=2,
            )
        except trio.TooSlowError:
            raise ValidationError(
                f"Initial proof retrieval failed: advertisement={advertisement}"
            )

        if not base_proof.is_complete:
            content_length = base_proof.get_content_length()
            chunk_count = (content_length + 31) // CHUNK_SIZE
            if chunk_count > 2:
                # We avoid fetching the first two chunks since we already have
                # them from the initial proof.
                chunk_to_request = secrets.randbelow(chunk_count - 2) + 2
                if not base_proof.has_chunk(chunk_to_request):
                    try:
                        custody_proof = await self._network.get_content_proof(
                            advertisement.node_id,
                            hash_tree_root=advertisement.hash_tree_root,
                            content_key=advertisement.content_key,
                            start_chunk_index=chunk_to_request,
                            max_chunks=2,
                        )
                    except trio.TooSlowError:
                        raise ValidationError(
                            f"Proof of custody check failed: "
                            f"advertisement={advertisement}"
                        )
                    else:
                        if not custody_proof.has_chunk(chunk_to_request):
                            # Failed proof of custody check
                            raise ValidationError(
                                f"Proof of custody check failed: "
                                f"advertisement={advertisement}"
                            )

    async def handle_advertisement(self, advertisement: Advertisement) -> bool:
        try:
            await self.validate_advertisement(advertisement)
        except ValidationError as err:
            self.logger.debug(
                "Advertisement failed validation: advertisement=%s err=%s",
                advertisement,
                err,
            )
            return False
        else:
            self._advertisement_db.add(advertisement)
            await self._network.broadcast(advertisement)
            await self.new_advertisement.trigger(advertisement)
            return True

    async def _handle_request(self, request: InboundMessage[AdvertiseMessage]) -> None:
        """
        Process the advertisements.

        Each advertisement goes through this process:

        - If already present we assume validity
        - Otherwise validate:
            - Bond with node to check liveliness
            - Check `content_key/hash_tree_root` against existing database entries.
                - If there is a discrepancy we must determine canonical `hash_tree_root`.
            - Proof of custody check.
                - Request random chunk(s) of the data and verify against `hash_tree_root`

        Once validated:

        - Broadcast advertisement to logarithmic subset of peers for which the
          advertisement falls within their ad radius
        """
        if not all(ad.is_valid for ad in request.message.payload):
            # TODO: this is a place where we should consider blacklisting
            self.logger.debug(
                "Invalid advertisements: from=%s  advertisements=%s",
                request.sender_node_id.hex(),
                request.message.payload,
            )
            return

        if any(ad.is_expired for ad in request.message.payload):
            self.logger.debug(
                "Expired advertisements: from=%s  advertisements=%s",
                request.sender_node_id.hex(),
                request.message.payload,
            )
            return

        # We Ack
        acked = tuple(
            self.check_interest(advertisement)
            for advertisement in request.message.payload
        )
        await self._network.client.send_ack(
            request.sender_node_id,
            request.sender_endpoint,
            advertisement_radius=self._network.local_advertisement_radius,
            acked=acked,
            request_id=request.request_id,
        )

        for advertisement, is_interesting in zip(request.message.payload, acked):
            if not is_interesting:
                continue

            # Log the advertisements on the way in so that we don't try to
            # rebroadcast back to this node.
            self._network.broadcast_log.log(request.sender_node_id, advertisement)

            await self.handle_advertisement(advertisement)
