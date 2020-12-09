import pytest

from eth_enr.tools.factories import ENRFactory
from eth_utils import ValidationError

from ddht.kademlia import compute_log_distance
from ddht.validation import validate_found_nodes_distances


@pytest.fixture
def local_enr():
    return ENRFactory()


@pytest.fixture
def enr_group(local_enr):
    enrs = tuple(ENRFactory() for _ in range(10))
    distances = tuple(
        compute_log_distance(enr.node_id, local_enr.node_id) for enr in enrs
    )
    return enrs, distances


def test_validate_found_nodes_distances(local_enr, enr_group):
    enrs, distances = enr_group
    assert validate_found_nodes_distances(enrs, local_enr.node_id, distances) is None


def test_validate_found_nodes_distances_catches_invalid_cases(local_enr, enr_group):
    enrs, distances = enr_group
    unique_distances = set(distances)
    unique_distances.remove(distances[0])
    with pytest.raises(ValidationError, match="Invalid response: distance"):
        validate_found_nodes_distances(enrs, local_enr.node_id, unique_distances)


def test_validate_found_nodes_distances_catches_self_reference(local_enr, enr_group):
    enrs, distances = enr_group
    invalid_enrs = enrs + (local_enr,)
    unique_distances = set(distances)
    assert 0 not in unique_distances
    with pytest.raises(ValidationError, match="Invalid response: distance=0"):
        validate_found_nodes_distances(
            invalid_enrs, local_enr.node_id, unique_distances
        )
