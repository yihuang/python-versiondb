import pytest
import rocksdb
from versiondb import KVPair, VersionDB


@pytest.fixture(scope="function")
def testdb(tmp_path):
    store = VersionDB(
        rocksdb.DB(str(tmp_path / "plain.db"), rocksdb.Options(create_if_missing=True)),
        rocksdb.DB(
            str(tmp_path / "changeset.db"), rocksdb.Options(create_if_missing=True)
        ),
        rocksdb.DB(
            str(tmp_path / "history.db"), rocksdb.Options(create_if_missing=True)
        ),
    )
    init_test_db(store)
    return store


def init_test_db(store: VersionDB):
    """
    include test cases for:
    - modify
    - deletion in historical state, but added again in latest state.
    - exist in historical state, but delete in latest state.
    - subkey, one key is another key's prefix.
    """
    change_sets = [
        [
            KVPair("evm", b"delete-in-block2", b"1"),
            KVPair("evm", b"re-add-in-block3", b"1"),
            KVPair("evm", b"z-genesis-only", b"2"),
            KVPair("evm", b"modify-in-block2", b"1"),
            KVPair("staking", b"key1", b"value1"),
            KVPair("staking", b"key1/subkey", b"value1"),
        ],
        [
            KVPair("evm", b"re-add-in-block3", None),
            KVPair("evm", b"add-in-block1", b"1"),
            KVPair("staking", b"key1", None),
        ],
        [
            KVPair("evm", b"add-in-block2", b"1"),
            KVPair("evm", b"delete-in-block2", None),
            KVPair("evm", b"modify-in-block2", b"2"),
            KVPair("staking", b"key1", b"value2"),
            KVPair("evm", b"key2", None),
        ],
        [
            KVPair("evm", b"re-add-in-block3", b"2"),
        ],
        [
            KVPair("evm", b"re-add-in-block3", None),
        ],
    ]
    for v, change_set in enumerate(change_sets):
        store.put(v, change_set)
