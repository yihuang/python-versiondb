import pytest
import rocksdb
from versiondb import KVPair, VersionDB


@pytest.fixture(scope="module")
def testdb(tmp_path_factory):
    path = tmp_path_factory.mktemp("testdb")
    plain = rocksdb.DB(str(path / "plain.db"), rocksdb.Options(create_if_missing=True))
    changeset = rocksdb.DB(
        str(path / "changeset.db"), rocksdb.Options(create_if_missing=True)
    )
    history = rocksdb.DB(
        str(path / "history.db"), rocksdb.Options(create_if_missing=True)
    )
    store = VersionDB(plain, changeset, history)
    init_test_db(store)
    return store


def init_test_db(store: VersionDB):
    change_sets = [
        [
            KVPair("evm", b"key1", b"value1"),
            KVPair("evm", b"key2", b"value2"),
            KVPair("staking", b"key1", b"value1"),
            KVPair("staking", b"key1/subkey", b"value1"),
        ],
        [
            KVPair("staking", b"key1", None),
        ],
        [
            KVPair("staking", b"key1", b"value2"),
            KVPair("evm", b"key2", None),
        ],
    ]
    for v, change_set in enumerate(change_sets):
        store.put(v, change_set)
