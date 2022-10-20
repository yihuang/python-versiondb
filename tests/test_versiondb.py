from versiondb import __version__


def test_version():
    assert __version__ == "0.1.0"


def test_db_operations(testdb):
    # test latest version
    assert b"value1" == testdb.get(None, "evm", b"key1")
    assert testdb.get(None, "evm", b"key2") is None
    assert b"value2" == testdb.get(None, "staking", b"key1")
    assert b"value2" == testdb.get(2, "staking", b"key1")
    assert b"value1" == testdb.get(0, "staking", b"key1/subkey")

    # test version 1
    assert testdb.get(1, "staking", b"key1") is None
    assert b"value1" == testdb.get(0, "staking", b"key1/subkey")

    # test version 0
    assert b"value1" == testdb.get(0, "staking", b"key1")
    assert b"value1" == testdb.get(0, "staking", b"key1/subkey")


def test_iteration(testdb):
    # latest version
    exp_v0 = [
        (b"s/k:evm/key1", b"value1"),
        (b"s/k:evm/key2", b"value2"),
        (b"s/k:staking/key1", b"value1"),
        (b"s/k:staking/key1/subkey", b"value1"),
    ]
    exp_v1 = [
        (b"s/k:evm/key1", b"value1"),
        (b"s/k:evm/key2", b"value2"),
        (b"s/k:staking/key1/subkey", b"value1"),
    ]
    exp_latest = [
        (b"s/k:evm/key1", b"value1"),
        (b"s/k:staking/key1", b"value2"),
        (b"s/k:staking/key1/subkey", b"value1"),
        # FIXME shouldn't contain this
        (b"s/latest", b"\x08\x02"),
    ]
    assert exp_latest == list(testdb.iterator(None))
    assert exp_v0 == list(testdb.iterator(0))
    assert exp_v1 == list(testdb.iterator(1))
    # assert exp_latest == list(testdb.iterator(2))
