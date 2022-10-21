from versiondb import KVPair, __version__


def test_version():
    assert __version__ == "0.1.0"


def test_db_operations(testdb):
    # test latest version
    assert b"2" == testdb.get(None, "evm", b"z-genesis-only")
    assert b"2" == testdb.get(4, "evm", b"z-genesis-only")
    assert testdb.get(None, "evm", b"re-add-in-block3") is None
    assert b"value2" == testdb.get(None, "staking", b"key1")
    assert b"value2" == testdb.get(2, "staking", b"key1")
    assert b"value1" == testdb.get(0, "staking", b"key1/subkey")

    # test version 1
    assert testdb.get(1, "staking", b"key1") is None
    assert b"value1" == testdb.get(0, "staking", b"key1/subkey")

    # test version 0
    assert b"value1" == testdb.get(0, "staking", b"key1")
    assert b"value1" == testdb.get(0, "staking", b"key1/subkey")


def test_iterator(testdb):
    # evm store
    exp_evm = [
        [
            (b"delete-in-block2", b"1"),
            (b"modify-in-block2", b"1"),
            (b"re-add-in-block3", b"1"),
            (b"z-genesis-only", b"2"),
        ],
        [
            (b"add-in-block1", b"1"),
            (b"delete-in-block2", b"1"),
            (b"modify-in-block2", b"1"),
            (b"z-genesis-only", b"2"),
        ],
        [
            (b"add-in-block1", b"1"),
            (b"add-in-block2", b"1"),
            (b"modify-in-block2", b"2"),
            (b"z-genesis-only", b"2"),
        ],
        [
            (b"add-in-block1", b"1"),
            (b"add-in-block2", b"1"),
            (b"modify-in-block2", b"2"),
            (b"re-add-in-block3", b"2"),
            (b"z-genesis-only", b"2"),
        ],
        [
            (b"add-in-block1", b"1"),
            (b"add-in-block2", b"1"),
            (b"modify-in-block2", b"2"),
            (b"z-genesis-only", b"2"),
        ],
    ]
    for i, exp in enumerate(exp_evm):
        # assert exp == list(testdb.iterator(i, "evm")), f"block-{i}"
        assert list(reversed(exp)) == list(
            testdb.iterator(i, "evm", reverse=True)
        ), f"block-{i}-reverse"
    assert exp_evm[-1] == list(testdb.iterator(None, "evm"))

    # with start parameter
    assert [] == list(testdb.iterator(2, "evm", start=b"\xff"))
    assert [] == list(testdb.iterator(2, "evm", start=b"\x00", reverse=True))
    assert exp_evm[2][-2:] == list(testdb.iterator(2, "evm", start=b"modify-in-block2"))
    assert list(reversed(exp_evm[2][:-1])) == list(
        testdb.iterator(2, "evm", start=b"mp", reverse=True)
    )
    assert list(reversed(exp_evm[2][:-1])) == list(
        testdb.iterator(2, "evm", start=b"modify-in-block2", reverse=True)
    )

    # delete the last key
    testdb.put(
        len(exp_evm),
        [
            KVPair("evm", b"z-genesis-only", None),
        ],
    )
    i = len(exp_evm)
    assert exp_evm[-1][:-1] == list(testdb.iterator(i, "evm")), f"block-{i}"
