from pathlib import Path
from typing import List, NamedTuple, Optional

import rocksdb
from cprotobuf import decode_primitive, encode_primitive
from roaring64 import BitMap64

from .iterator import VersionDBIter

LATEST_VERSION_KEY = b"s/latest"


class DB:
    pass


class KVPair(NamedTuple):
    store_key: str
    key: bytes
    # None means deletion
    value: Optional[bytes]


class VersionDB:
    plain: DB
    changeset: DB
    history: DB

    _is_rocksdb: bool

    def __init__(self, plain: DB, changeset: DB, history: DB):
        self.plain = plain
        self.changeset = changeset
        self.history = history

        try:
            self.plain.write
        except AttributeError:
            self._is_rocksdb = False
        else:
            self._is_rocksdb = True

    @classmethod
    def open_rocksdb(cls, path):
        path = Path(path)
        return cls(
            rocksdb.DB(str(path / "plain.db"), rocksdb.Options(create_if_missing=True)),
            rocksdb.DB(
                str(path / "changeset.db"), rocksdb.Options(create_if_missing=True)
            ),
            rocksdb.DB(
                str(path / "history.db"), rocksdb.Options(create_if_missing=True)
            ),
        )

    def get(self, version: Optional[int], store_key: str, key: bytes) -> bytes:
        if version is not None and version == self.latest_version():
            version = None

        key = full_key(store_key, key)

        if version is None:
            return self.plain.get(key)

        # find in historical changeset
        bitmap = get_bitmap(self.history, key)
        if not bitmap:
            return self.plain.get(key)
        v = seek_bitmap(bitmap, version)
        if v is None:
            return self.plain.get(key)

        # lookup in changeset db
        return self.changeset.get(changeset_key(v, key))

    def put(self, version: int, change_set: List[KVPair]):
        if self._is_rocksdb:
            # rocksdb
            self.put_batch(version, change_set)
        else:
            # lmdb
            self.put_transactional(version, change_set)

    def put_batch(self, version: int, change_set: List[KVPair]):
        plain_batch = rocksdb.WriteBatch()
        history_batch = rocksdb.WriteBatch()
        changeset_batch = rocksdb.WriteBatch()
        for item in change_set:
            key = full_key(item.store_key, item.key)

            if version == 0:
                # write genesis state into plain state directly
                assert item.value is not None, "can't delete in genesis state"
                plain_batch.put(key, item.value)
                continue

            original = self.plain.get(key)
            if original == item.value:
                continue

            # write histroy index
            bm = set_bitmap(self.history, key, version)
            history_batch.put(key, bm.serialize())

            # write changeset record
            if original is not None:
                changeset_batch.put(changeset_key(version, key), original)

            if item.value is None:
                plain_batch.delete(key)
            else:
                plain_batch.put(key, item.value)

        plain_batch.put(LATEST_VERSION_KEY, encode_stdint64(version))

        self.changeset.write(changeset_batch)
        self.history.write(history_batch)
        self.plain.write(plain_batch)

    def put_transactional(self, version: int, change_set: List[KVPair]):
        raise NotImplementedError()

    def latest_version(self) -> Optional[int]:
        v = self.plain.get(LATEST_VERSION_KEY)
        return decode_stdint64(v) if v else None

    def iterator(
        self,
        version: Optional[int],
        start: Optional[bytes] = None,
        reverse: bool = False,
    ):
        # TODO check latest version proactively if version is not None
        if version is None:
            it = self.plain.iteritems()
            if start is None:
                it.seek_to_first()
            else:
                it.seek(start)
            return it
        return VersionDBIter(self, version, start, reverse)


def encode_stdint64(n):
    """
    message StdInt64 {
      int64 a = 1;
    }
    """
    return b"\x08" + encode_primitive("int64", n)


def decode_stdint64(v):
    n, _ = decode_primitive(v[1:], "int64")
    return n


def full_key(store_key: str, key: bytes) -> bytes:
    return f"s/k:{store_key}/".encode() + key


def get_bitmap(history: DB, key: bytes) -> BitMap64:
    v = history.get(key)
    return BitMap64.deserialize(v) if v else None


def set_bitmap(history: DB, key: bytes, version: int) -> BitMap64:
    bm = get_bitmap(history, key)
    if bm is None:
        bm = BitMap64()
    bm.add(version)
    return bm


def seek_bitmap(bitmap: BitMap64, version: int) -> Optional[int]:
    "try to find the minimal number in bitmap that is larger than version"
    size = len(bitmap)
    if size == 0:
        return
    # Rank returns the number of integers that are smaller or equal to x.
    i = bitmap.rank(version)
    if i >= size:
        return
    return bitmap[i]


def changeset_key(version: int, key: bytes) -> bytes:
    return version.to_bytes(8, "big") + key
