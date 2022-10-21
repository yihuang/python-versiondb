from pathlib import Path
from typing import List, Optional

import rocksdb

from .iterator import VersionDBIter
from .utils import (KVPair, changeset_key, decode_stdint64, encode_stdint64,
                    full_key, get_bitmap, prefix_iteritems, seek_bitmap,
                    set_bitmap, store_key_prefix)

LATEST_VERSION_KEY = b"s/latest"


class DB:
    pass


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
        store_key: str,
        start: Optional[bytes] = None,
        reverse: bool = False,
    ):
        if version is not None and version == self.latest_version():
            version = None

        if version is None:
            it = self.plain.iteritems()
            prefix = store_key_prefix(store_key)
            it.seek(prefix + (start or b""))
            return prefix_iteritems(it, prefix, reverse)
        return VersionDBIter(self, version, store_key, start, reverse)
