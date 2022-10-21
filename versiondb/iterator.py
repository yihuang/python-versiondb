from __future__ import annotations

from typing import TYPE_CHECKING

from roaring64 import BitMap64

from .utils import (changeset_key, full_key, incr_bytes, prefix_iteritems,
                    seek_bitmap, store_key_prefix)

if TYPE_CHECKING:
    from .versiondb import VersionDB


class VersionDBIter:
    store: VersionDB
    version: int
    start: bytes
    reverse: bool

    # rocksdb.BaseIterator
    iter_plain: object
    iter_history: object

    status: int
    # record the last key values of history cursor
    hk: bytes
    hv: bytes
    # record the last key values of plain cursor
    pk: bytes
    pv: bytes

    def __init__(
        self,
        store: VersionDB,
        version: int,
        store_key: str,
        start: bytes,
        reverse: bool,
    ):
        self.store = store
        self.version = version
        self.store_key = store_key
        self.start = start
        self.reverse = reverse

        iter_plain = store.plain.iteritems()
        iter_history = store.history.iteritems()
        if reverse:
            iter_plain = reversed(iter_plain)
            iter_history = reversed(iter_history)
        self.status = 0

        prefix = store_key_prefix(store_key)
        if start:
            full_start = prefix + start
            iter_plain.seek(full_start)
            iter_history.seek(full_start)
            if reverse:
                if iter_plain.get()[0] > full_start:
                    next(iter_plain)
                if iter_history.get()[0] > full_start:
                    next(iter_history)
        else:
            if not reverse:
                iter_plain.seek(prefix)
                iter_history.seek(prefix)
            else:
                end = incr_bytes(prefix)
                iter_plain.seek_for_prev(end)
                iter_history.seek_for_prev(end)

        self.iter_plain = prefix_iteritems(iter_plain, prefix, reverse)
        self.iter_history = prefix_iteritems(iter_history, prefix, reverse)

    def __iter__(self):
        return self

    def _advance(self):
        if self.status == -2:
            raise StopIteration
        elif self.status == 1:
            self.hk, self.hv = incr_iter(self.iter_history)
        elif self.status == -1:
            self.pk, self.pv = incr_iter(self.iter_plain)
        else:
            self.hk, self.hv = incr_iter(self.iter_history)
            self.pk, self.pv = incr_iter(self.iter_plain)
        self.status = compare_key(self.pk, self.hk, self.reverse)

    def __next__(self):
        while True:
            self._advance()
            if self.status == -2:
                raise StopIteration
            elif self.status == 0:
                # both cursor at same key, try get historical value,
                # or fallback to latest one.
                bm = BitMap64.deserialize(self.hv)
                found = seek_bitmap(bm, self.version)
                if found is None:
                    return self.pk, self.pv
                v = self.store.changeset.get(
                    changeset_key(found, full_key(self.store_key, self.hk))
                )
                if not v:
                    # deleted, keep advancing
                    continue
                return self.hk, v
            elif self.status == -1:
                # the key don't exist in history state, use the plain state value.
                return self.pk, self.pv
            elif self.status == 1:
                # the key is deleted in plain state, try to use the history state.
                bm = BitMap64.deserialize(self.hv)
                found = seek_bitmap(bm, self.version)
                if found is None:
                    # deleted, keep advancing
                    continue
                v = self.store.changeset.get(
                    changeset_key(found, full_key(self.store_key, self.hk))
                )
                if not v:
                    # deleted, keep advancing
                    continue
                return self.hk, v


def compare_key(k1, k2, reverse: bool):
    """
    return the relationship of plain and history cursors:
    0: both are on par
    1: plain cursor ahead of history
    -1: history cursor ahead of plain
    -2: both cursor has finished

    empty value is like reached finish line, it's always the biggest,
    no matter reverse order or not.
    """
    if not k1 and not k2:
        return -2
    if not k1:
        return 1
    if not k2:
        return -1
    if k1 == k2:
        return 0

    result = 1 if k1 > k2 else -1
    if reverse:
        result = -result

    return result


def incr_iter(it):
    try:
        return next(it)
    except StopIteration:
        return None, None
