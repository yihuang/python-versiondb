import itertools
from collections.abc import Iterator
from typing import Callable, NamedTuple, Optional

from cprotobuf import decode_primitive, encode_primitive
from roaring64 import BitMap64


class KVPair(NamedTuple):
    store_key: str
    key: bytes
    # None means deletion
    value: Optional[bytes]


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


def store_key_prefix(store_key: str) -> bytes:
    return f"s/k:{store_key}/".encode()


def full_key(store_key: str, key: bytes) -> bytes:
    return store_key_prefix(store_key) + key


def get_bitmap(history, key: bytes) -> BitMap64:
    v = history.get(key)
    return BitMap64.deserialize(v) if v else None


def set_bitmap(history, key: bytes, version: int) -> BitMap64:
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


def incr_bytes(prefix: bytes) -> bytes:
    bz = list(prefix)
    while bz:
        if bz[-1] != 255:
            bz[-1] += 1
            break

        bz = bz[:-1]
    return bytes(bz)


def prefix_iteritems(
    it: Iterator, prefix: bytes, reverse: bool = False, end: Optional[bytes] = None
):
    if not reverse:
        end = incr_bytes(prefix) if not end else prefix + end
        it = itertools.takewhile(lambda t: t[0] < end, it)
    else:
        if end:
            it = itertools.takewhile(lambda t: t[0] > prefix + end, it)
        else:
            it = itertools.takewhile(lambda t: t[0] >= prefix, it)
    return ((k.removeprefix(prefix), v) for k, v in it)
