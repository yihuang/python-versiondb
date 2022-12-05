from cprotobuf import Field, ProtoEntity, decode_primitive

from .versiondb import KVPair


class StoreKVPairs(ProtoEntity):
    # the store key for the KVStore this pair originates from
    store_key = Field("string", 1)
    # true indicates a delete operation
    delete = Field("bool", 2)
    key = Field("bytes", 3)
    value = Field("bytes", 4)

    def to_kvpair(self):
        return KVPair(self.store_key, self.key, self.value if not self.delete else None)


def decode_stream_file(data, entry_cls=StoreKVPairs):
    """
    StoreKVPairs, StoreKVPairs, ...
    """
    assert int.from_bytes(data[:8], "big") + 8 == len(data), "incomplete file"

    items = []
    offset = 8
    while offset < len(data):
        size, n = decode_primitive(data[offset:], "uint64")
        offset += n
        item = entry_cls()
        item.ParseFromString(data[offset : offset + size])
        items.append(item)
        offset += size
    return items


def sync_local(path, versiondb):
    """load changeset from file streamer output to versiondb

    file streamer outputs start with block 1.
    """
    version = (versiondb.latest_version() or 0) + 1
    while True:
        try:
            items = decode_stream_file(
                open(path / f"block-{version}-data", "rb").read()
            )
        except FileNotFoundError:
            break
        items = [item.to_kvpair() for item in items]
        versiondb.put(version, items)
        version += 1
