import binascii
import itertools
from pathlib import Path

import click


def encode_bytes(v: bytes):
    try:
        return v.decode("utf-8")
    except UnicodeDecodeError:
        return "0x" + binascii.hexlify(v).decode()


def decode_bytes(v: str):
    if v.startswith("0x"):
        return binascii.unhexlify(v[2:])
    else:
        return v.encode()


@click.group
def cli():
    pass


@cli.command()
@click.option("--db", help="path to versiondb", type=click.Path(exists=True))
@click.argument("file-streamer", type=click.Path(exists=True))
def sync_local(db, file_streamer):
    from .sync import sync_local
    from .versiondb import VersionDB

    sync_local(Path(file_streamer), VersionDB.open_rocksdb(Path(db)))


@cli.command()
@click.option("--db", help="path to versiondb", type=click.Path(exists=True))
@click.option("--version", default=None, type=click.INT)
@click.argument("store_key", type=click.STRING)
@click.argument("key", type=click.STRING)
def get(db, store_key, key, version):
    from .versiondb import VersionDB

    key = decode_bytes(key)
    versiondb = VersionDB.open_rocksdb(Path(db))
    value = versiondb.get(version, store_key, key)
    if value:
        print(encode_bytes(value))


@cli.command()
@click.option("--db", help="path to versiondb", type=click.Path(exists=True))
@click.option("--version", default=None, type=click.INT)
@click.option("--start", type=click.STRING)
@click.option("--limit", default=100)
@click.option("--reverse", default=False)
@click.argument("store_key", type=click.STRING)
def range(db, version, store_key, start, reverse, limit):
    from .versiondb import VersionDB

    if start:
        start = decode_bytes(start)

    versiondb = VersionDB.open_rocksdb(Path(db))
    it = versiondb.iterator(version, store_key, start, reverse=reverse)
    for k, v in itertools.islice(it, limit):
        print(encode_bytes(k), encode_bytes(v))


if __name__ == "__main__":
    cli()
