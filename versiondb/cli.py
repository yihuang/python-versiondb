import binascii
from pathlib import Path

import click


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

    if key.startswith("0x"):
        key = binascii.unhexlify(key[2:])
    else:
        key = key.encode()

    versiondb = VersionDB.open_rocksdb(Path(db))
    value = versiondb.get(version, store_key, key)
    if value:
        try:
            print(value.decode("utf-8"))
        except UnicodeDecodeError:
            print("0x" + binascii.hexlify(value).decode())


if __name__ == "__main__":
    cli()
