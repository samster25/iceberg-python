from pyiceberg.manifest import *
from pyiceberg.io.fsspec import FsspecFileIO


path = "/Users/sammy/code/iceberg-python/52c53580-f5f9-4514-b5df-55e08fe7c0e4-m0.avro"
ff = FsspecFileIO({}).new_input(location=path)
with AvroFile[ManifestEntry](
            ff,
             MANIFEST_ENTRY_SCHEMAS[DEFAULT_READ_VERSION],
             read_types={-1: ManifestEntry, 2: DataFile},
             read_enums={0: ManifestEntryStatus, 101: FileFormat, 134: DataFileContent},
         ) as reader:
        count = 0
        for _ in reader:
            count += 1
        print(count)