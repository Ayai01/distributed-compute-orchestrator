"""Reuse compressed ZIP members while receiving an immutable, SHA-tagged result."""

from collections import OrderedDict
import hashlib
from pathlib import Path
import re
import shutil
import struct
import tempfile
import threading
import zipfile


HEADER = struct.Struct('<4s5H3I2H')
MIN_MEMBER = 1024 * 1024
MAX_MEMBER = 32 * MIN_MEMBER
BLOCK = 64 * 1024


def member(stream, offset):
    stream.seek(offset)
    header = stream.read(HEADER.size)
    if len(header) < HEADER.size:
        return None
    sig, _, flags, method, _, _, crc, compressed, size, names, extra = HEADER.unpack(header)
    if sig != b'PK\x03\x04' or flags & 9 or max(compressed, size) == 0xffffffff:
        return None
    name = stream.read(names)
    if len(name) != names:
        return None
    return (name, method, crc, compressed, size), stream.tell() + extra


def copy_length(source, target, length):
    while length:
        chunk = source.read(min(length, 1024 * 1024))
        if not chunk:
            raise EOFError('Truncated ZIP member')
        target.write(chunk)
        length -= len(chunk)


class MemberCache:
    def __init__(self, limit=128 * MIN_MEMBER):
        self.limit = limit
        self.bytes = 0
        self.entries = OrderedDict()
        self.lock = threading.Lock()

    def get(self, key):
        with self.lock:
            entry = self.entries.get(key)
            if entry is not None:
                self.entries.move_to_end(key)
            return entry

    def remember(self, path):
        # The TemporaryDirectory stays alive while any receiving thread holds
        # the entry, including after eviction. The worker's result can retire.
        try:
            with zipfile.ZipFile(path) as archive, path.open('rb') as source:
                for info in archive.infolist():
                    if not MIN_MEMBER <= info.compress_size <= min(MAX_MEMBER, self.limit):
                        continue
                    record = member(source, info.header_offset)
                    if record is None:
                        continue
                    key, offset = record
                    if key[3] != info.compress_size or self.get(key) is not None:
                        continue
                    directory = tempfile.TemporaryDirectory(prefix='dco-result-member-')
                    with (Path(directory.name) / 'member').open('wb') as output:
                        source.seek(offset)
                        copy_length(source, output, key[3])
                    with self.lock:
                        if key not in self.entries:
                            self.entries[key] = directory
                            self.bytes += key[3]
                            while self.bytes > self.limit:
                                evicted, _ = self.entries.popitem(last=False)
                                self.bytes -= evicted[3]
        except (OSError, EOFError, zipfile.BadZipFile):
            return  # A cache miss leaves the original receipt usable.


cache = MemberCache()


class Receiver:
    def __init__(self):
        self.header = 0
        self.check_at = HEADER.size

    def candidate(self, output, etag):
        end = output.tell()
        if end < self.check_at or not re.fullmatch(r'"[0-9a-f]{64}"', etag or ''):
            return None
        output.flush()
        with Path(output.name).open('rb') as paid:
            while self.header + HEADER.size <= end:
                record = member(paid, self.header)
                if record is None:
                    self.check_at = end + BLOCK
                    return None
                key, start = record
                stop = start + key[3]
                entry = cache.get(key) if stop - end >= MIN_MEMBER else None
                if entry is not None:
                    if end < start + BLOCK:
                        self.check_at = start + BLOCK
                        return None
                    paid.seek(start)
                    with (Path(entry.name) / 'member').open('rb') as donor:
                        left = end - start
                        while left:
                            count = min(left, BLOCK)
                            if paid.read(count) != donor.read(count):
                                break
                            left -= count
                        if not left:
                            return entry, end - start, stop - end
                self.header = stop
                self.check_at = stop + HEADER.size
        return None

    @staticmethod
    def complete(output, etag, candidate, fetch_tail):
        entry, donor_offset, skipped = candidate
        prefix = output.tell()
        with tempfile.TemporaryDirectory(prefix='dco-range-', dir=Path(output.name).parent) as tmp:
            stage_path = Path(tmp) / 'candidate'
            with stage_path.open('w+b') as stage:
                with Path(output.name).open('rb') as paid:
                    copy_length(paid, stage, prefix)
                with (Path(entry.name) / 'member').open('rb') as donor:
                    donor.seek(donor_offset)
                    copy_length(donor, stage, skipped)
                status, received_tag = fetch_tail(stage)
                if status not in (200, 206) or received_tag != etag:
                    return None
                stage.seek(0)
                digest = hashlib.sha256()
                for block in iter(lambda: stage.read(1024 * 1024), b''):
                    digest.update(block)
                if digest.hexdigest() != etag[1:-1]:
                    return None
                # Only the original writer appends. Until the whole recipient
                # matches its own SHA, the durable paid prefix is untouched.
                stage.seek(prefix)
                shutil.copyfileobj(stage, output, 1024 * 1024)
        return skipped if status == 206 else 0
