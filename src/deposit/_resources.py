"""Single-attempt resource upload and checked authenticated downloads."""

from base64 import b64decode, b64encode
from contextlib import contextmanager, suppress
from hashlib import sha256
import mimetypes
import re

from ._exchange import CHUNK_BYTES, _stream_blocks
from ._resource_buffer import freeze_resource
from .errors import DepositError, OutcomeUnknown


class TransferError(DepositError):
    def __init__(self, message):
        super().__init__("RESOURCE_CORRUPT", message)


@contextmanager
def _resource_input(resource, check_running):
    if resource._path is not None:
        # The Server independently checks the second pass against the first
        # digest before publication. Both passes use the same owned handle.
        with open(resource._path, "rb") as stream:
            digest = sha256()
            length = 0
            for block in _stream_blocks(stream):
                check_running()
                digest.update(block)
                length += len(block)
            stream.seek(0)
            yield length, digest.hexdigest(), _stream_blocks(stream)
    elif resource._stream is not None:
        with freeze_resource(resource._stream, check_running) as frozen:
            yield frozen
    else:
        raise ValueError("Resource upload requires a path or binary stream")


def upload_resource(endpoint, resource):
    filename = resource.filename
    if not isinstance(filename, str) or not filename:
        raise ValueError("Resource upload requires a filename")
    media_type = resource._value.get("mediaType") or mimetypes.guess_type(filename)[0] or "application/octet-stream"
    try:
        with _resource_input(resource, endpoint.check_running) as (size, checksum, chunks), endpoint.execution() as execution:
            started = execution.run("deposit.resource.beginUpload", filename, size, checksum, media_type)
            upload_id = started.get("uploadId")
            if not isinstance(upload_id, str) or started.get("nextOffset") != 0:
                raise TransferError("Server did not open a fresh resource upload")
            try:
                offset = 0
                for block in chunks:
                    next_offset = offset + len(block)
                    acknowledgement = execution.run("deposit.resource.uploadChunk", upload_id, offset,
                                                    b64encode(block).decode("ascii"), sha256(block).hexdigest())
                    if acknowledgement.get("nextOffset") != next_offset:
                        raise TransferError("Server did not confirm the resource chunk")
                    offset = next_offset
                result = execution.run("deposit.resource.finishUpload", upload_id)
                if (not isinstance(result.get("resourceId"), str) or result.get("state") != "ready"
                        or result.get("bytes") != size or result.get("checksum") != checksum):
                    raise TransferError("Server returned invalid completed resource metadata")
            except OutcomeUnknown:
                raise
            except BaseException:
                with suppress(Exception):
                    execution.run("deposit.resource.abortUpload", upload_id, cleanup=True)
                raise
    except BaseException:
        # A fence or the lost writer connection makes abandoned staging
        # collectible even when the explicit abort could not be delivered.
        resource._stream = None
        with suppress(Exception):
            endpoint.interrupt_upload()
        raise
    resource._value.update(result, filename=filename, mediaType=media_type, namespaceId=endpoint.scope)
    resource._path = resource._stream = None
    return resource


class ResourceReader:
    """Small checked binary reader over the server's chunk-download route."""

    def __init__(self, request, resource_id: str, metadata: dict[str, Any]):
        self._request = request
        self._resource_id = resource_id
        self._size = metadata.get("bytes")
        self._expected = metadata.get("checksum")
        self._chunk_size = CHUNK_BYTES
        self._offset = 0
        self._buffer = bytearray()
        self._digest = sha256()
        self._closed = False
        self._complete = False
        if type(self._size) is not int or self._size < 0 or not isinstance(self._expected, str) or not re.fullmatch(r"[0-9a-f]{64}", self._expected):
            raise TransferError("Resource metadata has invalid size or checksum")
        if self._size == 0:
            self._verify_complete()

    @property
    def closed(self) -> bool:
        return self._closed

    def readable(self) -> bool:
        return not self._closed

    def read(self, size: int = -1) -> bytes:
        if self._closed:
            raise ValueError("I/O operation on closed resource reader")
        if type(size) is not int:
            raise TypeError("read size must be an integer")
        if size == 0:
            return b""
        while not self._complete and (size < 0 or len(self._buffer) < size):
            self._read_chunk()
        if size < 0:
            result = bytes(self._buffer)
            self._buffer.clear()
            return result
        result = bytes(self._buffer[:size])
        del self._buffer[:size]
        return result

    def readinto(self, buffer) -> int:
        view = memoryview(buffer).cast("B")
        data = self.read(len(view))
        view[:len(data)] = data
        return len(data)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._buffer.clear()
        if not self._complete:
            raise TransferError("Resource download was closed before checksum verification completed")

    def __enter__(self):
        if self._closed:
            raise ValueError("I/O operation on closed resource reader")
        return self

    def __exit__(self, exc_type, exc, traceback):
        if exc_type is None:
            self.close()
            return False
        try:
            self.close()
        except TransferError as close_error:
            if close_error.message != "Resource download was closed before checksum verification completed":
                raise
        return False

    def _read_chunk(self) -> None:
        remaining = self._size - self._offset
        if remaining <= 0:
            self._verify_complete()
            return
        requested = min(self._chunk_size, remaining)
        data = self._request("deposit.resource.downloadChunk", self._resource_id, self._offset, requested)
        if not isinstance(data, dict):
            raise TransferError("Resource download returned invalid chunk metadata")
        try:
            block = b64decode(data["bytesBase64"], validate=True)
        except Exception as exc:
            raise TransferError("Resource download returned invalid Base64") from exc
        next_offset = self._offset + len(block)
        if (
            data.get("offset") != self._offset
            or data.get("nextOffset") != next_offset
            or not block
            or len(block) > requested
            or data.get("eof") is not (next_offset == self._size)
            or sha256(block).hexdigest() != data.get("chunkSha256")
        ):
            raise TransferError("Resource download chunk failed offset or checksum validation")
        self._digest.update(block)
        self._buffer.extend(block)
        self._offset = next_offset
        if self._offset == self._size:
            self._verify_complete()

    def _verify_complete(self) -> None:
        if self._digest.hexdigest() != self._expected:
            raise TransferError("Resource download failed its whole-file checksum")
        self._complete = True
