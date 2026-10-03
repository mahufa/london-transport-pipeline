import io


class IterStream(io.RawIOBase):
    def __init__(self, chunks):
        self._chunks = chunks
        self._leftover = b''

    def readable(self):
        return True

    def readinto(self, buffer):
        buffer_len = len(buffer)

        try:
            chunk = self._leftover or next(self._chunks)
        except StopIteration:
            return 0

        out, self._leftover = chunk[:buffer_len], chunk[buffer_len:]
        buffer[:len(out)] = out
        return len(out)
