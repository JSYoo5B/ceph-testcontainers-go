"""Edit the WSL 2 resource and module keys of a .wslconfig file and undo exactly that edit.

The file belongs to the user and configures every WSL 2 distribution of the
account, so this module changes as little as it can: it rewrites or adds only
the [wsl2] keys it is asked to raise or extend, keeps every other byte (encoding, byte
order mark, line endings, comments, key spelling), records what it wrote in a
state file next to the config, and restores from that record line by line.
Anything it cannot read unambiguously is refused instead of guessed.
"""

import json
import os
import re
import tempfile

SECTION = "wsl2"
SIZE_KEYS = ("memory", "swap")
COUNT_KEYS = ("processors",)
# Keys whose value is a comma-separated list that is extended, never shortened.
LIST_KEYS = ("loadkernelmodules",)
MANAGED_KEYS = SIZE_KEYS + COUNT_KEYS + LIST_KEYS
# Keys are matched without regard to case; a key this module adds is spelled
# as the WSL documentation spells it.
SPELLING = {"loadkernelmodules": "loadKernelModules"}
STATE_VERSION = 1
STATE_SUFFIX = ".ceph-testcontainers.state.json"
BACKUP_SUFFIX = ".ceph-testcontainers.bak"

_BOMS = (
    (b"\xef\xbb\xbf", "utf-8"),
    (b"\xff\xfe", "utf-16-le"),
    (b"\xfe\xff", "utf-16-be"),
)
_EOL = re.compile(r"(\r\n|\n|\r)")
_SECTION_LINE = re.compile(r"^\s*\[([^\[\]]+)\]\s*(?:[#;].*)?$")
_KEY_LINE = re.compile(r"^(\s*)([A-Za-z0-9_.]+)(\s*=\s*)(.*?)\s*$")
_SIZE = re.compile(r"^(\d+)\s*([KMGT]?B)?$", re.IGNORECASE)
_LIST_ITEM = re.compile(r"^[A-Za-z0-9_-]+$")
_UNITS = {"": 1, "B": 1, "KB": 1 << 10, "MB": 1 << 20, "GB": 1 << 30, "TB": 1 << 40}


class Refusal(Exception):
    """The request is not safe to carry out; nothing was written.

    code is a stable word for automation, and the message never contains a
    path or a value taken from the file.
    """

    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


class PartialFailure(Exception):
    """A write failed after another one had succeeded.

    rolled_back tells whether the config file is back to its previous bytes.
    """

    def __init__(self, message, rolled_back):
        super().__init__(message)
        self.rolled_back = rolled_back


def parse_size(text):
    """Bytes of a WSL size value ("8GB", "512MB", plain bytes), or None."""
    match = _SIZE.match(text.strip())
    if not match:
        return None
    return int(match.group(1)) * _UNITS[(match.group(2) or "").upper()]


def format_size(size):
    """The shortest WSL size value that is exactly size bytes."""
    for unit in ("GB", "MB", "KB"):
        if size % _UNITS[unit] == 0:
            return "%d%s" % (size // _UNITS[unit], unit)
    return str(size)


def parse_value(key, text):
    """The comparable number, or for a list key the list, of a managed key's value, or None."""
    if key in SIZE_KEYS:
        return parse_size(text)
    if key in LIST_KEYS:
        items = [item.strip() for item in text.split(",")] if text.strip() else []
        return items if all(_LIST_ITEM.match(item) for item in items) else None
    text = text.strip()
    return int(text) if text.isdigit() else None


def format_value(key, value):
    if key in LIST_KEYS:
        return ",".join(value)
    return format_size(value) if key in SIZE_KEYS else str(value)


def _split(line):
    """(body, line ending) of one physical line."""
    for ending in ("\r\n", "\n", "\r"):
        if line.endswith(ending):
            return line[:-len(ending)], ending
    return line, ""


class Document:
    """The decoded lines of a .wslconfig file, each with its own line ending."""

    def __init__(self, lines, encoding, bom, newline):
        self.lines = lines
        self.encoding = encoding
        self.bom = bom
        self.newline = newline

    @classmethod
    def parse(cls, data, newline="\n"):
        """Decode and check data. newline is used only when data has none."""
        encoding, bom = "utf-8", b""
        for mark, name in _BOMS:
            if data.startswith(mark):
                encoding, bom = name, mark
                break
        try:
            text = data[len(bom):].decode(encoding)
        except UnicodeDecodeError:
            raise Refusal("encoding", "the file is not UTF-8 or BOM-marked UTF-16 text") from None
        if "\x00" in text:
            raise Refusal("encoding", "the file contains NUL characters (UTF-16 without a byte order mark?)")
        parts = _EOL.split(text)
        lines = [parts[i] + (parts[i + 1] if i + 1 < len(parts) else "") for i in range(0, len(parts), 2)]
        lines = [line for line in lines if line]
        crlf = sum(1 for line in lines if line.endswith("\r\n"))
        endings = sum(1 for line in lines if _split(line)[1])
        if endings:
            newline = "\r\n" if crlf * 2 >= endings else "\n"
        document = cls(lines, encoding, bom, newline)
        document._check()
        return document

    def render(self):
        return self.bom + "".join(self.lines).encode(self.encoding)

    def _entries(self):
        """Yield (index, kind, section, key, match) for every line."""
        section = None
        for index, line in enumerate(self.lines):
            body = _split(line)[0]
            stripped = body.strip()
            if not stripped:
                yield index, "blank", section, None, None
            elif stripped[0] in "#;":
                yield index, "comment", section, None, None
            elif _SECTION_LINE.match(body):
                section = _SECTION_LINE.match(body).group(1).strip().lower()
                yield index, "section", section, None, None
            elif _KEY_LINE.match(body):
                match = _KEY_LINE.match(body)
                yield index, "key", section, match.group(2).lower(), match
            else:
                yield index, "other", section, None, None

    def _check(self):
        sections, keys = 0, set()
        for index, kind, section, key, _ in self._entries():
            if kind == "other":
                raise Refusal("malformed", "line %d is not a section, a key=value pair or a comment" % (index + 1))
            if kind == "key" and section is None:
                raise Refusal("malformed", "line %d sets a key before any section" % (index + 1))
            if kind == "section" and section == SECTION:
                sections += 1
                if sections > 1:
                    raise Refusal("ambiguous", "the file has more than one [wsl2] section")
            if kind == "key" and section == SECTION and key in MANAGED_KEYS:
                if key in keys:
                    raise Refusal("ambiguous", "[wsl2] sets %s more than once" % key)
                keys.add(key)

    def _section(self):
        """(header index, index after the last key, whether it holds a key or comment), or None."""
        header = after = None
        content = False
        for index, kind, section, _, _ in self._entries():
            if kind == "section":
                if header is not None:
                    break
                if section == SECTION:
                    header, after = index, index + 1
            elif header is not None and kind == "key":
                after, content = index + 1, True
            elif header is not None and kind == "comment":
                content = True
        return None if header is None else (header, after, content)

    def find(self, key):
        """(line index, regex match) of a managed key in [wsl2], or None."""
        for index, kind, section, name, match in self._entries():
            if kind == "key" and section == SECTION and name == key:
                return index, match
        return None

    def body(self, key):
        """The line of a managed key without its line ending, or None."""
        found = self.find(key)
        return None if found is None else _split(self.lines[found[0]])[0]

    def value(self, key):
        """The number or list a managed key is set to, or None when unset.

        A value that is set but not understood is refused: comparing it or
        overwriting it would be a guess.
        """
        found = self.find(key)
        if found is None:
            return None
        number = parse_value(key, found[1].group(4))
        if number is None:
            raise Refusal("ambiguous", "[wsl2] %s has a value this tool does not understand" % key)
        return number

    def set(self, key, text, record):
        """Write key=text into [wsl2], creating the key or the section.

        record collects what a later restore needs to know.
        """
        found = self.find(key)
        if found is not None:
            index, match = found
            body, ending = _split(self.lines[index])
            record["keys"].setdefault(key, {"previous": body})
            written = match.group(1) + match.group(2) + match.group(3) + text
            self.lines[index] = written + ending
            record["keys"][key]["written"] = written
            return
        section = self._section()
        if section is None:
            self._terminate(len(self.lines), record)
            self.lines.append("[%s]%s" % (SECTION, self.newline))
            record["added_section"] = True
            position = len(self.lines)
        else:
            position = section[1]
            self._terminate(position, record)
        written = "%s=%s" % (SPELLING.get(key, key), text)
        self.lines.insert(position, written + self.newline)
        record["keys"].setdefault(key, {"previous": None})
        record["keys"][key]["written"] = written

    def _terminate(self, position, record):
        """Give the line before position a line ending if it is the unterminated last one."""
        if position and position == len(self.lines) and not _split(self.lines[-1])[1]:
            self.lines[-1] += self.newline
            record["terminated_last_line"] = True

    def unset(self, record):
        """Undo what set recorded. The caller has checked that the lines are unchanged."""
        tail = False
        for key, entry in record["keys"].items():
            index = self.find(key)[0]
            if entry["previous"] is None:
                tail = tail or index == len(self.lines) - 1
                del self.lines[index]
            else:
                self.lines[index] = entry["previous"] + _split(self.lines[index])[1]
        if record.get("added_section"):
            header, _, content = self._section()
            if not content:
                tail = tail or header == len(self.lines) - 1
                del self.lines[header]
        if record.get("terminated_last_line") and tail and self.lines:
            self.lines[-1] = _split(self.lines[-1])[0]


class Files:
    """The file operations of apply and restore, replaceable in tests."""

    def read(self, path):
        """The bytes of path, or None when it does not exist."""
        try:
            with open(path, "rb") as handle:
                return handle.read()
        except FileNotFoundError:
            return None

    def write_atomic(self, path, data):
        """Replace path with data through a temporary file in the same directory."""
        directory = os.path.dirname(path) or "."
        descriptor, temporary = tempfile.mkstemp(prefix=".ceph-testcontainers-", dir=directory)
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            if os.path.exists(path):
                os.chmod(temporary, os.stat(path).st_mode & 0o777)
            os.replace(temporary, path)
        except BaseException:
            if os.path.exists(temporary):
                os.remove(temporary)
            raise

    def create_exclusive(self, path, data):
        """Write a new file and fail when path already exists."""
        with open(path, "xb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())

    def remove(self, path):
        try:
            os.remove(path)
        except FileNotFoundError:
            pass

    def writable_regular_file(self, path):
        """None when path can be edited in place, else why not."""
        if os.path.islink(path):
            return "it is a symbolic link"
        if not os.path.exists(path):
            return None if os.access(os.path.dirname(path) or ".", os.W_OK) else "its directory is not writable"
        if not os.path.isfile(path):
            return "it is not a regular file"
        if not os.access(path, os.W_OK):
            return "it is read-only"
        return None


def state_path(path):
    return path + STATE_SUFFIX


def backup_path(path):
    return path + BACKUP_SUFFIX


def load_state(path, files=None):
    """The record of this tool's edit of the config at path, or None."""
    data = (files or Files()).read(state_path(path))
    if data is None:
        return None
    try:
        state = json.loads(data.decode("utf-8"))
        valid = (state["version"] == STATE_VERSION and isinstance(state["keys"], dict) and state["keys"]
                 and all(key in MANAGED_KEYS and isinstance(entry["written"], str)
                         and (entry["previous"] is None or isinstance(entry["previous"], str))
                         for key, entry in state["keys"].items()))
    except (ValueError, KeyError, TypeError):
        valid = False
    if not valid:
        raise Refusal("state", "the state file of this tool is unreadable; review the config by hand")
    return state


def _changed_keys(document, state):
    """Owned keys whose line no longer is what this tool wrote."""
    return sorted(key for key, entry in state["keys"].items() if document.body(key) != entry["written"])


def inspect(path, files=None, newline="\n"):
    """(document or None when the file is missing, state or None), without writing."""
    files = files or Files()
    data = files.read(path)
    state = load_state(path, files)
    return (None if data is None else Document.parse(data, newline)), state


def plan(document, wanted):
    """Keys of wanted that the document must raise, as {key: value text}.

    wanted maps a managed key to the least value that is enough, or for a
    list key to the items the list must contain. A key that is already set to
    that much or more is left alone, and a list keeps the items it has.
    """
    changes = {}
    for key in MANAGED_KEYS:
        if key not in wanted:
            continue
        current = None if document is None else document.value(key)
        if key in LIST_KEYS:
            present = [item.replace("-", "_") for item in current or []]
            missing = [item for item in wanted[key] if item.replace("-", "_") not in present]
            if missing:
                changes[key] = format_value(key, (current or []) + missing)
        elif current is None or current < wanted[key]:
            changes[key] = format_value(key, wanted[key])
    return changes


def apply(path, wanted, files=None, newline="\n"):
    """Raise the wanted keys in the config at path. Returns the keys written.

    The first edit copies the previous file to a backup next to it. A second
    call that finds nothing to raise writes nothing at all.
    """
    files = files or Files()
    reason = files.writable_regular_file(path)
    if reason:
        raise Refusal("not-writable", "the config file cannot be edited in place: " + reason)
    original = files.read(path)
    state = load_state(path, files)
    document = Document.parse(original or b"", newline)
    if state is None and files.read(backup_path(path)) is not None:
        raise Refusal("state", "a backup of this tool exists without its state file; review both by hand")
    if state is not None:
        changed = _changed_keys(document, state)
        if changed:
            raise Refusal("owned-key-changed",
                          "[wsl2] %s changed after this tool wrote it; restore or edit by hand" % ", ".join(changed))
    changes = plan(document if original is not None else None, wanted)
    if not changes:
        return []
    record = state or {"version": STATE_VERSION, "created_file": original is None, "added_section": False,
                       "terminated_last_line": False, "keys": {}}
    for key, text in changes.items():
        document.set(key, text, record)
    made_backup = state is None and original is not None
    if made_backup:
        files.create_exclusive(backup_path(path), original)
    try:
        if files.read(path) != original:
            raise Refusal("concurrent-edit", "the config file changed while this tool was running")
        files.write_atomic(path, document.render())
    except BaseException:
        if made_backup:
            files.remove(backup_path(path))
        raise
    try:
        files.write_atomic(state_path(path), json.dumps(record, indent=2, sort_keys=True).encode("utf-8"))
    except Exception as error:
        raise _roll_back(files, path, original, made_backup, error) from error
    return sorted(changes)


def _roll_back(files, path, original, made_backup, error):
    """Put the config back after the state file could not be recorded."""
    try:
        if original is None:
            files.remove(path)
        else:
            files.write_atomic(path, original)
        if made_backup:
            files.remove(backup_path(path))
    except Exception:
        return PartialFailure(
            "the config was written but its state file was not, and putting the config back failed (%s); "
            "compare the config with the backup next to it" % type(error).__name__, rolled_back=False)
    return PartialFailure("the state file could not be written (%s); the config is back to its previous content"
                          % type(error).__name__, rolled_back=True)


def restore(path, files=None, newline="\n"):
    """Undo this tool's edit of the config at path. Returns the keys restored.

    Every other line stays as it is now, so edits made after the apply
    survive. An owned key that was edited or removed since is refused.
    """
    files = files or Files()
    state = load_state(path, files)
    if state is None:
        raise Refusal("no-state", "this tool has no recorded change to undo")
    reason = files.writable_regular_file(path)
    if reason:
        raise Refusal("not-writable", "the config file cannot be edited in place: " + reason)
    original = files.read(path)
    if original is None:
        raise Refusal("owned-key-changed", "the config file this tool edited no longer exists")
    document = Document.parse(original, newline)
    changed = _changed_keys(document, state)
    if changed:
        raise Refusal("owned-key-changed",
                      "[wsl2] %s changed after this tool wrote it; restoring would overwrite that edit"
                      % ", ".join(changed))
    document.unset(state)
    if state.get("created_file") and not "".join(document.lines).strip():
        files.remove(path)
    else:
        files.write_atomic(path, document.render())
    try:
        files.remove(state_path(path))
        files.remove(backup_path(path))
    except Exception as error:
        raise PartialFailure("the config is restored, but the state or backup file next to it could not be "
                             "removed (%s); delete them by hand" % type(error).__name__, rolled_back=False) from error
    return sorted(state["keys"])
