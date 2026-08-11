"""the prose goes stale in silence, so the suite is what keeps it honest"""

import pathlib
import re

import pytest

from fast_tasks.store import redis as redis_store
from fast_tasks.store.sqlalchemy import runs

DOCS = sorted(pathlib.Path("docs").glob("*.md")) + [pathlib.Path("README.md")]

# what is named in the prose and belongs to somebody else: python, redis, sqlalchemy, mysql and the tools around them
FOREIGN = {
    "DATETIME",
    "UPDATE",
    "SELECT",
    "INSERT",
    "MIT",
    "AsyncIO",
    "PostgreSQL",
    "SQLAlchemy",
    "FastAPI",
    "Redis",
    "Lua",
    "NAT",
    "socket_timeout",
    "socket_connect_timeout",
    "health_check_interval",
    "retry_on_timeout",
    "from_url",
    "create_async_engine",
    "journal_mode",
    "exec_driver_sql",
    "asyncio.CancelledError",
    "CancelledError",
    "SystemExit",
    "KeyboardInterrupt",
    "sys.exit",
    "Exception",
    "BaseException",
    "TypeError",
    "apply_async",
    "pytest.ini",
    "pyproject.toml",
    "Makefile",
    "FAST_TASKS_REDIS_URL",
    "FAST_TASKS_MYSQL_URL",
    "FAST_TASKS_POSTGRES_URL",
    "make_url",
}


def written() -> str:
    root = pathlib.Path(".")
    files = [path for folder in ("src", "tests") for path in (root / folder).rglob("*.py")]

    return "\n".join(path.read_text() for path in files)


def prose() -> str:
    return "\n".join(path.read_text() for path in DOCS)


def test_the_table_the_documentation_names_is_the_table_the_store_builds():
    """it drifted once already, and a wrong name sends somebody looking for a table nobody has"""
    named = set(re.findall(r"`(fast_tasks_\w+)`", prose()))
    built = {runs.name} | {index.name for index in runs.indexes}

    assert named <= built, f"the documentation names a table or index that does not exist: {sorted(named - built)}"
    assert runs.name in named, "the one table this store builds is worth naming somewhere"


def test_the_redis_keys_the_documentation_names_are_the_ones_the_store_writes():
    """the families the prose lists are read out of the store itself, so a layout that moves takes the documentation with it"""
    named = {piece.split(":")[0] for piece in re.findall(r"`fast_tasks:([\w{}:]+)`", prose())}
    source = pathlib.Path("src/fast_tasks/store/redis.py").read_text()
    written = {family for family in re.findall(r"[':]:(\w+):", source)} | {"leased"}

    assert named, "the redis layout is worth naming somewhere"
    assert named <= written, f"the prose names a key family the store never writes: {sorted(named - written)}"
    assert redis_store.PREFIX == "fast_tasks"


def test_every_symbol_the_documentation_names_still_exists():
    """a rename leaves the prose pointing at nothing, and whoever follows it looks for a function that is gone"""
    cited = {name for name in re.findall(r"`([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*)`", prose()) if "_" in name or name[0].isupper()}
    code = written()

    missing = sorted(name for name in cited - FOREIGN if not re.search(rf"\b{re.escape(name.split('.')[-1])}\b", code))

    assert missing == [], f"the documentation names what the code no longer has: {missing}"


@pytest.mark.parametrize("page", DOCS)
def test_every_page_the_index_points_at_exists(page):
    for link in re.findall(r"\]\((?!http)([^)#]+)\)", page.read_text()):
        target = (page.parent / link).resolve()

        assert target.exists(), f"{page} points at {link}, which is not there"


def headings(page: pathlib.Path) -> list[str]:
    """the `#` inside a fenced block is a comment of the language in it, and never a heading"""
    lines, fenced = [], False

    for line in page.read_text().splitlines():
        if line.startswith("```"):
            fenced = not fenced
        elif not fenced and line.startswith("#"):
            lines.append(line)

    return lines


@pytest.mark.parametrize("page", DOCS)
def test_every_heading_of_every_page_is_marked(page):
    """the pages are read by eye before they are read by word, and one page out of step is the one nobody finds"""
    bare = [line for line in headings(page) if not re.match(r"^#{1,6} [^\w\s]", line)]

    assert bare == [], f"{page} has a heading with nothing to see it by: {bare}"


@pytest.mark.parametrize("page", DOCS)
def test_no_page_marks_two_headings_the_same_way(page):
    marks = [re.match(r"^#{1,6} (\S+)", line).group(1) for line in headings(page)]
    repeated = sorted({mark for mark in marks if marks.count(mark) > 1})

    assert repeated == [], f"{page} uses the same mark for more than one heading: {repeated}"


def test_every_page_of_the_documentation_is_linked_from_the_readme():
    """a page nothing points at is a page nobody reads"""
    linked = set(re.findall(r"\]\(docs/([\w-]+\.md)\)", pathlib.Path("README.md").read_text()))
    present = {path.name for path in pathlib.Path("docs").glob("*.md")} - {"index.md"}

    assert present <= linked, f"the readme does not point at: {sorted(present - linked)}"
