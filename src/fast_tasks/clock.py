from datetime import datetime, timezone

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def now() -> datetime:
    """every instant this library writes down is utc, so two machines in two zones agree on when a run is due"""
    return datetime.now(timezone.utc)


def as_utc(value: datetime | None) -> datetime | None:
    """a naive value is read as utc, because that is the only thing a column without an offset can mean"""
    if value is None:
        return None

    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)

    return value.astimezone(timezone.utc)


def naive_utc(value: datetime | None) -> datetime | None:
    """mysql holds no offset, so what reaches a driver is always the naive utc instant"""
    converted = as_utc(value)

    return None if converted is None else converted.replace(tzinfo=None)
