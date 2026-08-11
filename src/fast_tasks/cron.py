from datetime import datetime, timedelta

from fast_tasks.errors import CronError

# minute, hour, day of month, month, day of week
FIELD_RANGES = ((0, 59), (0, 23), (1, 31), (1, 12), (0, 7))

WEEKDAYS = frozenset(range(7))

# the most days each month ever has, february counted as 29 because a leap year is one of the years an expression lives through
MONTH_LENGTHS = (31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)

# eight years of days: a leap day is the furthest any expression has to look, and the century that is not a leap year makes that gap eight and not four
HORIZON = 8 * 366


def number_of(value: str) -> int:
    try:
        return int(value)
    except ValueError as error:
        raise CronError(f"'{value}' is not a number") from error


def values_of(token: str, low: int, high: int) -> set[int]:
    base, sliced, step_spec = token.partition("/")
    step = number_of(step_spec) if sliced else 1

    if step <= 0:
        raise CronError(f"'{token}' steps by {step}, which never advances")

    if base == "*":
        start, end = low, high
    elif "-" in base:
        start_spec, _, end_spec = base.partition("-")
        start, end = number_of(start_spec), number_of(end_spec)
    else:
        start = end = number_of(base)

    if start < low or end > high or start > end:
        raise CronError(f"'{token}' falls outside [{low}, {high}]")

    return set(range(start, end + 1, step))


def field_of(spec: str, low: int, high: int) -> set[int]:
    values: set[int] = set()

    for token in spec.split(","):
        values.update(values_of(token, low, high))

    return values


def reachable(days: set[int], months: set[int], weekdays: set[int]) -> bool:
    """posix joins the two day fields with an or when both are restricted, so a day none of the named months ever has is only fatal while the weekday field is open"""
    if weekdays != WEEKDAYS:
        return True

    return any(day <= MONTH_LENGTHS[month - 1] for month in months for day in days)


def parse(expression: str) -> tuple[set[int], ...]:
    """the five fields of a posix expression, each read into the set of values it stands for"""
    fields = expression.split()

    if len(fields) != 5:
        raise CronError(f"a cron expression has 5 fields and '{expression}' has {len(fields)}")

    parsed = [field_of(field, low, high) for field, (low, high) in zip(fields, FIELD_RANGES)]

    # posix writes sunday as 0 or 7, and the matcher only speaks 0
    if 7 in parsed[4]:
        parsed[4] = (parsed[4] - {7}) | {0}

    # refused where it is written: nothing else here can tell that february has no thirtieth, and the search would walk a year of minutes to find that out on every pass for as long as the process lives
    if not reachable(parsed[2], parsed[3], parsed[4]):
        raise CronError(f"'{expression}' asks for a day that none of the months it names ever has")

    return tuple(parsed)


def on_day(moment: datetime, days, months, weekdays, day_restricted: bool, weekday_restricted: bool) -> bool:
    """posix joins day of month and day of week with an or when both are restricted, and with an and when either is open"""
    if moment.month not in months:
        return False

    day_match = moment.day in days
    weekday_match = (moment.weekday() + 1) % 7 in weekdays

    return day_match or weekday_match if day_restricted and weekday_restricted else day_match and weekday_match


def time_on(moment: datetime, minutes, hours) -> datetime | None:
    """the first minute of this day the expression matches that is not before `moment`, and nothing once the day is past all of them"""
    for hour in sorted(hours):
        if hour < moment.hour:
            continue

        for minute in sorted(minutes):
            if hour > moment.hour or minute >= moment.minute:
                return moment.replace(hour=hour, minute=minute)

    return None


def next_after(expression: str, moment: datetime) -> datetime:
    """the first minute strictly after `moment` that the expression matches. a day that cannot match is skipped whole, because walking it minute by minute is half a million steps to answer what one comparison answers — and a leap day, which is eight years out at worst, is unreachable at any cost per step"""
    minutes, hours, days, months, weekdays = parse(expression)
    day_restricted = days != set(range(FIELD_RANGES[2][0], FIELD_RANGES[2][1] + 1))
    weekday_restricted = weekdays != WEEKDAYS

    candidate = (moment + timedelta(minutes=1)).replace(second=0, microsecond=0)

    for _ in range(HORIZON):
        if on_day(candidate, days, months, weekdays, day_restricted, weekday_restricted):
            found = time_on(candidate, minutes, hours)

            if found is not None:
                return found

        candidate = (candidate + timedelta(days=1)).replace(hour=0, minute=0)

    raise CronError(f"'{expression}' matches nothing in the eight years after {moment}")
