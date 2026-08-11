"""what innodb does under load, and what the store owes it: a deadlock is the database asking for the transaction again, and it says so"""

from datetime import timedelta

import pytest
from sqlalchemy.exc import DBAPIError

from fast_tasks.clock import now
from fast_tasks.run import Run
from fast_tasks.store.sqlalchemy import TRIES, contended, under_contention


class Refused(Exception):
    def __init__(self, code: int) -> None:
        super().__init__(code)

        self.args = (code, "refused")


def refusal(code: int) -> DBAPIError:
    return DBAPIError("UPDATE", {}, Refused(code))


@pytest.mark.parametrize("code", [1205, 1213])
def test_the_two_answers_innodb_gives_under_contention_are_recognised(code):
    assert contended(refusal(code)) is True


@pytest.mark.parametrize("code", [1062, 1146])
def test_anything_else_is_not_contention_and_is_never_retried(code):
    assert contended(refusal(code)) is False


def test_an_error_with_nothing_to_say_is_not_contention():
    assert contended(DBAPIError("UPDATE", {}, Exception())) is False


async def test_a_write_the_database_asked_to_repeat_is_repeated():
    tries = []

    async def once():
        tries.append(1)

        if len(tries) < 3:
            raise refusal(1213)

        return "written"

    assert await under_contention(once) == "written"
    assert len(tries) == 3


async def test_a_write_that_keeps_deadlocking_stops_and_says_so():
    tries = []

    async def once():
        tries.append(1)

        raise refusal(1213)

    with pytest.raises(DBAPIError):
        await under_contention(once)

    assert len(tries) == TRIES, "it gave up instead of retrying forever"


async def test_a_claim_the_database_refused_is_rolled_back_before_it_is_tried_again(sql_store):
    """the session is shared by every candidate of the pass, so a failed statement left in it would take the next one down too"""
    written = await sql_store.add(Run(name="greet"))
    calls = []

    async with sql_store.sessions() as session:
        original = session.execute

        async def refused_once(statement, *arguments, **options):
            calls.append(1)

            if len(calls) == 1:
                raise refusal(1213)

            return await original(statement, *arguments, **options)

        session.execute = refused_once

        assert await sql_store.take(session, written.id, "worker-1", timedelta(seconds=60), now()) is True

    assert len(calls) == 2, "it was refused once and written on the second"
    assert (await sql_store.get(written.id)).worker == "worker-1"


async def test_anything_that_is_not_contention_is_raised_at_once():
    tries = []

    async def once():
        tries.append(1)

        raise refusal(1062)

    with pytest.raises(DBAPIError):
        await under_contention(once)

    assert len(tries) == 1, "a duplicate key never gets better by being tried again"
