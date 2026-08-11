class QueueError(Exception):
    """what this library raises, so a caller can tell its failures from the ones of the code it runs"""


class UnknownTask(QueueError):
    """a name nobody registered, which is a typo or a worker running older code than whoever enqueued"""


class PermanentError(QueueError):
    """a handler saying this attempt is the last one: a malformed payload never gets better by being retried"""


class CronError(ValueError):
    """an expression that does not parse, raised where it is written instead of on the night it would first run"""
