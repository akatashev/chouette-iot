from time import sleep
from threading import Timer

import pytest

from chouette import Cancellable, Scheduler

PERIODIC_JOB_METHODS = (
    Scheduler.schedule_at_fixed_rate,
    Scheduler.schedule_with_fixed_delay,
)


@pytest.fixture(scope="module", params=PERIODIC_JOB_METHODS)
def periodic_job_method(request):
    """
    Fixture that is used to run the same tests for both
    `schedule_at_fixed_rate`
    and
    `schedule_with_fixed_delay`
    """
    return request.param


@pytest.fixture(params=[None, Timer(1, print, ("Hello world!",))])
def timer(request):
    """
    A Timer fixture for Cancellable tests.
    """
    test_timer = request.param
    yield test_timer
    if isinstance(test_timer, Timer):
        test_timer.cancel()


def test_cancellable_is_cancellable(timer):
    """
    Cancellable can be cancelled.

    GIVEN: We have a fresh cancellable.
    WHEN: We use its .cancel() method.
    THEN: Its cancelled property changes to False.
    """
    cancellable = Cancellable(timer)
    first_check = cancellable.is_cancelled()
    result = cancellable.cancel()
    second_check = cancellable.is_cancelled()
    assert first_check is False
    assert result is True
    assert second_check is True


def test_cancelled_cancellable_is_not_cancellable(timer):
    """
    Cancelled cancellable returns False on cancel.

    GIVEN: We have a cancelled cancellable.
    WHEN: We use its .cancel() method.
    THEN: It returns False.
    """
    cancellable = Cancellable(timer)
    cancellable.cancel()
    result = cancellable.cancel()
    assert result is False


def test_cancellable_set_timer(timer):
    """
    Uncancelled Cancellable can receive new timers.

    GIVEN: We have a working cancellable.
    WHEN: We use its .set_timer method.
    THEN: It returns True.
    AND: Its _timer property contains provided value.
    """
    cancellable = Cancellable(None)
    result = cancellable.set_timer(timer)
    assert result is True
    assert cancellable._timer == timer


def test_cancelled_cancellable_set_timer(timer):
    """
    Cancelled Cancellable can't receive new timers.

    GIVEN: We have a cancelled cancellable.
    WHEN: We use its .set_timer method.
    THEN: It returns False.
    AND: Its _timer property is unchanged.
    """
    cancellable = Cancellable("Not a timer or None")
    cancellable.cancel()
    result = cancellable.set_timer(timer)
    assert result is False
    assert cancellable._timer == "Not a timer or None"


def test_schedule_once_sends_a_message_only_once(test_actor):
    """
    Schedule once method sends a message only once,

    GIVEN: We scheduled to send one message to a fresh actor.
    AND: We waited for twice the specified timeout.
    WHEN: We get number of received messages from the actor.
    THEN: It returns 1.
    """
    Scheduler.schedule_once(0.1, test_actor.tell, "increment")
    sleep(0.15)
    first_count = test_actor.ask("count")
    sleep(0.1)
    second_count = test_actor.ask("count")
    assert first_count == 1
    assert second_count == first_count


def test_schedule_once_is_cancellable(test_actor):
    """
    Schedule once can be cancelled.

    GIVEN: We scheduled to send one message to a fresh actor.
    AND: We cancelled it.
    AND: We waited for twice the specified timeout.
    WHEN: We get number of received messages from the actor.
    THEN: It returns 0.
    """
    cancellable = Scheduler.schedule_once(0.1, test_actor.tell, "increment")
    cancellable.cancel()
    sleep(0.2)
    count = test_actor.ask("count")
    assert count == 0


def test_periodic_job_is_cancellable_before_the_first_run(
    periodic_job_method, test_actor
):
    """
    Periodic schedulers can be cancelled before the first run.

    GIVEN: We scheduled to send messages periodically to an actor.
    AND: We cancelled the scheduler without waiting.
    AND: We waited for twice the specified timeout.
    WHEN: We get number of received messages from the actor.
    THEN: It returns 0.
    """
    cancellable = periodic_job_method(0.1, 0.1, test_actor.tell, "increment")
    cancellable.cancel()
    sleep(0.2)
    count = test_actor.ask("count")
    assert count == 0


def test_periodic_job_is_cancellable_after_the_first_run(
    periodic_job_method, test_actor
):
    """
    Periodic schedulers can be cancelled after the first run.

    GIVEN: We scheduled to send messages periodically to an actor.
    AND: We cancelled the scheduler after waiting for an interval once.
    AND: We waited for the interval once more.
    WHEN: We get number of received messages from the actor.
    THEN: It returns 1.
    """
    cancellable = periodic_job_method(0.1, 0.1, test_actor.tell, "increment")
    sleep(0.15)
    cancellable.cancel()
    first_count = test_actor.ask("count")
    sleep(0.1)
    second_count = test_actor.ask("count")
    assert first_count == 1
    assert second_count == first_count


def test_periodic_job_is_executed_periodically(periodic_job_method, test_actor):
    """
    Periodic schedulers are executed periodically.

    GIVEN: We scheduled to send messages periodically to an actor.
    WHEN: We wait for a specified timeout and check number of received messages.
    THEN: Every check it's increased by one.
    """
    cancellable = periodic_job_method(0.1, 0.1, test_actor.tell, "increment")
    sleep(0.15)
    first_count = test_actor.ask("count")
    sleep(0.1)
    second_count = test_actor.ask("count")
    sleep(0.1)
    third_count = test_actor.ask("count")
    cancellable.cancel()
    assert first_count == 1
    assert second_count == 2
    assert third_count == 3


def test_periodic_job_stops_when_actor_is_stopped(
    periodic_job_method, test_actor_class
):
    """
    Periodic scheduler stops when its addressee dies.

    GIVEN: We scheduled to send metrics periodically to an actor.
    AND: This actor was stopped.
    WHEN: Message is being sent to a stopped actor.
    THEN: There is no exception raised.
    """
    test_actor = test_actor_class.start()
    cancellable = periodic_job_method(0.1, 0.1, test_actor.tell, "increment")
    test_actor.stop()
    sleep(0.15)
    cancellable.cancel()
