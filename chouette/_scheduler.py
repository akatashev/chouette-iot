"""
chouette.Scheduler object for Gevent-based systems.

It won't work without patching time.sleep with gevent.sleep.
"""
import logging
import time
from threading import Lock
from typing import Any, Callable, Optional

from gevent import spawn_later, Greenlet

__all__ = ["Scheduler", "Cancellable"]

logger = logging.getLogger("chouette")


class Cancellable:
    """
    Signifies a delayed task that can be cancelled.

    Built around a `Greenlet` object and is able to cancel it.
    For periodical jobs its `_timer` property is being updated on
    every run from a separated thread, so to avoid race conditions where
    in the middle of the `cancel` run another thread updates the `_timer`
    property with a non-cancelled timer, Lock is used.
    """

    def __init__(self, timer: Optional[Greenlet]) -> None:
        self._cancelled: bool = False
        self._timer: Optional[Greenlet] = timer
        self._timer_lock: Lock = Lock()

    def is_cancelled(self) -> bool:
        """
        `._cancelled` property getter for external users.

        There is no external setter for this property.

        Returns: Bool that shows whether it is cancelled.
        """
        return self._cancelled

    def set_timer(self, timer: Greenlet) -> bool:
        """
        `_.timer` property setter to update timers for periodic jobs.

        There is no external getter for this property.

        Cancelled Canellable objects shouldn't allow to update their timers.

        Args:
            timer (Timer): Timer object handling a delayed task execution.
        Returns: Bool that shows whether the '_.timer` property was updated.
        """
        with self._timer_lock:
            if self.is_cancelled():
                logger.error(
                    "An attempt to update a timer for a stopped Cancellable happened."
                )
                return False
            self._timer = timer
            return True

    def cancel(self) -> bool:
        """
        Cancels the Cancellable by stopping its timer.

        Returns True only if it is not in a cancelled state.
        If it was requested to cancel an already cancelled Cancellable,
        it returns False to be consistent with a Cancellable object
        from Akka.

        Returns: Bool that shows whether this request actually cancelled
                 the Cancellable timer.
        """
        with self._timer_lock:
            if hasattr(self._timer, "kill"):
                self._timer.kill()
            if self.is_cancelled():
                return False
            self._cancelled = True
            return True


class Scheduler:
    """
    A basic Scheduler service based on Akka Scheduler behaviour.

    Its main purpose is to execute a specified actor once or periodically
    after some delay.
    """

    @staticmethod
    def schedule_once(delay: float, func: Callable, *args: Any) -> Cancellable:
        """
        Takes a Callable function and its arguments and creates a timer to run
        it after `delay` seconds.

        Args:
            delay: How many seconds Scheduler waits before executing a func.
            func: Callable that must be executed.
            args: Arguments for the function provided as func.
        Returns: Cancellable object.
        """
        timer = spawn_later(delay, func, *args)
        return Cancellable(timer)

    @classmethod
    def schedule_at_fixed_rate(
        cls, initial_delay: float, interval: float, func: Callable, *args: Any
    ) -> Cancellable:
        """
        Takes a Callable function and its arguments and creates a timer to be
        run every `interval` seconds.
        This method is precise, so on every run it will try compensate time
        drift to keep the interval between function executions precise.

        Args:
            initial_delay: How many seconds Scheduler waits before the first
                           func execution.
            interval: How many seconds must pass between periodic executions.
            func: Callable that must be executed.
            args: Arguments for the function provided as func.
        Returns: Cancellable object.
        """
        return cls._execute_periodically(
            initial_delay, interval, func, *args, precise=True
        )

    @classmethod
    def schedule_with_fixed_delay(
        cls, initial_delay: float, delay: float, func: Callable, *args: Any
    ) -> Cancellable:
        """
        Takes a Callable function and its arguments and creates a timer to be
        run every `interval` seconds.
        This method is imprecise, so it doesn't try to compensate any time
        drift between calls, so intervals between func executions can be
        different and normally they are a bit longer than specified.

        Args:
            initial_delay: How many seconds Scheduler waits before the first
                           func execution.
            delay: How many seconds must pass between periodic executions.
            func: Callable that must be executed.
            args: Arguments for the function provided as func.
        Returns: Cancellable object.
        """
        return cls._execute_periodically(
            initial_delay, delay, func, *args, precise=False
        )

    @classmethod
    def _execute_periodically(
        cls,
        initial_delay: float,
        interval: float,
        func: Callable,
        *args: Any,
        precise: bool,
    ) -> Cancellable:
        """
        A generic method to handle both precise and imprecise versions of
        periodical func executions.

        It returns a Cancellable object, that can anytime cancel scheduled
        executions by calling its `.cancel()` method. Its `._timer` property
        is being updated via a pseudo-callback `_execute_and_update_cancellable`
        that sends a message and creates a new Timer for another execution.
        To keep it cancellable, this callback replaces an expired timer of
        the returned Cancellable object with this newly created timer.

        Args:
            initial_delay (float): How many seconds we wait before the first execution.
            interval (float): How many seconds we wait between executions.
            func: Callable that must be executed.
            args: Arguments for the function provided as func.
            precise (bool): Defines whether the time drift should be compensated to keep
                            the fixed messaging rate.
        Returns: A Cancellable object.
        """
        cancellable = Cancellable(None)

        started = time.time() + initial_delay

        timer = spawn_later(
            initial_delay,
            cls._execute_and_update_cancellable,
            cancellable,
            interval,
            func,
            *args,
            started=started,
            precise=precise,
        )

        cancellable.set_timer(timer)
        return cancellable

    @classmethod
    def _execute_and_update_cancellable(
        cls,
        cancellable: Cancellable,
        interval: float,
        func: Callable,
        *args: Any,
        started: float,
        precise: bool,
    ) -> None:
        """
        A pseudo-callback function that creates a new Timer for the next
        func execution and updates a Cancellable object with this Timer
        to keep the scheduled activity cancellable.

        If `started` parameter is not None, it tried to be as precise as
        possible and to calculate and compensate time drift from the 'ideal'
        execution time.

        Args:
            cancellable (Cancellable): A previously returned object whose
                `_timer` property we need to update.
            interval (float): How many seconds we wait between executions.
            func: Callable that must be executed.
            args: Arguments for the function provided as func.
            started (Optional[float]): Unix timestamp that says when our
                first execution was happened. If set, it's used to calculate
                and compensate time drift between executions.
        Returns: None, since that's a pseudo-callback.
        """
        now = time.time()
        if precise or now < started:
            time_drift = (now - started) % interval
            delay = interval - time_drift
        else:
            delay = interval

        try:
            if now > started:
                func(*args)
            timer = spawn_later(
                delay,
                cls._execute_and_update_cancellable,
                cancellable,
                interval,
                func,
                *args,
                started=started,
                precise=precise,
            )

            cancellable.set_timer(timer)
        except Exception:
            logger.error("Stopping periodic job because of exception.", exc_info=True)
