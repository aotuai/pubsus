from collections import defaultdict
from enum import Enum
from threading import Event, RLock
from typing import Callable, DefaultDict, List
from weakref import ReferenceType, WeakMethod, ref

SUBSCRIBER = Callable[..., None]
"""A function that can be called when a topic has a new value"""


class SubscriberNotFoundError(Exception):
    pass


class DuplicateSubscriberError(Exception):
    pass


class PubSubMixin:
    """A PubSub mixin that simplifies the process of adding a publisher
    subscriber interface to a service.
    """

    def __init__(self):
        self._subscribers: DefaultDict[Enum, List[ReferenceType[SUBSCRIBER]]]
        self._subscribers = defaultdict(list)
        self._subscribers_lock = RLock()

    def subscribe(self, topic: Enum, subscriber: SUBSCRIBER) -> None:
        """Adds a new subscriber to the given topic.

        Subscribers will be notified of updates in the same thread as the
        publisher, so subscription functions should be thread-safe and should
        avoid doing a lot of work.

        :param topic: The topic the subscriber wants to be notified of
        :param subscriber: A function that will be called when the value of the
            topic changes

        :raises DuplicateSubscriber: When subscribing a listener twice to the
            same resource.
        """
        with self._subscribers_lock:
            if self.is_subscribed(topic, subscriber):
                message = f"A subscriber already exists for the topic {topic}"
                raise DuplicateSubscriberError(message)

            self._subscribers[topic].append(self._to_weakref(subscriber))

    def subscribe_as_event(self, topic: Enum) -> Event:
        """Adds a new subscriber to the given topic and returns an event object
        that is set when the topic is published to. If the publisher provides
        any arguments, they will be discarded.

        :param topic: The topic to trigger the event
        :return: An event that is set when the topic is published to
        """
        event = _PublishEvent()

        with self._subscribers_lock:
            self._subscribers[topic].append(
                self._to_weakref(event.set_ignore_args))

        return event

    def unsubscribe(self, topic: Enum, subscriber: SUBSCRIBER) -> None:
        """Unsubscribe a listener from this topic

        :raises SubscriberNotFound: When a subscriber doesn't exist for this
            particular topic.
        """
        with self._subscribers_lock:
            # Find the weakref that refers to this subscriber
            topic_subscribers = self._subscribers[topic]
            subscriber_weakref = self._to_registered_weakref(topic, subscriber)
            topic_subscribers.remove(subscriber_weakref)

    def publish(self, topic: Enum, *args, **kwargs) -> None:
        """Publishes a new value for a topic.

        :param topic: The topic to update
        :param value: The new value
        """
        with self._subscribers_lock:
            for callback_weakref in self._subscribers[topic].copy():
                callback = callback_weakref()
                if callback is None:
                    # This listener has been garbage collected, clean it out
                    self._subscribers[topic].remove(callback_weakref)
                    continue
                callback(*args, **kwargs)

    def is_subscribed(self, topic: Enum, subscriber: SUBSCRIBER) -> bool:
        try:
            self._to_registered_weakref(topic, subscriber)
        except SubscriberNotFoundError:
            return False
        else:
            return True

    def _to_weakref(self, callable) -> ReferenceType:
        """Return the appropriate WeakRef wrapper for this callable.

        For bound methods, it returns a WeakMethod. For anything else it
        returns a ref()
        """
        if hasattr(callable, "__self__"):
            return WeakMethod(callable)
        else:
            return ref(callable)

    def _to_registered_weakref(self, topic, subscriber) -> ReferenceType:
        """Looks for the weakref for this specifically (already subscribed)
        subscriber. If it's not found, it raises an exception.

        :raises SubscriberNotFound: When a subscriber doesn't exist for this
            particular topic.
        """
        subscriber_weakref = self._to_weakref(subscriber)
        try:
            with self._subscribers_lock:
                weakref = next(s for s in self._subscribers[topic]
                               if s == subscriber_weakref)
        except StopIteration:
            raise SubscriberNotFoundError(
                f"Could not find subscriber {subscriber}"
                f"in {self.__class__.__name__}")
        return weakref


class _PublishEvent(Event):
    """An event with a method that can throw away arguments given to it by a
    publisher, and therefore can be used as a subscriber in
    ``PubSubMixin.subscribe_as_event``.

    Why use this instead of a simple function or lambda that throws away
    arguments and calls ``event.set()`` instead? Using this method ensures that
    the subscription is garbage collected once this event object falls out of
    scope. If we used a weakref to a function or lambda instead, the function
    would fall out of scope immediately after calling
    ``PubSubMixin.subscribe_as_event`` and get garbage collected. If it was a
    regular reference to a function or lambda, it would never fall out of scope
    and callers would need to remember to unsubscribe the event.
    """
    def set_ignore_args(self, *_args, **_kwargs) -> None:
        self.set()
