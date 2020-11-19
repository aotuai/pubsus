import gc
from enum import Enum
from queue import Queue

import pytest

from pubsus import DuplicateSubscriberError, PubSubMixin, \
    SubscriberNotFoundError


class Topics(Enum):
    TOPIC_1 = "topic_1"
    TOPIC_2 = "topic_2"


def test_publishing_subscribing():
    """A basic test of Publisher/Subscriber functionality"""
    pubsub = PubSubMixin()
    queue_1 = Queue()
    queue_2 = Queue()

    # Smoke test
    pubsub.subscribe(Topics.TOPIC_1, queue_1.put)
    pubsub.publish(Topics.TOPIC_1, "topic_1_content")
    assert queue_1.qsize() == 1
    assert queue_1.get() == "topic_1_content"

    # Publish to multiple listeners
    pubsub.subscribe(Topics.TOPIC_1, queue_2.put)
    pubsub.publish(Topics.TOPIC_1, "more content")
    assert queue_1.get() == "more content"
    assert queue_2.get() == "more content"

    # Publish to just one of the queues, make sure the other doesn't receive
    # any content
    pubsub.subscribe(Topics.TOPIC_2, queue_1.put)
    pubsub.publish(Topics.TOPIC_2, "content for queue_1 only")
    assert queue_1.get() == "content for queue_1 only"
    assert queue_2.qsize() == 0


def test_gc_listener_gets_removed():
    """A test to make sure that if a listener is garbage collected, it is
    removed from the Queue after the next time publish is called"""
    pubsub = PubSubMixin()
    queue_1 = Queue()

    # Subscribe the listener, queue_1.put, and verify there is a weak ref
    assert len(pubsub._subscribers) == 0
    pubsub.subscribe(Topics.TOPIC_1, queue_1.put)
    assert len(pubsub._subscribers) == 1
    assert len(pubsub._subscribers[Topics.TOPIC_1]) == 1

    # Publish some content
    pubsub.publish(Topics.TOPIC_1, "alex is cool")
    assert queue_1.get() == "alex is cool"

    # Delete the listener and garbage collect
    del queue_1
    gc.collect()

    # Test the listener is deleted the next time something is published
    assert len(pubsub._subscribers[Topics.TOPIC_1]) == 1
    pubsub.publish(Topics.TOPIC_1, "i am a string look at me")
    assert len(pubsub._subscribers[Topics.TOPIC_1]) == 0


def test_removing_listener():
    """Test removing subscribers from a topic works """
    pubsub = PubSubMixin()
    queue_1 = Queue()
    queue_2 = Queue()

    # Send a method to two channels
    pubsub.subscribe(Topics.TOPIC_1, queue_1.put)
    pubsub.subscribe(Topics.TOPIC_1, queue_2.put)
    pubsub.publish(Topics.TOPIC_1, "to_both")
    assert queue_1.get() == "to_both"
    assert queue_2.get() == "to_both"

    # Remove a listener, and make sure it gets nothing
    pubsub.unsubscribe(Topics.TOPIC_1, queue_1.put)
    pubsub.publish(Topics.TOPIC_1, "to_queue_2")
    assert queue_2.get() == "to_queue_2"
    assert queue_1.qsize() == 0

    # Test removing an already removed listener raises an error
    with pytest.raises(SubscriberNotFoundError):
        pubsub.unsubscribe(Topics.TOPIC_1, queue_1.put)


def test_no_duplicate_subscribers():
    pubsub = PubSubMixin()
    queue = Queue()

    pubsub.subscribe(Topics.TOPIC_1, queue.put)

    # This should not be okay
    with pytest.raises(DuplicateSubscriberError):
        pubsub.subscribe(Topics.TOPIC_1, queue.put)

    # This should be okay
    pubsub.subscribe(Topics.TOPIC_2, queue.put)


def test_subscribe_call_counts():
    """Tests that subscribers are notified when a topic's value changes"""
    pubsub = PubSubMixin()

    subscriber1_call_count = 0

    def subscriber1(value: int):
        assert value == 5
        nonlocal subscriber1_call_count
        subscriber1_call_count += 1

    pubsub.subscribe(Topics.TOPIC_1, subscriber1)

    subscriber2_call_count = 0

    def subscriber2(value: int):
        assert value == 5
        nonlocal subscriber2_call_count
        subscriber2_call_count += 1

    pubsub.subscribe(Topics.TOPIC_1, subscriber2)

    pubsub.publish(Topics.TOPIC_1, 5)

    assert subscriber1_call_count == 1
    assert subscriber2_call_count == 1


def test_publish_no_subscribers():
    """Tests that no errors occur when publishing to a topic with no
    subscribers.
    """
    pubsub = PubSubMixin()
    pubsub.publish(Topics.TOPIC_1, 8)


def test_subscribe_as_event():
    """Tests that event subscribers are notified of a publish"""
    pubsub = PubSubMixin()

    event = pubsub.subscribe_as_event(Topics.TOPIC_1)
    assert not event.is_set(), "The event was set before a publish"

    # Tests that the event subscriber receives the publish
    pubsub.publish(Topics.TOPIC_1, "Hello event!", "You should be set")
    assert event.is_set(), "The event was not set after a publish"
    event.clear()

    # Tests that the event subscriber does not receive a publish on a different
    # topic
    pubsub.publish(Topics.TOPIC_2, "This is a topic with no events")
    assert not event.is_set(), \
        "The event was set from a publish on a different topic"


def test_gc_subscribe_as_event_removed():
    """Tests that the event subscription is removed when the event is garbage
    collected.
    """
    pubsub = PubSubMixin()

    event = pubsub.subscribe_as_event(Topics.TOPIC_1)
    assert len(pubsub._subscribers[Topics.TOPIC_1]) == 1

    del event
    gc.collect()

    assert len(pubsub._subscribers[Topics.TOPIC_1]) == 1
    pubsub.publish(Topics.TOPIC_1, "What is good")
    assert len(pubsub._subscribers[Topics.TOPIC_1]) == 0


def test_multiple_subscribe_as_event():
    """Tests that all subscribed events are notified with a single publish"""
    pubsub = PubSubMixin()

    event1 = pubsub.subscribe_as_event(Topics.TOPIC_1)
    event2 = pubsub.subscribe_as_event(Topics.TOPIC_1)

    pubsub.publish(Topics.TOPIC_1, "What's up, my events!")

    assert event1.is_set()
    assert event2.is_set()
