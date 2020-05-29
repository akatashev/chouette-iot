from chouette.metrics.plugins.messages import StatsResponse, StatsRequest


def test_stats_response_str_and_repr():
    """
    StatsResponse:
    __str__ and __repr__  methods return the same string.
    """
    msg = StatsResponse("TestProducer", iter([]))
    assert str(msg) == "<StatsResponse from TestProducer>"
    assert repr(msg) == str(msg)


def test_stats_request_str_and_repr(test_actor_class):
    """
    StatsRequest:
    __str__ and __repr__  methods return the same string.
    """
    test_actor = test_actor_class.start()
    msg = StatsRequest(test_actor)
    assert str(msg) == f"<StatsRequest from {test_actor}>"
    assert repr(msg) == str(msg)
