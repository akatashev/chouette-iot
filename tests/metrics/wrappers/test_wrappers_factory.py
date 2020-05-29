import pytest

from chouette.metrics.wrappers import WrappersFactory, SimpleWrapper


@pytest.mark.parametrize(
    "wrapper_name, result", [("simple", SimpleWrapper()), ("none", None)]
)
def test_wrappers_factory(wrapper_name, result):
    """
    WrapperFactory returns a know wrapper object or None.

    Scenario 1:
    WHEN: `get_wrapper` method is executed with a known wrapper name.
    THEN: Instance of this wrappers class is returned.

    Scenario 2:
    WHEN: `get_wrapper` method is executed with an unknown wrapper name.
    THEN: None is returned.
    """
    wrapper = WrappersFactory.get_wrapper(wrapper_name)
    assert wrapper.__class__ == result.__class__
