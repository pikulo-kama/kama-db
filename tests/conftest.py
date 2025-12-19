import pytest


@pytest.fixture
def module_patch(get_module_patch):
    return get_module_patch("kdb")
