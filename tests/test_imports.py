def test_config_imports():
    """Verify that the core config and app can be imported."""
    from src.config import app

    assert app is not None


def test_math_logic():
    """A simple dummy test to verify pytest is working."""
    assert 1 + 1 == 2
