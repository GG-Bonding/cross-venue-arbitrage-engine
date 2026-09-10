import pytest

from arbitrage.persistence.session_lock import SessionLock


def test_same_database_cannot_have_two_paper_sessions(tmp_path):
    path = tmp_path / "paper.db"
    with SessionLock(path):
        with pytest.raises(RuntimeError, match="already running"):
            with SessionLock(path):
                pytest.fail("A second session acquired an occupied database")
        with SessionLock(tmp_path / "other.db"):
            pass
    with SessionLock(path):
        pass  # Released after shutdown; leftover lock file is not an active lock.


def test_lock_is_released_after_runner_error(tmp_path):
    path = tmp_path / "paper.db"
    with pytest.raises(ValueError, match="runner failed"):
        with SessionLock(path):
            raise ValueError("runner failed")
    with SessionLock(path):
        pass
