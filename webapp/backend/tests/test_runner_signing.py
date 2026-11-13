def _runner(store):
    from core.runner import Runner

    runner = Runner.__new__(Runner)
    runner.store = store
    return runner


def test_runner_defers_to_browser_signers(store):
    runner = _runner(store)
    for method in ("nip07", "nip46"):
        store.set_setting("signing_method", method)
        assert runner._client_signer_active() is True


def test_runner_publishes_with_nsec_mode(store):
    runner = _runner(store)
    store.set_setting("signing_method", "nsec")
    assert runner._client_signer_active() is False
    store.set_setting("signing_method", "")
    assert runner._client_signer_active() is False
