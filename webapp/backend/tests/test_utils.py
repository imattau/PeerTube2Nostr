import pytest
from ..core.utils import UrlNormaliser, parse_any_timestamp

def test_url_normalisation(normaliser):
    assert normaliser.normalise_http_url("HTTPS://EXAMPLE.COM/") == "https://example.com/"
    assert normaliser.normalise_relay_url("WSS://RELAY.DAMUS.IO/") == "wss://relay.damus.io/"
    
    with pytest.raises(ValueError):
        normaliser.normalise_http_url("ftp://invalid.com")

def test_extract_channel_ref(normaliser):
    base, channel = normaliser.extract_channel_ref("https://peertube.social/c/the_channel")
    assert base == "https://peertube.social"
    assert channel == "the_channel"

def test_parse_any_timestamp():
    assert parse_any_timestamp(1700000000) == 1700000000
    assert parse_any_timestamp("2024-01-01T00:00:00Z") == 1704067200
    assert parse_any_timestamp(None) is None
