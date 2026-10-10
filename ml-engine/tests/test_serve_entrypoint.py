"""B5e: api/serve.py chooses TLS from the environment and restarts on certificate rotation (Codex task-08 r34)."""

import os
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from api import serve  # noqa: E402


def test_plain_http_without_certificates():
    kwargs, watched = serve.config_from_env({})
    assert kwargs["port"] == 8000 and "ssl_certfile" not in kwargs and watched == []


def test_tls_with_certificates_and_no_trusted_forwarding(tmp_path):
    env = {"FORECASTER_TLS_CERT": str(tmp_path / "tls.crt"), "FORECASTER_TLS_KEY": str(tmp_path / "tls.key")}
    kwargs, watched = serve.config_from_env(env)
    assert kwargs["port"] == 8443 and kwargs["ssl_certfile"].endswith("tls.crt") and kwargs["ssl_keyfile"].endswith("tls.key")
    assert kwargs["proxy_headers"] is False and kwargs["forwarded_allow_ips"] == ""   # the scheme comes from the socket
    assert watched == [env["FORECASTER_TLS_CERT"], env["FORECASTER_TLS_KEY"]]
    assert serve.config_from_env({**env, "FORECASTER_PORT": "9443"})[0]["port"] == 9443


class FakeServer:
    should_exit = False


def run_watch(paths, change=None):
    server, stop = FakeServer(), threading.Event()
    t = threading.Thread(target=serve.watch, args=(server, paths, 0.05, stop), daemon=True)
    t.start()
    time.sleep(0.15)
    if change:
        change()
    time.sleep(0.3)
    stop.set()
    t.join(2)
    return server.should_exit


def test_a_rotated_certificate_restarts_the_server(tmp_path):
    cert = tmp_path / "tls.crt"
    cert.write_text("old")

    def rotate():
        new = tmp_path / "tls.crt.new"
        new.write_text("renewed certificate")
        os.replace(new, cert)            # a new inode, as a Secret volume's symlink swap gives

    assert run_watch([str(cert)], rotate) is True


def test_an_unchanged_certificate_does_not(tmp_path):
    cert = tmp_path / "tls.crt"
    cert.write_text("same")
    assert run_watch([str(cert)]) is False
