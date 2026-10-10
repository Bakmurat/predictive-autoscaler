"""The forecasting service's entrypoint (B5e; Codex task-08 r34).

With FORECASTER_TLS_CERT and FORECASTER_TLS_KEY set it serves HTTPS on FORECASTER_PORT (default 8443), otherwise
plain HTTP on FORECASTER_PORT (default 8000). Authentication (api/auth.py) refuses requests that did not arrive over
TLS on its own, whatever starts the process.

Certificate rotation: a cert-manager renewal or a replaced Secret updates the mounted files, but a running server keeps
its old SSL context. The files are checked every CERT_RECHECK_S; when they change, the server shuts down gracefully
and exits, so the kubelet restarts the container with the new certificate (a few seconds without the forecasting
service; the operator falls back to a recent cached forecast or the reactive rule meanwhile).
"""

import logging
import os
import threading

import uvicorn

logger = logging.getLogger("api.serve")
CERT_RECHECK_S = float(os.getenv("FORECASTER_CERT_RECHECK_S", "60"))


def stamp(paths):
    out = []
    for p in paths:
        try:
            st = os.stat(p)                                  # follows the Secret volume's symlinks
            out.append((p, st.st_mtime_ns, st.st_size, st.st_ino))
        except OSError:
            out.append((p, None, None, None))
    return tuple(out)


def watch(server, paths, interval, stop):
    """Ask the server to exit when any of paths changes (checked every interval seconds, until stop is set)."""
    first = stamp(paths)
    while not stop.wait(interval):
        if stamp(paths) != first:
            logger.warning("TLS certificate files changed: restarting to load them")
            server.should_exit = True
            return


def config_from_env(env=os.environ):
    cert, key = env.get("FORECASTER_TLS_CERT", ""), env.get("FORECASTER_TLS_KEY", "")
    tls = bool(cert and key)
    port = int(env.get("FORECASTER_PORT", "8443" if tls else "8000"))
    kwargs = {"host": "0.0.0.0", "port": port, "proxy_headers": False, "forwarded_allow_ips": ""}
    if tls:
        kwargs.update(ssl_certfile=cert, ssl_keyfile=key)
    return kwargs, ([cert, key] if tls else [])


def main():
    kwargs, watched = config_from_env()
    server = uvicorn.Server(uvicorn.Config("api.main:app", **kwargs))
    stop = threading.Event()
    if watched:
        threading.Thread(target=watch, args=(server, watched, CERT_RECHECK_S, stop), daemon=True).start()
    try:
        server.run()
    finally:
        stop.set()


if __name__ == "__main__":
    main()
