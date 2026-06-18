#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later

"""UHD siggen with a small runtime HTTP control API."""

import json
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

try:
    import uhd_siggen_base as base
except ImportError:
    from gnuradio.uhd import uhd_siggen_base as base


def build_args():
    parser = base.setup_argparser()
    group = parser.add_argument_group("REST control")
    group.add_argument("--ctrl-host", default="127.0.0.1")
    group.add_argument("--ctrl-port", type=int, default=5678)
    return parser.parse_args()


def locked(tb, operation):
    if hasattr(tb, "lock"):
        tb.lock()
    try:
        return operation()
    finally:
        if hasattr(tb, "unlock"):
            tb.unlock()


def get_gain(tb):
    if hasattr(tb, "get_gain_or_power"):
        return float(tb.get_gain_or_power())
    return None


def set_gain(tb, value):
    locked(tb, lambda: tb.set_gain_or_power(value))


def get_amplitude(tb):
    return float(tb[base.AMPLITUDE_KEY])


def set_amplitude(tb, value):
    def operation():
        tb[base.AMPLITUDE_KEY] = value

    locked(tb, operation)


class JammerState:
    def __init__(self, tb):
        self.tb = tb
        self.lock = threading.RLock()
        self.target_gain = get_gain(tb) or 0.0
        self.target_amplitude = get_amplitude(tb)
        if self.target_amplitude <= 0:
            raise ValueError("configured siggen amplitude must be greater than zero")
        self.muted = False

    def off(self):
        with self.lock:
            set_amplitude(self.tb, 0.0)
            set_gain(self.tb, 0.0)
            self.muted = True
            return self.status()

    def on(self, gain):
        with self.lock:
            self.target_gain = float(gain)
            set_gain(self.tb, self.target_gain)
            set_amplitude(self.tb, self.target_amplitude)
            self.muted = False
            return self.status()

    def status(self):
        status = {
            "ok": True,
            "muted": self.muted,
            "target_gain": self.target_gain,
            "target_amplitude": self.target_amplitude,
            "gain_or_power": get_gain(self.tb),
            "amplitude": get_amplitude(self.tb),
            "mode": getattr(self.tb, "gain_type", "unknown"),
        }
        try:
            status["tx_freq"] = float(self.tb[base.TX_FREQ_KEY])
        except Exception:
            pass
        return status


class CtrlHandler(BaseHTTPRequestHandler):
    def _reply(self, status_code, payload):
        data = json.dumps(payload).encode()
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _ok(self, payload):
        self._reply(200, payload)

    def _bad(self, status_code, message):
        self._reply(status_code, {"ok": False, "error": message})

    def do_GET(self):
        try:
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)
            if parsed.path == "/health":
                return self._ok({"ok": True})
            if parsed.path == "/status":
                return self._ok(self.server.jammer.status())
            if parsed.path == "/off":
                return self._ok(self.server.jammer.off())
            if parsed.path == "/on":
                if "gain" not in query:
                    return self._bad(400, "missing query param 'gain'")
                return self._ok(self.server.jammer.on(float(query["gain"][0])))
            if parsed.path == "/setgain":
                if "gain" not in query:
                    return self._bad(400, "missing query param 'gain'")
                gain = float(query["gain"][0])
                set_gain(self.server.jammer.tb, gain)
                self.server.jammer.target_gain = gain
                return self._ok({"ok": True, "applied_gain": gain})
            if parsed.path == "/setpower":
                if "dbm" not in query:
                    return self._bad(400, "missing query param 'dbm'")
                dbm = float(query["dbm"][0])
                set_gain(self.server.jammer.tb, dbm)
                return self._ok({"ok": True, "applied_power_dbm": dbm})
            return self._bad(404, "unknown endpoint")
        except (TypeError, ValueError) as exc:
            return self._bad(400, str(exc))
        except Exception as exc:
            return self._bad(500, f"{type(exc).__name__}: {exc}")

    def log_message(self, *_args, **_kwargs):
        pass


def start_http(jammer, host, port):
    httpd = HTTPServer((host, port), CtrlHandler)
    httpd.jammer = jammer
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    return httpd


def main():
    args = build_args()
    tb = base.USRPSiggen(args)
    jammer = JammerState(tb)
    jammer.off()
    httpd = start_http(jammer, args.ctrl_host, args.ctrl_port)
    print(
        f"[REST] HTTP control at http://{args.ctrl_host}:{args.ctrl_port} "
        "(endpoints: /health /status /on /off /setgain /setpower)"
    )
    try:
        tb.start()
        tb.wait()
    except KeyboardInterrupt:
        pass
    finally:
        try:
            jammer.off()
        finally:
            httpd.shutdown()
            try:
                tb.stop()
            except Exception:
                pass
            tb.wait()


if __name__ == "__main__":
    main()
