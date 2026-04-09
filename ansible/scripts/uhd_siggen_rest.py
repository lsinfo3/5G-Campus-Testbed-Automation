#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
"""
UHD Siggen with a tiny runtime HTTP API.

Endpoints:
  GET /health
  GET /status
  GET /setgain?gain=<dB>        # forces gain path if available
  GET /setpower?dbm=<dBm>       # uses power mode path (amplitude-corrected)

Examples:
  ./uhd_siggen_rest -f 3619200000 --gaussian -s 20M -g 1 -m 1 --ctrl-port 5678
  curl 'http://127.0.0.1:5678/setgain?gain=70'
"""

import json
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

try:
    import uhd_siggen_base as base
except ImportError:
    from gnuradio.uhd import uhd_siggen_base as base

# We’ll extend the base parser with control-plane args.
def build_args():
    parser = base.setup_argparser()  # provided by your base
    group = parser.add_argument_group("REST control")
    group.add_argument("--ctrl-host", default="127.0.0.1",
                       help="HTTP control host (default: 127.0.0.1)")
    group.add_argument("--ctrl-port", type=int, default=5678,
                       help="HTTP control port (default: 5678)")
    return parser.parse_args()

def safe_set_gain(tb, val):
    # Prefer explicit set_gain if available; otherwise fall back to set_gain_or_power
    if hasattr(tb, "lock"):
        tb.lock()
    try:
        if hasattr(tb, "set_gain"):
            print("using set_gain")
            # FIXME
            # tb.set_gain(val)
            tb.set_gain_or_power(val)
        else:
            print("using set_gain_or_power")
            tb.set_gain_or_power(val)  # base decides per gain_type
    finally:
        if hasattr(tb, "unlock"):
            tb.unlock()

def safe_set_power(tb, dbm):
    # Let the base convert requested dBm to power reference using amplitude offset
    if hasattr(tb, "lock"):
        tb.lock()
    try:
        tb.set_gain_or_power(dbm)
    finally:
        if hasattr(tb, "unlock"):
            tb.unlock()
			
def get_status(tb):
    # Minimal, robust status snapshot
    status = {"ok": True}
    # Current effective gain-or-power (base reports gain or power depending on mode)
    if hasattr(tb, "get_gain_or_power"):
        status["gain_or_power"] = float(tb.get_gain_or_power())
    # Try to expose some common bits if present
    status["mode"] = getattr(tb, "gain_type", "unknown")
    # Frequency & amplitude if accessible via pubsub mapping
    try:
        status["tx_freq"] = float(tb[base.TX_FREQ_KEY])
        status["amplitude"] = float(tb[base.AMPLITUDE_KEY])
    except Exception:
        pass
    return status

class CtrlHandler(BaseHTTPRequestHandler):
    def _ok(self, obj):
        data = json.dumps(obj).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _bad(self, code, msg):
        data = json.dumps({"ok": False, "error": msg}).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        try:
            parsed = urlparse(self.path)
            qs = parse_qs(parsed.query)
            if parsed.path == "/health":
                return self._ok({"ok": True})
            if parsed.path == "/status":
                return self._ok(get_status(self.server.tb))
            if parsed.path == "/setgain":
                if "gain" not in qs:
                    return self._bad(400, "missing query param 'gain'")
                try:
                    gain = float(qs["gain"][0])
                except Exception:
                    return self._bad(400, "invalid 'gain' value")
                safe_set_gain(self.server.tb, gain)
                return self._ok({"ok": True, "applied_gain": gain})
            if parsed.path == "/setpower":
                if "dbm" not in qs:
                    return self._bad(400, "missing query param 'dbm'")
                try:
                    dbm = float(qs["dbm"][0])
                except Exception:
                    return self._bad(400, "invalid 'dbm' value")
                safe_set_power(self.server.tb, dbm)
                return self._ok({"ok": True, "applied_power_dbm": dbm})
            return self._bad(404, "unknown endpoint")
        except Exception as e:
            return self._bad(500, f"{type(e).__name__}: {e}")
			
    def log_message(self, *_args, **_kwargs):
        # keep console quiet; comment out to enable HTTP logs
        pass			
			

def start_http(tb, host, port):
    httpd = HTTPServer((host, port), CtrlHandler)
    httpd.tb = tb
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    return httpd

def main():
    args = build_args()
    # Build the flowgraph using the base’s USRPSiggen
    tb = base.USRPSiggen(args)  # sets up USRP, publishers, etc.
    httpd = start_http(tb, args.ctrl_host, args.ctrl_port)
    print(f"[REST] HTTP control at http://{args.ctrl_host}:{args.ctrl_port} "
          "(endpoints: /health /status /setgain /setpower)")
    try:
        tb.start()
        tb.wait()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.shutdown()
        try:
            tb.stop()
        except Exception:
            pass
        tb.wait()

if __name__ == "__main__":
    main()