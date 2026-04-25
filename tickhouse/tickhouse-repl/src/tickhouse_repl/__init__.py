import argparse
from tickhouse_repl.repl import start

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 7474

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Tickhouse REPL client",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--host", default=DEFAULT_HOST, help="Server host")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Server port")
    args = parser.parse_args()

    start(host=args.host, port=args.port)
