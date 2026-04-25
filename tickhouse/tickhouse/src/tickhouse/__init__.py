import argparse
from tickhouse.server import start

DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 7474

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Tickhouse time-series database server",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--host", default=DEFAULT_HOST, help="Bind address")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="TCP port")
    parser.add_argument("--data-dir", default="data", help="Root data directory")
    args = parser.parse_args()

    start(host=args.host, port=args.port, data_dir=args.data_dir)
