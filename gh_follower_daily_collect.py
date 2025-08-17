#!/usr/bin/env python3
import os
import sys


def main() -> None:
    base_dir = os.path.dirname(__file__)
    if base_dir not in sys.path:
        sys.path.insert(0, base_dir)

    from collectors.gh_followers_lib import run_collection  # type: ignore

    run_collection()


if __name__ == "__main__":
    main()


