#!/usr/bin/env python3
import os
import sys
import logging


def main() -> None:
    log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        stream=sys.stdout,
        force=True,
    )
    logger = logging.getLogger(__name__)
    logger.info("LOG_LEVEL=%s", log_level_str)
    base_dir = os.path.dirname(__file__)
    if base_dir not in sys.path:
        sys.path.insert(0, base_dir)

    from collectors.gh_followers_lib import run_collection  # type: ignore
    logger.info("gh_follower_daily_collect entrypoint started")
    try:
        run_collection()
        logger.info("gh_follower_daily_collect completed successfully")
    except Exception:
        logger.exception("gh_follower_daily_collect failed with an exception")
        import sys as _sys
        _sys.exit(1)


if __name__ == "__main__":
    main()


