"""Spark entry point outside the package, avoiding sibling stdlib-name shadowing."""
from marketpulse_app.journey_publish import main

if __name__ == "__main__":
    raise SystemExit(main())
