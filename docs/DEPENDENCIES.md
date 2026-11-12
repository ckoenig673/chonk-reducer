# Dependency Maintenance

Chonk Reducer separates human-edited dependency intent from reproducible locks:

- `requirements.in` lists direct runtime dependencies.
- `requirements.txt` is the exact runtime lock installed in the container.
- `requirements-dev.in` lists direct development and test dependencies.
- `requirements-dev.txt` is the exact Python 3.11 development/test lock used by CI.

Change an `.in` file first. Regenerate the corresponding `.txt` lock in a clean
environment, review all transitive changes, run `pytest -q`, and build the
container before merging. Do not hand-edit a lock without updating or confirming
its source intent.

The `linuxserver/ffmpeg` base in `Dockerfile` is pinned by a readable release tag
and an immutable multi-architecture digest. When updating it, verify the tag and
digest refer to the same manifest, build the image, confirm Python and FFmpeg
start successfully, and run the full test suite. A digest refresh is an explicit
maintenance change; normal builds must not silently move to a newer base image.
