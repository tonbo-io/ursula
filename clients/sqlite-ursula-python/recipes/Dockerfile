# An application image that can host sqlite-ursula, from python:3.13-slim in one line.
#
# sqlite-ursula needs a Python whose sqlite3 module loads extensions and runs a SQLite the extension
# supports. python:3.13-slim runs Debian's SQLite, which is older, and fails with host_unsupported.
# Alpine 3.23 and newer ship a SQLite that is new enough, so the one change is the base image:
#
#   -FROM python:3.13-slim
#   +FROM python:3.13-alpine
#
# The prebuilt extension for musl is x64 only: on arm64, use Dockerfile.uv.
FROM python:3.13-alpine

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# Fails the build, saying what to change, on a base image that cannot host the extension.
RUN python -m sqlite_ursula
COPY . .
CMD ["python", "main.py"]
