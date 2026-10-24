# Security policy

## Reporting a vulnerability

Please report it privately through GitHub: open the repository's **Security** tab and choose **Report a
vulnerability**. Do not open a public issue for it. A report that says what you found, how to reproduce it and
which version it affects is enough; we acknowledge it within a few working days and keep you informed until it is
fixed.

## Supported versions

Only the latest release receives fixes. The Tria spot ships as a container image and a Helm chart, both published
from the same tag; update to the newest one.

## What to expect from the spot

- The spot has no shell, no filesystem tools and no cluster RBAC. A cluster is reached only through a connector,
  and the tools it offers are named one by one.
- A credential is always a file path (a mounted Secret or a projected token), never an environment variable or an
  argument.
- The chart renders nothing until the spot is enabled, and its pod runs with the runtime seccomp profile, a
  read-only root filesystem and no privilege escalation.
