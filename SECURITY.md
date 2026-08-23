# Security Policy

## Supported Versions

Tachyon is pre-1.0 (currently `0.1.0`). There is one supported line: the
latest commit on `main` and the most recent tagged Docker image
(`adikeshri/tachyon:latest`). Older tags do not receive backports.

## Reporting a Vulnerability

Please do not open a public issue for a security vulnerability.

Use GitHub's private reporting instead:
[Report a vulnerability](https://github.com/adikeshri/tachyon/security/advisories/new)
(Security tab → Advisories → "Report a vulnerability"). If you cannot use
that, email keshri.aditya999@gmail.com.

Include what you'd include in any good bug report: the affected version or
commit, reproduction steps, and the impact as you understand it (e.g. does it
require authentication, does it need network access, what does it expose or
corrupt).

You should get an acknowledgement within a few days. There is no bug bounty.

## Scope

Most relevant here: request parsing and the query language (`tachyon-query`),
the on-disk and WAL formats (`tachyon-storage`, `tachyon-index`), and the API
key auth in `tachyon-server`. Denial of service via a resource-exhausting
query or oversized document is in scope; the fact that an unauthenticated
instance is fully open by default is documented, expected behavior — see
[Configuration](README.md#configuration) in the README — not a
vulnerability to report.
