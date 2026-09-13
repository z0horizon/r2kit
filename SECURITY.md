# Security policy

## Reporting a vulnerability

Please do not open a public issue for a suspected credential leak, presigned URL
exposure, request-signing flaw, or other vulnerability. Use GitHub's private
security advisory flow for `z0horizon/r2kit` instead.

Include the affected version or commit, a minimal reproduction, expected
impact, and whether any real R2 credential or presigned URL was exposed. Revoke
and rotate exposed Cloudflare credentials immediately; do not include them in
the report.

## Scope

Until the first stable release, security fixes are made on the latest release
line only. Presigned URLs are bearer credentials. Applications remain
responsible for authorization before issuing them, short expirations, CORS,
rate limiting, and lifecycle cleanup of abandoned multipart uploads.
