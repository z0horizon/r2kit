# R2 and r2kit transfer limits

The table collects limits enforced locally before network I/O. Values reflect
the transfer model used by `r2kit`; consult Cloudflare when configuring bucket
policies, lifecycle rules, or account-level quotas.

| Limit | Value | Where it matters |
|---|---:|---|
| Object key | 1–1,024 UTF-8 bytes | All object operations |
| Single PUT | 5 MiB below 5 GiB | `put_*` and `presign_put*` |
| Multipart part size | 5 MiB through 5 MiB below 5 GiB | Managed and presigned multipart |
| Multipart parts | At most 10,000 | Managed and presigned multipart |
| List page | 1–1,000 keys | `ListObjectsBuilder::limit` |
| Delete request | At most 1,000 keys | Batched automatically by `delete_objects` |
| Presigned lifetime | 1 second through 7 days | Presigned GET, PUT, and upload parts |
| Managed concurrency | 1–64 parts | `ManagedMultipartBuilder::concurrency` |
| Managed attempts | 1–16 per part | `ManagedMultipartBuilder::max_attempts` |
| Default part-buffer budget | 256 MiB | Validated as `part_size × concurrency` |
| Object metadata | At most 8,192 bytes | Typed headers and custom metadata |

For local files larger than a single PUT, prefer managed multipart. For browser
or mobile uploads, use presigned multipart so application servers do not proxy
the object body. Tune part size and concurrency together: higher concurrency
can increase throughput, but each in-flight part consumes one full part buffer.
