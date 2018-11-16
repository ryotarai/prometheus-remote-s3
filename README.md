# prometheus-remote-s3

[![Docker Repository on Quay](https://quay.io/repository/ryotarai/prometheus-remote-s3/status "Docker Repository on Quay")](https://quay.io/repository/ryotarai/prometheus-remote-s3)

```
+------------+                  +----------------------+       +----+
|            |                  |                      |       |    |
| Prometheus +------------------> prometheus-remote-s3 +-------> S3 |
|            |                  |                      |       |    |
+------------+  Remote Storage  +----------------------+       +----+
                   (/write)
```
