# nginx-rados-module

```
Rados nginx module
    server {
        listen       80;
        server_name  localhost;

        rados_conf "/etc/ceph/ceph.conf";
        rados_pool "data";

        location /f/ {
            rados;
            rados_throttle 1m;
            add_header Content-Disposition "attachment; filename*=\"UTF-8''$arg_f\"";
        }
}
```
