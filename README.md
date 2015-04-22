# Nginx Rados access module

 Supports range requests and conditional requests
 Module is nonblocking
 
 Example usage:
 ```
     server {
         listen       80;
         server_name  localhost;
         
         rados_conf "/etc/ceph/ceph.conf";
         rados_pool "data";
 
         location / {
            rados;
            rados_throttle 1m;
         }
     }
 ```