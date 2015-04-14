# Nginx Rados access module

 Supports range requests and conditional requests
 Module is sync for now so spin up lot of workers to handle concurrent clients
 
 Example usage:
 ```
     server {
         listen       80;
         server_name  localhost;
         
         rados_conf "/etc/ceph/ceph.conf";
         rados_pool "data";
 
         location / {
             rados;
         }
     }
 ```