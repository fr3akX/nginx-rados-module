 Nginx Rados access module 
 =============
 
 Supports range requests and conditional requests (WIP)
 
 Example usage:
 `
     server {
         listen       80;
         server_name  localhost;
         
         rados_conf "/etc/ceph/ceph.conf";
         rados_pool "data";
 
         location / {
             rados;
         }
     }

 `