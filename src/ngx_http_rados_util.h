#ifndef H_NGX_HTTP_RADOS_UTIL
#define H_NGX_HTTP_RADOS_UTIL

#include <ngx_http.h>

/**
* Range request header parser
*/
void http_parse_range(ngx_http_request_t* r, ngx_str_t* range_str, uint64_t* range_start, uint64_t* range_end, size_t content_length);

/**
* Tests If-Mofidied-Since against provided timestamp
*/
ngx_uint_t ngx_http_test_if_modified(ngx_http_request_t *r);


/*
* Retrieves request key
*/
ngx_uint_t nginx_http_get_rados_key(ngx_http_request_t *request, char **value);

#endif
