#include <ngx_http.h>

static char h_digit(char hex) {
    return (hex >= '0' && hex <= '9') ? hex - '0': ngx_tolower(hex)-'a'+10;
}

static int htoi(char* h) {
    char ok[] = "0123456789AaBbCcDdEeFf";

    if (ngx_strchr(ok, h[0]) == NULL || ngx_strchr(ok,h[1]) == NULL) { return -1; }
    return h_digit(h[0])*16 + h_digit(h[1]);
}

static int url_decode(char * filename) {
    char * read = filename;
    char * write = filename;
    char hex[3];
    int c;

    hex[2] = '\0';
    while (*read != '\0'){
        if (*read == '%') {
            hex[0] = *(++read);
            if (hex[0] == '\0') return 0;
            hex[1] = *(++read);
            if (hex[1] == '\0') return 0;
            c = htoi(hex);
            if (c == -1) return 0;
            *write = (char)c;
        }
        else *write = *read;
        read++;
        write++;
    }
    *write = '\0';
    return 1;
}

void http_parse_range(ngx_http_request_t* r, ngx_str_t* range_str, uint64_t* range_start, uint64_t* range_end, size_t content_length) {
    u_char *p, *last;
    off_t start, end;
    ngx_uint_t bad;
    enum {
        sw_start = 0,
        sw_first_byte_pos,
        sw_first_byte_pos_n,
        sw_last_byte_pos,
        sw_last_byte_pos_n,
        sw_done
    } state = 0;

    p = (u_char *) ngx_strnstr(range_str->data, "bytes=", range_str->len);

    if (p == NULL) {
        return;
    }

    p += sizeof("bytes=") - 1;
    last = range_str->data + range_str->len;

    /*
     * bytes= contain ranges compatible with RFC 2616, "14.35.1 Byte Ranges",
     * but no whitespaces permitted
     */

    bad = 0;
    start = 0;
    end = 0;

    while (p < last) {

        switch (state) {

        case sw_start:
        case sw_first_byte_pos:
            if (*p == '-') {
                p++;
                state = sw_last_byte_pos;
                break;
            }
            start = 0;
            state = sw_first_byte_pos_n;

            /* fall through */

        case sw_first_byte_pos_n:
            if (*p == '-') {
                p++;
                state = sw_last_byte_pos;
                break;
            }
            if (*p < '0' || *p > '9') {
                ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                               "bytes header filter: unexpected char '%c'"
                               " (expected first-byte-pos)", *p);
                bad = 1;
                break;
            }
            start = start * 10 + *p - '0';
            p++;
            break;

        case sw_last_byte_pos:
            if (*p == ',' || *p == '&' || *p == ';') {
                /* no last byte pos, assume end of file */
                end = content_length - 1;
                state = sw_done;
                break;
            }
            end = 0;
            state = sw_last_byte_pos_n;

            /* fall though */

        case sw_last_byte_pos_n:
            if (*p == ',' || *p == '&' || *p == ';') {
                state = sw_done;
                break;
            }
            if (*p < '0' || *p > '9') {
                ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                               "bytes header filter: unexpected char '%c'"
                               " (expected last-byte-pos)", *p);
                bad = 1;
                break;
            }
            end = end * 10 + *p - '0';
            p++;
            break;

        case sw_done:
            *range_start = start;
            *range_end = end;

            break;
        }

        if (bad) {
            ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                           "bytes header filter: invalid range specification");
            return;
        }
    }

    switch (state) {

    case sw_last_byte_pos:
        end = content_length - 1;

    case sw_last_byte_pos_n:
        if (start > end) {
            ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                           "bytes header filter: invalid range specification");
            return;
        }

        *range_start = start;
        *range_end = end;
        break;

    default:
        ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                       "bytes header filter: invalid range specification");
        return;

    }
}


ngx_uint_t ngx_http_test_if_modified(ngx_http_request_t *r)
{
    time_t                     ims;
    ngx_http_core_loc_conf_t  *clcf;

    clcf = ngx_http_get_module_loc_conf(r, ngx_http_core_module);

    if (clcf->if_modified_since == NGX_HTTP_IMS_OFF) {
        return 1;
    }

    ims = ngx_http_parse_time(r->headers_in.if_modified_since->value.data,
                              r->headers_in.if_modified_since->value.len);

    ngx_log_debug2(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                   "http ims:%d lm:%d", ims, r->headers_out.last_modified_time);

    if (ims == r->headers_out.last_modified_time) {
        return 0;
    }

    if (clcf->if_modified_since == NGX_HTTP_IMS_EXACT
        || ims < r->headers_out.last_modified_time)
    {
        return 1;
    }

    return 0;
}

ngx_uint_t nginx_http_get_rados_key(ngx_http_request_t *request, char **value)
{

    ngx_str_t location_name;
    ngx_str_t full_uri;
    ngx_http_core_loc_conf_t *core_conf;
    core_conf = ngx_http_get_module_loc_conf(request, ngx_http_core_module);

    location_name = core_conf->name;
    full_uri = request->uri;

    if(full_uri.len - location_name.len == 0) {
        return NGX_HTTP_NOT_FOUND;
     }

    if (full_uri.len < location_name.len) {
        ngx_log_error(NGX_LOG_ERR, request->connection->log, 0,
                      "Invalid location name or uri.");
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    *value = ngx_pcalloc(request->pool, sizeof(char) * (full_uri.len - location_name.len + 1));
    if (*value == NULL) {
        ngx_log_error(NGX_LOG_ERR, request->connection->log, 0,
                      "Failed to allocate memory for value buffer.");
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    ngx_memcpy(*value, full_uri.data + location_name.len, full_uri.len - location_name.len);
    value[full_uri.len - location_name.len] = '\0';


    if (!url_decode(*value)) {
        ngx_log_error(NGX_LOG_ERR, request->connection->log, 0,
            "Malformed request.");
            free(*value);
        return NGX_HTTP_BAD_REQUEST;
    }

    return NGX_OK;
}

//void mangle_filename_by_request_arg(ngx_http_request_t *request, char *my_variable_name) {
//    ngx_http_variable_value_t  *vv;
//    ngx_str_t                   variable_name;
//    ngx_uint_t                  hash_key;
//    variable_name.data = (u_char *)my_variable_name;
//    variable_name.len = sizeof(my_variable_name) -1;
//    hash_key = ngx_hash_key((u_char *)my_variable_name, sizeof(my_variable_name) -1);
//    vv = ngx_http_get_variable(request, &variable_name, hash_key);
//
//    if(vv != NULL && !vv->not_found && vv->len > 0){
//        u_char *arg = ngx_pcalloc(request->pool, sizeof(u_char) * (vv->len + 1));
//        ngx_cpystrn(arg, vv->data, vv->len);
//
//        *(arg+vv->len + 1) = '\0';
//
//        dd("Args Recieved f: %s", arg);
//    }
//}