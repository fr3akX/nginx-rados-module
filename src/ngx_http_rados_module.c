#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include <rados/librados.h>
#include "ngx_http_rados_util.h"

static char* ngx_http_rados(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);

static void* ngx_http_rados_create_loc_conf(ngx_conf_t *cf);
static char* ngx_http_rados_merge_loc_conf(ngx_conf_t *cf,
    void *parent, void *child);
static void* ngx_http_rados_create_main_conf(ngx_conf_t* directive);
static ngx_int_t ngx_http_rados_init_worker(ngx_cycle_t* cycle);

typedef struct {
    ngx_array_t loc_confs; /* ngx_http_gridfs_loc_conf_t */
} ngx_http_rados_main_conf_t;

typedef struct {
    rados_t cluster;
    rados_ioctx_t io;
    ngx_str_t pool;
} ngx_http_rados_connection_t;

static ngx_http_rados_connection_t* ngx_http_get_rados_connection( ngx_str_t name );

typedef struct {
    ngx_str_t pool;
    ngx_str_t conf_path;
    ngx_flag_t enable;
    ngx_http_upstream_conf_t upstream;
} ngx_http_rados_loc_conf_t;

static ngx_int_t ngx_http_rados_init(ngx_http_rados_loc_conf_t *cf);

ngx_array_t ngx_http_rados_connections;

static ngx_int_t ngx_http_rados_add_connection(ngx_cycle_t* cycle, ngx_http_rados_loc_conf_t* rados_loc_conf);

static ngx_command_t  ngx_http_rados_commands[] = {
    { ngx_string("rados"),
      NGX_HTTP_LOC_CONF|NGX_CONF_NOARGS,
      ngx_http_rados,
      NGX_HTTP_LOC_CONF_OFFSET,
      0,
      NULL },

    { ngx_string("rados_pool"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_str_slot,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_rados_loc_conf_t, pool),
      NULL },

    { ngx_string("rados_conf"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_str_slot,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_rados_loc_conf_t, conf_path),
      NULL },
      ngx_null_command
};

static ngx_http_module_t  ngx_http_rados_module_ctx = {
    NULL,                          /* preconfiguration */
    NULL,           /* postconfiguration */

    ngx_http_rados_create_main_conf,                          /* create main configuration */
    NULL,                          /* init main configuration */

    NULL,                          /* create server configuration */
    NULL,                          /* merge server configuration */

    ngx_http_rados_create_loc_conf,  /* create location configuration */
    ngx_http_rados_merge_loc_conf /* merge location configuration */
};

ngx_module_t  ngx_http_rados_module = {
    NGX_MODULE_V1,
    &ngx_http_rados_module_ctx, /* module context */
    ngx_http_rados_commands,   /* module directives */
    NGX_HTTP_MODULE,               /* module type */
    NULL,                          /* init master */
    NULL,                          /* init module */
    ngx_http_rados_init_worker,   /* init process */
    NULL,                          /* init thread */
    NULL,                          /* exit thread */
    NULL,                          /* exit process */
    NULL,                          /* exit master */
    NGX_MODULE_V1_PADDING
};

//not possible to implement with rados_io, as nginx frees allocated buffer after handler return
//typedef struct  {
//    ngx_http_request_t *request;
//    size_t size;
//    time_t mtime;
//    char *key;
//} aio_state;
//#define HELLO_WORLD "hello world"
//
//static u_char ngx_hello_world[] = HELLO_WORLD;
//
//static void on_aio_complete(rados_completion_t cb, void *arg){
//    rados_aio_release(cb);
//    aio_state *state = (aio_state *) arg;
//    if(!state->size || !state->mtime) {
//        printf("NOT FOUND: %s\n", state->key);
////        state->request->headers_out.status = NGX_HTTP_NOT_FOUND;
//    }else{
//        printf("Recieved callback for %s, size: %zd, mtime: %zd\n", state->key, state->size, state->mtime);
// //       state->request->headers_out.status = NGX_HTTP_OK;
// //       state->request->headers_out.content_length_n = state->size;
//
//    }
//    if(state->request->connection != NULL) {
//    printf("Connection is heer, destroyed: %u\n", state->request->connection->destroyed);
//    }
//
// //   ngx_http_set_content_type(state->request);
////    ngx_http_send_header(state->request);
//
//
//    ngx_buf_t *b;
//    ngx_chain_t out;
//
//    /* Set the Content-Type header. */
//    state->request->headers_out.content_type.len = sizeof("text/plain") - 1;
//    state->request->headers_out.content_type.data = (u_char *) "text/plain";
//
//    /* Allocate a new buffer for sending out the reply. */
//    b = ngx_pcalloc(state->request->pool, sizeof(ngx_buf_t));
//
//    /* Insertion in the buffer chain. */
//    out.buf = b;
//    out.next = NULL; /* just one buffer */
//
//    b->pos = ngx_hello_world; /* first position in memory of the data */
//    b->last = ngx_hello_world + sizeof(ngx_hello_world); /* last position in memory of the data */
//    b->memory = 1; /* content is in read-only memory */
//    b->last_buf = 1; /* there will be no more buffers in the request */
//
//    /* Sending the headers for the reply. */
//    state->request->headers_out.status = NGX_HTTP_OK; /* 200 status code */
//    /* Get the content length of the body. */
//    state->request->headers_out.content_length_n = sizeof(ngx_hello_world);
//    ngx_http_send_header(state->request); /* Send the headers */
//
//    /* Send the body, and return the status code of the output filter chain. */
//    ngx_http_output_filter(state->request, &out);
//}

static ngx_int_t
ngx_http_rados_handler(ngx_http_request_t *request)
{
    ngx_http_rados_loc_conf_t* rados_conf;
    ngx_buf_t* buffer;
    ngx_chain_t out;
    char* value = NULL;
    ngx_http_rados_connection_t *rados_conn;
    char* contenttype = NULL;
    uint64_t range_start = 0;
    uint64_t range_end   = 0;

    size_t ctypebuf_len = 1024;
    char ctypebuf[ctypebuf_len];

    size_t BUF_LEN = 1048576;
    char iobuffer[BUF_LEN+1];

    size_t size;
    time_t mtime;

    ngx_int_t rc = NGX_OK;
    ngx_int_t err;

	rc = ngx_http_discard_request_body(request);
    if (rc != NGX_OK)
		return rc;

    rados_conf = ngx_http_get_module_loc_conf(request, ngx_http_rados_module);

    rados_conn = ngx_http_get_rados_connection( rados_conf->pool );
    if(rados_conn == NULL) {
        ngx_log_error(NGX_LOG_DEBUG, request->connection->log, 0,
                          "Rados Connection not found: \"%s\"", &rados_conf->pool);
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    rc = nginx_http_get_rados_key(request, &value);
    if(rc != NGX_OK)
        return rc;

    ngx_log_error(NGX_LOG_DEBUG, request->connection->log, 0,
                              "Request key: \"%s\"", value);
//    if(true) {
//        rados_completion_t comp;
//        aio_state *state = ngx_pnalloc(request->pool,sizeof(aio_state));
//        state->request = request;
//        state->key = value;
//        err = rados_aio_create_completion(state, on_aio_complete, NULL, &comp);
//        if (err < 0) {
//                    ngx_log_error(NGX_LOG_DEBUG, request->connection->log, 0,
//                                              "Could not create aio completition");
//                return NGX_HTTP_INTERNAL_SERVER_ERROR;
//        }
//
//        rados_aio_stat(rados_conn->io, value, comp, &state->size, &state->mtime);
//        printf("Connection is heer, destroyed: %u\n", state->request->connection->destroyed);
//        return NGX_HTTP_AIO_ON;
//    }

    err = rados_stat(rados_conn->io, value, &size, &mtime);
    if (err < 0) {
        ngx_log_error(NGX_LOG_ERR, request->connection->log, 0,
                      "File not found in rados: %s", value);
        return NGX_HTTP_NOT_FOUND;
    }

    request->headers_out.last_modified_time = mtime;

    if(request->headers_in.if_modified_since && !ngx_http_test_if_modified(request)) {
        return NGX_HTTP_NOT_MODIFIED;
    }

    err = rados_getxattr(rados_conn->io, value, "content_type", (char *)&ctypebuf, ctypebuf_len);
    if(err > 0) {
        contenttype = ngx_pnalloc(request->pool,err + 1);
        ngx_memcpy(contenttype, ctypebuf, err);
        contenttype[err+1] = '\0';
    }

    if (request->headers_in.range) {
        http_parse_range(request, &request->headers_in.range->value, &range_start, &range_end, size);
    }
    if (range_start == 0 && range_end == 0) {
        request->headers_out.status = NGX_HTTP_OK;
        request->headers_out.content_length_n = size;
    } else if(range_start >= size || range_end > size || range_end < range_start){
        ngx_log_error(NGX_LOG_ERR, request->connection->log, 0,
                      "Invalid range requested start: %i end: %i", range_start, range_end);
        return NGX_HTTP_RANGE_NOT_SATISFIABLE;
    }else {
        request->headers_out.status = NGX_HTTP_PARTIAL_CONTENT;
        request->headers_out.content_length_n = size;

        ngx_table_elt_t   *content_range = ngx_list_push(&request->headers_out.headers);
        if (content_range == NULL) {
            return NGX_ERROR;
        }
        request->headers_out.content_range = content_range;
        content_range->hash = 1;
        ngx_str_set(&content_range->key, "Content-Range");
        content_range->value.data = ngx_pnalloc(request->pool,sizeof("bytes -/") - 1 + 3 * NGX_OFF_T_LEN);
        if (content_range->value.data == NULL) {
            return NGX_ERROR;
        }
        content_range->value.len = ngx_sprintf(content_range->value.data,
                                               "bytes %O-%O/%O",
                                               range_start, range_end,
                                               request->headers_out.content_length_n) - content_range->value.data;

        request->headers_out.content_length_n = range_end - range_start + 1;
    }

    if (contenttype != NULL) {
        request->headers_out.content_type.len = strlen(contenttype);
        request->headers_out.content_type.data = (u_char*)contenttype;
    }
    else ngx_http_set_content_type(request);

    ngx_http_send_header(request);




    size_t offset = range_start, total_read = 0;
    int read = 0;

    if(range_end == 0 ) range_end = size;

    if(request->method == NGX_HTTP_GET) {
        do {
            read = rados_read(rados_conn->io, value, iobuffer, BUF_LEN, offset);
            if(read > 0) {
                buffer = ngx_pcalloc(request->pool, sizeof(ngx_buf_t));
                if (buffer == NULL) {
                    ngx_log_error(NGX_LOG_ERR, request->connection->log, 0,
                                  "Failed to allocate response buffer");
                    return NGX_HTTP_INTERNAL_SERVER_ERROR;
                }

                offset += read;
                total_read += read;

                buffer->pos = (u_char*)iobuffer;
                buffer->last = (u_char*)iobuffer + read;
                buffer->memory = 1;
                buffer->last_buf = (size == total_read);
                out.buf = buffer;
                out.next = NULL;

                rc = ngx_http_output_filter(request, &out);
                ngx_pfree(request->pool, buffer);
            }
        }while(read > 0 && (total_read + range_start) < range_end);

        if(read < 0) {
            ngx_log_error(NGX_LOG_ERR, request->connection->log, 0,
                          "Failure while reading from rados");
        }
    }

    return rc;
}

static ngx_int_t ngx_http_rados_init_worker(ngx_cycle_t* cycle) {

    ngx_http_rados_main_conf_t* rados_main_conf = ngx_http_cycle_get_module_main_conf(cycle, ngx_http_rados_module);
    ngx_http_rados_loc_conf_t** rados_loc_confs;
    ngx_uint_t i;

    signal(SIGPIPE, SIG_IGN);

    rados_loc_confs = rados_main_conf->loc_confs.elts;
    ngx_array_init(&ngx_http_rados_connections, cycle->pool, 4, sizeof(ngx_http_rados_connection_t));

    for (i = 0; i < rados_main_conf->loc_confs.nelts; i++) {
        if (ngx_http_rados_add_connection(cycle, rados_loc_confs[i]) == NGX_ERROR) {
            return NGX_OK;
        }
    }

    return NGX_OK;
}

static char *
ngx_http_rados(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_core_loc_conf_t  *clcf;
    ngx_http_rados_loc_conf_t *cglcf = conf;

    clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
    clcf->handler = ngx_http_rados_handler;

    cglcf->enable = 1;

    return NGX_CONF_OK;
}

static ngx_int_t
ngx_http_rados_init(ngx_http_rados_loc_conf_t *cglcf)
{
  return 1;
}

static void *ngx_http_rados_create_main_conf(ngx_conf_t *cf) {
    ngx_http_rados_main_conf_t  *rados_main_conf;

    rados_main_conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_rados_main_conf_t));
    if (rados_main_conf == NULL) {
        return NULL;
    }

    if (ngx_array_init(&rados_main_conf->loc_confs, cf->pool, 4,
                       sizeof(ngx_http_rados_loc_conf_t *))
        != NGX_OK) {
        return NULL;
    }

    return rados_main_conf;
}

static void *
ngx_http_rados_create_loc_conf(ngx_conf_t *cf)
{
    ngx_http_rados_loc_conf_t  *conf;

    conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_rados_loc_conf_t));
    if (conf == NULL) {
        return NGX_CONF_ERROR;
    }
    conf->conf_path.data = NULL;
    conf->conf_path.len = 0;
    conf->pool.data = NULL;
    conf->pool.len = 0;
    conf->enable = NGX_CONF_UNSET;
    return conf;
}

static char *
ngx_http_rados_merge_loc_conf(ngx_conf_t *cf, void *parent, void *child)
{
    ngx_http_rados_loc_conf_t *prev = parent;
    ngx_http_rados_loc_conf_t *conf = child;

    ngx_http_rados_main_conf_t *rados_main_conf = ngx_http_conf_get_module_main_conf(cf, ngx_http_rados_module);
    ngx_http_rados_loc_conf_t **rados_loc_conf;

    ngx_conf_merge_str_value(conf->pool, prev->pool, NULL);
    ngx_conf_merge_str_value(conf->conf_path, prev->conf_path, NULL);
    ngx_conf_merge_value(conf->enable, prev->enable, 0);

    if (conf->pool.len == 0 || conf->conf_path.len == 0) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "rados_pool and rados_conf should be specified");
        return NGX_CONF_ERROR;
    } else {
        rados_loc_conf = ngx_array_push(&rados_main_conf->loc_confs);
        *rados_loc_conf = child;
    }


    if(conf->enable) {
        ngx_http_rados_init(conf);
    }

    return NGX_CONF_OK;
}

ngx_http_rados_connection_t* ngx_http_get_rados_connection( ngx_str_t name ) {
    ngx_http_rados_connection_t *rados_conns;
    ngx_uint_t i;

    rados_conns = ngx_http_rados_connections.elts;

    for ( i = 0; i < ngx_http_rados_connections.nelts; i++ ) {
        if ( name.len == rados_conns[i].pool.len
             && ngx_strncmp(name.data, rados_conns[i].pool.data, name.len) == 0 ) {
            return &rados_conns[i];
        }
    }

    return NULL;
}

static ngx_int_t ngx_http_rados_add_connection(ngx_cycle_t* cycle, ngx_http_rados_loc_conf_t* rados_loc_conf) {
    ngx_http_rados_connection_t* rados_conn;
    //ngx_uint_t status;
    ngx_int_t err;

    u_char conf[255];
    u_char pool[255];

    ngx_cpystrn( conf, rados_loc_conf->conf_path.data, rados_loc_conf->conf_path.len + 1 );
    ngx_cpystrn( pool, rados_loc_conf->pool.data, rados_loc_conf->pool.len + 1 );

    rados_conn = ngx_http_get_rados_connection(rados_loc_conf->pool);
    if(rados_conn != NULL) {
        return NGX_OK;
    }

    rados_conn = ngx_array_push(&ngx_http_rados_connections);
    if (rados_conn == NULL) {
        ngx_log_error(NGX_LOG_ERR, cycle->log, 0, "Could not allocate rados connection");
        return NGX_ERROR;
    }

    rados_conn->pool = rados_loc_conf->pool;

    ngx_log_error(NGX_LOG_DEBUG, cycle->log, 0, "Initing cluster");
    err = rados_create(&rados_conn->cluster, NULL);
    if (err < 0) {
        ngx_log_error(NGX_LOG_ERR, cycle->log, 0, "Could not init cluster handle");
        return NGX_ERROR;
    }

    ngx_log_error(NGX_LOG_DEBUG, cycle->log, 0, "Reading conf: %s", conf);
    err = rados_conf_read_file(rados_conn->cluster, (const char *)conf);
    if (err < 0) {
        ngx_log_error(NGX_LOG_ERR, cycle->log, 0, "Could not load cluster config: %s", conf);
        return NGX_ERROR;
    }

    ngx_log_error(NGX_LOG_DEBUG, cycle->log, 0, "Connecting cluster");
    err = rados_connect(rados_conn->cluster);
    if (err < 0) {
        ngx_log_error(NGX_LOG_ERR, cycle->log, 0, "Cannot connect to cluster");
        return NGX_ERROR;
    }

    ngx_log_error(NGX_LOG_DEBUG, cycle->log, 0, "Opening io: %s", pool);
    err = rados_ioctx_create(rados_conn->cluster, (const char *)pool, &rados_conn->io);
    if (err < 0) {
        ngx_log_error(NGX_LOG_ERR, cycle->log, 0, "Cannot open rados pool");
        rados_shutdown(rados_conn->cluster);
        return err;
    }

    return NGX_OK;
}