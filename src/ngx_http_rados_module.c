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

#ifndef DDEBUG
#define DDEBUG 1
#endif
#include "ddebug.h"

#define BUF_LEN 1048576;

static char* ngx_http_rados(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);

static void* ngx_http_rados_create_loc_conf(ngx_conf_t *cf);
static char* ngx_http_rados_merge_loc_conf(ngx_conf_t *cf,
    void *parent, void *child);
static void* ngx_http_rados_create_main_conf(ngx_conf_t* directive);
static ngx_int_t ngx_http_rados_init_worker(ngx_cycle_t* cycle);
static void on_aio_complete_body(rados_completion_t cb, void *arg);

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
    size_t rados_throttle;
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

    { ngx_string("rados_throttle"),
      NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_size_slot,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_rados_loc_conf_t, rados_throttle),
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


static void send_status_and_finish_connection(ngx_http_request_t *request, ngx_uint_t status, ngx_str_t *message, int ngx_code) {
    ngx_chain_t* out = (ngx_chain_t*)ngx_pcalloc(request->connection->pool, sizeof(ngx_chain_t));

    out->buf = ngx_create_temp_buf(request->connection->pool, 1);

    if(message != NULL) {
        request->headers_out.content_type.len = sizeof("text/plain") - 1;
        request->headers_out.content_type.data = (u_char *) "text/plain";

        out->buf->pos = message->data;
        out->buf->last = message->data + message->len;
        request->headers_out.content_length_n = message->len;
    }else{
        out->buf->pos = (u_char*)"";
        out->buf->last = out->buf->pos+1;
    }
    out->next = NULL;
    out->buf->last_buf = 1;
    request->headers_out.status = status;

    ngx_http_send_header(request);
    ngx_http_output_filter(request, out);
    ngx_http_finalize_request(request, ngx_code);
}

typedef struct  {
    ngx_http_request_t *request;
    size_t size;
    time_t mtime;
    char *key;
    ngx_http_rados_connection_t *rados_conn;

    char *iobuffer;
    size_t buf_len;
    size_t offset;

    size_t total_read;

    uint64_t range_start;
    uint64_t range_end;
    ngx_event_t wev;
    ngx_msec_t throttle;
    //ngx_buf_t *buffer;
    ngx_chain_t chain_link;
} ngx_http_rados_ctx_t;


static
void rados_reading_callback(ngx_event_t *wev)
{
    dd("IN rados_reading_callback");
    int err;
    ngx_http_rados_ctx_t *state = (ngx_http_rados_ctx_t *) wev->data;

    ngx_log_error(NGX_LOG_DEBUG, wev->log, 0, "In Reading callback");

    if(state->request->connection->write->error) {
        dd("Connection has been reset by peer");
        ngx_http_finalize_request(state->request, NGX_ERROR);
        return;
    }

    rados_completion_t comp;
    err = rados_aio_create_completion(state, on_aio_complete_body, NULL, &comp);
    if (err < 0) {
            ngx_log_error(NGX_LOG_DEBUG, state->request->connection->log, 0,
                                          "Could not create aio completition");
            ngx_http_finalize_request(state->request, NGX_ERROR);
            return;
    }


    dd("Spawning async rados_aio_read offset: %zd", state->total_read);
    err = rados_aio_read(state->rados_conn->io, state->key, comp, state->iobuffer, state->buf_len, state->offset);
    if (err < 0) {
                ngx_log_error(NGX_LOG_DEBUG, state->request->connection->log, 0,
                                          "rados_aio_read Failed");
            ngx_http_finalize_request(state->request, NGX_ERROR);
            return;
    }


}


static void on_aio_complete_body(rados_completion_t cb, void *arg){
    ngx_int_t err;
    rados_aio_release(cb);
    ngx_http_rados_ctx_t *state = (ngx_http_rados_ctx_t *) arg;

    ngx_buf_t *buffer;
    ngx_chain_t out;

    if(state->request->connection->write->error) {
        dd("Connection has been reset by peer");
        ngx_http_finalize_request(state->request, NGX_ERROR);
        return;
    }

    int read = rados_aio_get_return_value(cb);
    if(read <= 0) {
        ngx_log_error(NGX_LOG_DEBUG, state->request->connection->log, 0,
                                      "Rados AIO Read failed");
        ngx_http_finalize_request(state->request, NGX_ERROR);
        return;
    }

    //dd("on_aio_complete_body returned: %u bytes, current offset: %zd, total_read: %zd, total_size: %zd", read, state->offset, state->total_read, state->size);

    /* Allocate a new buffer for sending out the reply. */
    if(state->chain_link.buf != NULL ){
        out = state->chain_link;
    }
    if(state->chain_link.buf != NULL) {
        buffer = state->chain_link.buf;
    }else {
        state->chain_link.buf = (ngx_buf_t *) ngx_pcalloc(state->request->pool, sizeof(ngx_buf_t));
        buffer = state->chain_link.buf;
    }
    if(buffer == NULL) {
        ngx_log_error(NGX_LOG_DEBUG, state->request->connection->log, 0,
                                      "Could not allocate read buffer");
        ngx_http_finalize_request(state->request, NGX_ERROR);
        return;
    }

    state->offset += read;
    state->total_read += read;

    buffer->pos = (u_char*)state->iobuffer;
    buffer->last = (u_char*)state->iobuffer + read;
    buffer->memory = 1;
    buffer->last_buf = (state->size <= state->total_read);

    out.buf = buffer;
    out.next = NULL;

    if(!buffer->last_buf) {
        dd("Allocating next buffer");
        ngx_chain_t* out2 = (ngx_chain_t *) ngx_pcalloc(state->request->pool, sizeof(ngx_chain_t));;
        ngx_buf_t* buf2 = (ngx_buf_t *) ngx_pcalloc(state->request->pool, sizeof(ngx_buf_t));
        out2->buf = buf2;
        out.next = out2;
        state->chain_link = *out2;
        dd("After");
    }

    dd("Writing to htt pout %zd of %zd", state->total_read, state->size);
    ngx_http_output_filter(state->request, &out);

    if(buffer->last_buf) {
        dd("Transfer from rados completed");
        ngx_http_finalize_request(state->request, NGX_OK);
        return;
    }else{
        dd("Transfering from rados: %zd bytes from %zd offset", state->buf_len, state->offset);
        //call another async, doing async recursion
        rados_completion_t comp;
        err = rados_aio_create_completion(state, on_aio_complete_body, NULL, &comp);
        if (err < 0) {
                ngx_log_error(NGX_LOG_DEBUG, state->request->connection->log, 0,
                                              "Could not create aio completition");
                ngx_http_finalize_request(state->request, NGX_ERROR);
                return;
        }


        //    /**
        //    TIMEER TEST
        //    */

        if(state->throttle > 0) {
            dd("Adding Reading timer, throttling to sleep per buffer: %zd", state->throttle);
            ngx_add_timer(&state->wev, (ngx_msec_t)state->throttle);
        } else {
            dd("Spawning async rados_aio_read offset: %zd", state->total_read);
            err = rados_aio_read(state->rados_conn->io, state->key, comp, state->iobuffer, state->buf_len, state->offset);
            if (err < 0) {
                        ngx_log_error(NGX_LOG_DEBUG, state->request->connection->log, 0,
                                                  "rados_aio_read Failed");
                    ngx_http_finalize_request(state->request, NGX_ERROR);
                    return;
            }
        }

    }
}

static void on_aio_complete_header(rados_completion_t cb, void *arg){
    ngx_int_t err;
    int success;
    ngx_http_rados_ctx_t *state;

    state = (ngx_http_rados_ctx_t *) arg;
    success = rados_aio_get_return_value(cb);
    rados_aio_release(cb);

    if(success < 0 || !state->size || !state->mtime) {
        ngx_log_error(NGX_LOG_ERR, state->request->connection->log, 0,
                                  "File not found in rados: %s", state->key);
        ngx_str_t error_message = ngx_string("File not found\n");
        send_status_and_finish_connection(state->request, NGX_HTTP_NOT_FOUND, &error_message, NGX_OK);
        return;
    }

    dd("Recieved callback for %s, size: %zd, mtime: %zd\n", state->key, state->size, state->mtime);
    state->request->headers_out.status = NGX_HTTP_OK;
    state->request->headers_out.content_length_n = state->size;
    state->request->headers_out.last_modified_time = state->mtime;

    if(state->request->headers_in.if_modified_since && !ngx_http_test_if_modified(state->request)) {
        state->request->headers_out.status = NGX_HTTP_NOT_MODIFIED;
        ngx_http_send_header(state->request); /* Send the headers */
        ngx_http_finalize_request(state->request, NGX_OK);
        return;
    }

    if(state->request->method == NGX_HTTP_HEAD) {
        send_status_and_finish_connection(state->request, NGX_HTTP_OK, NULL, NGX_OK);
        return;
    }
    if(state->request->method != NGX_HTTP_GET) {
        ngx_str_t error_message = ngx_string("Method not implemented\n");
        send_status_and_finish_connection(state->request, NGX_HTTP_NOT_IMPLEMENTED, &error_message, NGX_OK);
        return;
    }

    //XXX Range request
    state->range_start = 0;
    state->range_end = 0;
    if (state->request->headers_in.range) {
        http_parse_range(state->request, &state->request->headers_in.range->value, &state->range_start, &state->range_end, state->size);
        dd("Requested range request: %zd, %zd\n", state->range_start, state->range_end);
    }
    if (state->range_start == 0 && state->range_end == 0) {
        state->request->headers_out.status = NGX_HTTP_OK;
        state->request->headers_out.content_length_n = state->size;
    } else if(state->range_start >= state->size || state->range_end > state->size || state->range_end < state->range_start){
         ngx_log_error(NGX_LOG_ERR, state->request->connection->log, 0,
                       "Invalid range requested start: %i end: %i", state->range_start, state->range_end);
        ngx_str_t error_message = ngx_string("Invalid range in range request\n");
        send_status_and_finish_connection(state->request, NGX_HTTP_RANGE_NOT_SATISFIABLE, &error_message, NGX_OK);
     } else {
        dd("Doing range request, range: %ld-%ld", (long)state->range_start, (long)state->range_end);

        state->request->headers_out.status = NGX_HTTP_PARTIAL_CONTENT;
        state->request->headers_out.content_length_n = state->size;

        ngx_table_elt_t   *content_range = ngx_list_push(&state->request->headers_out.headers);
        if (content_range == NULL) {
                 ngx_log_error(NGX_LOG_ERR, state->request->connection->log, 0,
                               "Failure to do ngx_list_push");
            ngx_str_t error_message = ngx_string("Failure to do ngx_list_push\n");
            send_status_and_finish_connection(state->request, NGX_HTTP_BAD_REQUEST, &error_message, NGX_ERROR);
        }
        state->request->headers_out.content_range = content_range;
        content_range->hash = 1;
        ngx_str_set(&content_range->key, "Content-Range");
        content_range->value.data = ngx_pnalloc(state->request->pool,sizeof("bytes -/") - 1 + 3 * NGX_OFF_T_LEN);
        if (content_range->value.data == NULL) {
                         ngx_log_error(NGX_LOG_ERR, state->request->connection->log, 0,
                                       "Failure to allocate memory");
            ngx_str_t error_message = ngx_string("Failure to allocate memory\n");
            send_status_and_finish_connection(state->request, NGX_HTTP_INTERNAL_SERVER_ERROR, &error_message, NGX_ERROR);
        }
        content_range->value.len = ngx_sprintf(content_range->value.data,
                                             "bytes %O-%O/%O",
                                             state->range_start, state->range_end,
                                             state->request->headers_out.content_length_n) - content_range->value.data;

        state->request->headers_out.content_length_n = state->range_end - state->range_start + 1;
    }

    rados_completion_t comp;
    err = rados_aio_create_completion(state, on_aio_complete_body, NULL, &comp);
    if (err < 0) {
        ngx_log_error(NGX_LOG_DEBUG, state->request->connection->log, 0,
                                  "Could not allocate rados_aio_create_completion");
        ngx_str_t error_message = ngx_string("Could not allocate rados_aio_create_completion\n");
        send_status_and_finish_connection(state->request, NGX_HTTP_INTERNAL_SERVER_ERROR, &error_message, NGX_ERROR);
    }

    state->offset = state->range_start;
    state->total_read = 0;
    if(state->range_end == 0 ) state->range_end = state->size;
    state->buf_len = BUF_LEN;

    if(state->size < state->buf_len) {
        state->buf_len = state->size;
    }

    state->iobuffer = ngx_pnalloc(state->request->pool, state->buf_len+1);
    if(state->iobuffer == NULL) {
        ngx_log_error(NGX_LOG_ALERT, state->request->connection->log, 0,
                                  "Could not allocate result buffer");
        ngx_str_t error_message = ngx_string("Could not allocate result buffer\n");
        send_status_and_finish_connection(state->request, NGX_HTTP_INTERNAL_SERVER_ERROR, &error_message, NGX_ERROR);
    }
    dd("Spawning async rados_aio_read");
    err = rados_aio_read(state->rados_conn->io, state->key, comp, state->iobuffer, state->buf_len, state->offset);
    if (err < 0) {
        ngx_log_error(NGX_LOG_DEBUG, state->request->connection->log, 0,
                                  "rados_aio_read Failed");
        ngx_str_t error_message = ngx_string("rados_aio_read Failed\n");
        send_status_and_finish_connection(state->request, NGX_HTTP_INTERNAL_SERVER_ERROR, &error_message, NGX_ERROR);
        return;
    }
    ngx_http_send_header(state->request); /* Send the headers */
}

static void
ngx_http_rados_cleanup(void *data)
{
    ngx_http_rados_ctx_t *state = (ngx_http_rados_ctx_t *) data;

    dd("echo sleep cleanup");

    if(state->wev.timer_set) {
        dd("Deleting timer");
        ngx_del_timer(&state->wev);
        return;
    }
    dd("Timer not set");
}

ngx_http_rados_ctx_t *
ngx_http_rados_create_ctx(ngx_http_request_t *r)
{
    ngx_http_rados_ctx_t         *ctx;

    ctx = ngx_pcalloc(r->pool, sizeof(ngx_http_rados_ctx_t));
    if (ctx == NULL) {
        return NULL;
    }

    ctx->wev.handler   = rados_reading_callback;
    ctx->wev.data      = ctx;
    ctx->wev.log       = r->connection->log;

    ngx_http_cleanup_t *cln = ngx_http_cleanup_add(r, 0);
    cln->handler = ngx_http_rados_cleanup;
    cln->data = ctx;

    return ctx;
}

inline static ngx_msec_t compute_throttle(size_t limit) {
    if(!limit) return (ngx_msec_t)0;

    float buf = BUF_LEN;
    float bufs_per_second = limit / buf;
    float sleep = 1000/bufs_per_second;
    return (ngx_msec_t)sleep;
}

static ngx_int_t
ngx_http_rados_handler(ngx_http_request_t *request)
{
    ngx_http_rados_loc_conf_t* rados_conf;
    char* value = NULL;
    ngx_http_rados_connection_t *rados_conn;

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

    rados_completion_t comp;
    ngx_http_rados_ctx_t *state = ngx_http_rados_create_ctx(request);
    if(state == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    state->request = request;
    state->key = value;
    state->rados_conn = rados_conn;
    state->throttle = compute_throttle(rados_conf->rados_throttle);
    err = rados_aio_create_completion(state, on_aio_complete_header, NULL, &comp);
    if (err < 0) {
                ngx_log_error(NGX_LOG_DEBUG, request->connection->log, 0,
                                          "Could not create aio completition");
            return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    rados_aio_stat(rados_conn->io, value, comp, &state->size, &state->mtime);
    request->main->count++;

    return NGX_DONE;
}


#define TICK_TIME 10

#if 1
void ngx_dummy_timer_handler(ngx_event_t *ev)
{
  ngx_add_timer(ev, TICK_TIME);
}
#endif

static ngx_int_t ngx_http_rados_init_worker(ngx_cycle_t* cycle) {

    ngx_http_rados_main_conf_t* rados_main_conf = ngx_http_cycle_get_module_main_conf(cycle, ngx_http_rados_module);
    ngx_http_rados_loc_conf_t** rados_loc_confs;
    ngx_uint_t i;

    signal(SIGPIPE, SIG_IGN);

#if 1
   ngx_event_t *ngx_dummy_timer = ngx_pcalloc(cycle->pool, sizeof(ngx_event_t));
   ngx_dummy_timer->log     = ngx_cycle->log;
   ngx_dummy_timer->data    = NULL;
   ngx_dummy_timer->handler = ngx_dummy_timer_handler;
   ngx_add_timer(ngx_dummy_timer, (ngx_msec_t)TICK_TIME);
#endif

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
    conf->rados_throttle = NGX_CONF_UNSET;
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
    ngx_conf_merge_size_value(conf->rados_throttle, prev->rados_throttle, (size_t)0);


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