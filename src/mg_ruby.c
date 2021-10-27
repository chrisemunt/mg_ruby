/*
   ----------------------------------------------------------------------------
   | mg_ruby: Ruby Extension for M/Cache/IRIS                                 |
   | Author: Chris Munt cmunt@mgateway.com                                    |
   |                    chris.e.munt@gmail.com                                |
   | Copyright (c) 2016-2020 M/Gateway Developments Ltd,                      |
   | Surrey UK.                                                               |
   | All rights reserved.                                                     |
   |                                                                          |
   | http://www.mgateway.com                                                  |
   |                                                                          |
   | Licensed under the Apache License, Version 2.0 (the "License"); you may  |
   | not use this file except in compliance with the License.                 |
   | You may obtain a copy of the License at                                  |
   |                                                                          |
   | http://www.apache.org/licenses/LICENSE-2.0                               |
   |                                                                          |
   | Unless required by applicable law or agreed to in writing, software      |
   | distributed under the License is distributed on an "AS IS" BASIS,        |
   | WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. |
   | See the License for the specific language governing permissions and      |
   | limitations under the License.                                           |      
   ----------------------------------------------------------------------------
*/

/*

Change Log:

Version 2.0.39 17 October 2009:
   First release (as Freeware).

Version 2.1.40 10 December 2019; 20 January 2020:
   Update code base.
   First Open Source release.

Version 2.2.41 16 February 2021:
   Introduce support for M transaction processing: tstart, $tlevel, tcommit, trollback.
   Introduce support for the M increment function.
   Allow the DB server response timeout to be modified via the mg_ruby.m_set_timeout() function.
   - mg_ruby.m_set_timeout(<dbhandle>,<timeout>)

Version 2.2.42 14 March 2021:
   Introduce support for YottaDB Transaction Processing over API based connectivity.
   - This functionality was previously only available over network-based connectivity to YottaDB.

Version 2.3.43 20 April 2021:
   Introduce improved support for InterSystems Objects for the standard (PHP/Python/Ruby) connectivity protocol.

Version 2.3.44 27 October 2021:
   Ensure that data strings returned from YottaDB are correctly terminated.
   Verify that mg_ruby will build and work with Ruby v3.0.x.

*/


#define MG_VERSION               "2.3.44"

#define MG_MAX_KEY               256
#define MG_MAX_PAGE              256
#define MG_MAX_VARGS             32

#define MG_T_VAR                 0
#define MG_T_STRING              1
#define MG_T_INTEGER             2
#define MG_T_FLOAT               3
#define MG_T_LIST                4

#define MG_PRODUCT               "r"

#define MG_DBA_EMBEDDED          1
#include "mg_dbasys.h"
#include "mg_dba.h"

/* include standard header */

#include <ruby.h>


#define MG_ERROR(e) \
   rb_raise(rb_eRuntimeError, "%s", (char *) e); \


#define MG_WARN(e) \
   rb_warn((char *) e); \


#define MG_MEMCHECK(e, c) \
   if (p_page && p_page->p_srv->mem_error == 1) { \
      mg_db_disconnect(p_page->p_srv, chndle, c); \
      rb_raise(rb_eRuntimeError, "%s", (char *) e); \
      return mg_r_nil; \
   } \

#define MG_FTRACE(e) \

typedef struct tagMGPTYPE {
   short type;
   short byref;
} MGPTYPE;

static int le_mg_user;

typedef struct tagMGVARGS {
   int phndle;
   char *global;
   int global_len;
   VALUE rvars[MG_MAX_VARGS];
   MGSTR cvars[MG_MAX_VARGS];
} MGVARGS, *LPMGVARGS;

typedef struct tagMGUSER {
   char buffer[MG_BUFSIZE];
} MGUSER;

typedef struct tagMGPAGE {
   MGSRV       srv;
   MGSRV *     p_srv;
} MGPAGE;

typedef struct tagMGMCLASS {
   int         phndle;
   int         oref;
} MGMCLASS;


static MGPAGE gpage;
static MGPAGE *tp_page[MG_MAX_PAGE] = {NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL};

static long request_no = 0;

static char minit[256] = {'\0'};

VALUE mg_r_nil    = Qnil;
VALUE mg_ruby     = Qnil;
VALUE mg_mclass   = Qnil; /* v2.3.43 */


int            mg_type                    (VALUE item);
int            mg_get_array_size          (VALUE rb_array);
int            mg_get_integer             (VALUE item);
double         mg_get_float               (VALUE item);
char *         mg_get_string              (VALUE item, VALUE * item_tmp, int *size);
int            mg_get_vargs               (int argc, VALUE *argv, MGVARGS *pvargs, int context);
int            mg_get_keys                (VALUE keys, MGSTR * ckeys, VALUE * keys_tmp, char *krec);
int            mg_set_list_item           (VALUE list, int index, VALUE item);
int            mg_kill_list               (VALUE list);
int            mg_kill_list_item          (VALUE list, int index);

MGPAGE *       mg_ppage                   (int phndle);
int            mg_ppage_init              (MGPAGE * p_page);

/* v2.3.43 */
void           mclass_free                (void * data);
size_t         mclass_size                (const void* data);
VALUE          mclass_alloc               (VALUE self);
VALUE          mclass_m_initialize        (VALUE self, VALUE rb_oref, VALUE rb_phndle);
static VALUE   ex_m_mclass                ();
static VALUE   ex_mclass_method           (int argc, VALUE *argv, VALUE self);
static VALUE   ex_mclass_getproperty      (VALUE self, VALUE r_pname);
static VALUE   ex_mclass_setproperty      (VALUE self, VALUE r_pname, VALUE p_pvalue);
static VALUE   ex_mclass_close            (VALUE self);


static const rb_data_type_t mclass_type = {
	.wrap_struct_name = "mclass",
	.function = {
		.dmark = NULL,
		.dfree = mclass_free,
		.dsize = mclass_size,
	},
	.data = NULL,
	.flags = RUBY_TYPED_FREE_IMMEDIATELY,
};


/*
static VALUE t_init(VALUE self)
{
   VALUE arr;
   VALUE r_phndle;
   VALUE r_str;

   r_str = rb_str_new2("the cat sat on the mat");
   r_phndle = rb_int2inum((long) 7);

   arr = rb_ary_new();
   rb_iv_set(self, "@arr", arr);

   rb_iv_set(self, "@r_phndle", r_phndle);
   rb_iv_set(self, "@r_str", r_str);

   return self;
}


static VALUE t_add(VALUE self, VALUE anObject)
{
   VALUE arr;
   VALUE str;

   arr = rb_iv_get(self, "@arr");
   rb_ary_push(arr, anObject);

   str = rb_str_new2("test");
  return str;
}

static void t_free(void *p) {
   mg_free(p, 0);
}
*/


static VALUE ex_m_ext_version(VALUE self)
{
   char buffer[256];
/*
   char *str;
   int phndle, len;
   VALUE r_str;
   VALUE r_phndle;
   VALUE r;

   r_str = rb_iv_get(self, "@r_str");
   r_phndle = rb_iv_get(self, "@r_phndle");

   str = mg_get_string(r_str, &r, &len);
   phndle = mg_get_integer(r_phndle);
*/
   sprintf(buffer, "M/Gateway Developments Ltd. - mg_ruby: Ruby Gateway to M - Version %s", MG_VERSION);

   return rb_str_new2(buffer);
}


static VALUE ex_m_set_storage_mode(VALUE self, VALUE r_mode)
{
   int mode, phndle;
   MGPAGE *p_page;

   phndle = 0;
   p_page = mg_ppage(phndle);

   mode = mg_get_integer(r_mode);

   p_page->p_srv->storage_mode = mode;

   return rb_str_new2("");

}


static VALUE ex_m_set_host(VALUE self, VALUE r_netname, VALUE r_port, VALUE r_username, VALUE r_password)
{
   int len, phndle;
   char *netname, *port, *username, *password;
   MGPAGE *p_page;
   VALUE r[32];

   phndle = 0;
   p_page = mg_ppage(phndle);

   netname = mg_get_string(r_netname, &r[0], &len);
   port = mg_get_string(r_port, &r[1], &len);
   username = mg_get_string(r_username, &r[2], &len);
   password = mg_get_string(r_password, &r[3], &len);

   strcpy(p_page->p_srv->ip_address, netname);
   p_page->p_srv->port = (int) strtol(port, NULL, 10);
   strcpy(p_page->p_srv->username, username);
   strcpy(p_page->p_srv->password, password);

   return rb_str_new2("");

}

static VALUE ex_m_set_uci(VALUE self, VALUE r_uci)
{
   int len, phndle;
   char * uci;
   MGPAGE *p_page;
   VALUE r;

   phndle = 0;
   p_page = mg_ppage(phndle);

   uci = mg_get_string(r_uci, &r, &len);

   strcpy(p_page->p_srv->uci, uci);

   return rb_str_new2("");
}


static VALUE ex_m_set_server(VALUE self, VALUE r_server)
{
   int len, phndle;
   char * server;
   MGPAGE *p_page;
   VALUE r;

   phndle = 0;
   p_page = mg_ppage(phndle);

   server = mg_get_string(r_server, &r, &len);

   strcpy(p_page->p_srv->server, server);

   return rb_str_new2("");
}


/* v2.2.41 */
static VALUE ex_m_set_timeout(VALUE self, VALUE r_timeout)
{
   int phndle, timeout;
   MGPAGE *p_page;

   phndle = 0;
   p_page = mg_ppage(phndle);

   timeout = mg_get_integer(r_timeout);

   if (timeout >= 0) {
      p_page->p_srv->timeout = timeout;
   }

   return rb_str_new2("");

}


static VALUE ex_m_bind_server_api(VALUE self, VALUE r_dbtype_name, VALUE r_path, VALUE r_username, VALUE r_password, VALUE r_env, VALUE r_params)
{
   int result, phndle, len;
   char buffer[32];
   char *dbtype_name, *path, *username, *password, *env, *params;
   MGPAGE *p_page;
   VALUE r[32];

   result = 0;
   phndle = 0;
   p_page = mg_ppage(phndle);

   dbtype_name = mg_get_string(r_dbtype_name, &r[0], &len);
   path = mg_get_string(r_path, &r[1], &len);
   username = mg_get_string(r_username, &r[2], &len);
   password = mg_get_string(r_password, &r[3], &len);
   env = mg_get_string(r_env, &r[4], &len);
   params = mg_get_string(r_params, &r[5], &len);

   strcpy(p_page->p_srv->dbtype_name, dbtype_name);
   strcpy(p_page->p_srv->shdir, path);
   strcpy(p_page->p_srv->username, password);
   strcpy(p_page->p_srv->password, password);

   p_page->p_srv->p_env = (MGBUF *) mg_malloc(sizeof(MGBUF), 0);
   mg_buf_init(p_page->p_srv->p_env, MG_BUFSIZE, MG_BUFSIZE);
   mg_buf_cpy(p_page->p_srv->p_env, env, (int) strlen(env));

   result = mg_bind_server_api(p_page->p_srv, 0);

   if (!result) {
      if (!strlen(p_page->p_srv->error_mess)) {
         strcpy(p_page->p_srv->error_mess, "The server API is not available on this host");
      }
      MG_ERROR(p_page->p_srv->error_mess);
      return mg_r_nil;
   }

   sprintf(buffer, "%d", result);
   return rb_str_new2(buffer);
}


static VALUE ex_m_release_server_api(VALUE self, VALUE args)
{
   int result, phndle;
   char buffer[32];
   MGPAGE *p_page;

   result = 0;
   phndle = 0;
   p_page = mg_ppage(phndle);

   result = mg_release_server_api(p_page->p_srv, 0);

   sprintf(buffer, "%d", result);
   return rb_str_new2(buffer);
}



static VALUE ex_m_get_last_error(VALUE self)
{

   return rb_str_new2("");

}


static VALUE ex_m_set(int argc, VALUE *argv, VALUE self)
{
   MGBUF mgbuf, *p_buf;
   int n, max;
   char ifc[4];
   int chndle, phndle;
   MGPAGE *p_page;
   MGVARGS vargs;

   if ((max = mg_get_vargs(argc, argv, &vargs, 0)) == -1)
      return mg_r_nil;

   phndle = 0;
   p_page = mg_ppage(phndle);

   p_buf = &mgbuf;
   mg_buf_init(p_buf, MG_BUFSIZE, MG_BUFSIZE);

   MG_FTRACE("m_set");

   n = mg_db_connect(p_page->p_srv, &chndle, 1);

   if (!n) {
      MG_ERROR(p_page->p_srv->error_mess);
      return mg_r_nil;
   }

   mg_request_header(p_page->p_srv, p_buf, "S", MG_PRODUCT);

   ifc[0] = 0;
   ifc[1] = MG_TX_DATA;
   mg_request_add(p_page->p_srv, chndle, p_buf, (unsigned char *) vargs.global, (int) vargs.global_len, (short) ifc[0], (short) ifc[1]);

   for (n = 1; n < max; n ++) {
      ifc[0] = 0;
      ifc[1] = MG_TX_DATA;
      mg_request_add(p_page->p_srv, chndle, p_buf, vargs.cvars[n].ps, vargs.cvars[n].size, (short) ifc[0], (short) ifc[1]);
   }

   MG_MEMCHECK("Insufficient memory to process request", 1);

   mg_db_send(p_page->p_srv, chndle, p_buf, 1);

   mg_db_receive(p_page->p_srv, chndle, p_buf, MG_BUFSIZE, 0);

   MG_MEMCHECK("Insufficient memory to process response", 0);

   mg_db_disconnect(p_page->p_srv, chndle, 1);

   if ((n = mg_get_error(p_page->p_srv, p_buf->p_buffer))) {
      MG_ERROR(p_buf->p_buffer + MG_RECV_HEAD);
      return mg_r_nil;
   }

   return rb_str_new(p_buf->p_buffer + MG_RECV_HEAD, p_buf->data_size - MG_RECV_HEAD);
}


static VALUE ex_ma_set(VALUE self, VALUE r_global, VALUE key, VALUE r_data)
{
   MGBUF mgbuf, *p_buf;
   int n, max, data_len, len;
   char ifc[4];
   char *global, *data;
   MGSTR nkey[MG_MAX_KEY];
   int chndle, phndle;
   MGPAGE *p_page;
   VALUE p;
   VALUE r_nkey[MG_MAX_KEY];

   if (mg_type(key) != MG_T_LIST) {
      MG_ERROR("mg_ruby: Argument 2 to 'ma_set' must be an array");
      return mg_r_nil;
   }

   phndle = 0;
   p_page = mg_ppage(phndle);

   p_buf = &mgbuf;
   mg_buf_init(p_buf, MG_BUFSIZE, MG_BUFSIZE);

   MG_FTRACE("ma_set");

   n = mg_db_connect(p_page->p_srv, &chndle, 1);
   if (!n) {
      MG_ERROR(p_page->p_srv->error_mess);
      return mg_r_nil;
   }

   mg_request_header(p_page->p_srv, p_buf, "S", MG_PRODUCT);

   max = mg_get_keys(key, nkey, r_nkey, NULL);
   global = mg_get_string(r_global, &p, &len);
   data = mg_get_string(r_data, &p, &data_len);

   ifc[0] = 0;
   ifc[1] = MG_TX_DATA;
   mg_request_add(p_page->p_srv, chndle, p_buf, (unsigned char *) global, (int) strlen((char *) global), (short) ifc[0], (short) ifc[1]);

   for (n = 1; n <= max; n ++) {
      ifc[0] = 0;
      ifc[1] = MG_TX_DATA;
      mg_request_add(p_page->p_srv, chndle, p_buf, nkey[n].ps, nkey[n].size, (short) ifc[0], (short) ifc[1]);
   }
   ifc[0] = 0;
   ifc[1] = MG_TX_DATA;
   mg_request_add(p_page->p_srv, chndle, p_buf, (unsigned char *) data, data_len, (short) ifc[0], (short) ifc[1]);

   MG_MEMCHECK("Insufficient memory to process request", 1);

   mg_db_send(p_page->p_srv, chndle, p_buf, 1);

   mg_db_receive(p_page->p_srv, chndle, p_buf, MG_BUFSIZE, 0);

   MG_MEMCHECK("Insufficient memory to process response", 0);

   mg_db_disconnect(p_page->p_srv, chndle, 1);

   if ((n = mg_get_error(p_page->p_srv, p_buf->p_buffer))) {
      MG_ERROR(p_buf->p_buffer + MG_RECV_HEAD);
      return mg_r_nil;
   }

   return rb_str_new(p_buf->p_buffer + MG_RECV_HEAD, p_buf->data_size - MG_RECV_HEAD);

}


static VALUE ex_m_get(int argc, VALUE *argv, VALUE self)
{
   MGBUF mgbuf, *p_buf;
   int n, max;
   char ifc[4];
   int chndle, phndle;
   MGPAGE *p_page;
   MGVARGS vargs;

   if ((max = mg_get_vargs(argc, argv, &vargs, 0)) == -1)
      return mg_r_nil;

   phndle = 0;
   p_page = mg_ppage(phndle);

   p_buf = &mgbuf;
   mg_buf_init(p_buf, MG_BUFSIZE, MG_BUFSIZE);

   MG_FTRACE("m_get");

   n = mg_db_connect(p_page->p_srv, &chndle, 1);

   if (!n) {
      MG_ERROR(p_page->p_srv->error_mess);
      return mg_r_nil;
   }

   mg_request_header(p_page->p_srv, p_buf, "G", MG_PRODUCT);

   ifc[0] = 0;
   ifc[1] = MG_TX_DATA;
   mg_request_add(p_page->p_srv, chndle, p_buf, (unsigned char *) vargs.global, (int) vargs.global_len, (short) ifc[0], (short) ifc[1]);

   for (n = 1; n < max; n ++) {
      ifc[0] = 0;
      ifc[1] = MG_TX_DATA;
      mg_request_add(p_page->p_srv, chndle, p_buf, vargs.cvars[n].ps, vargs.cvars[n].size, (short) ifc[0], (short) ifc[1]);
   }

   MG_MEMCHECK("Insufficient memory to process request", 1);

   mg_db_send(p_page->p_srv, chndle, p_buf, 1);

   mg_db_receive(p_page->p_srv, chndle, p_buf, MG_BUFSIZE, 0);

   MG_MEMCHECK("Insufficient memory to process response", 0);

   mg_db_disconnect(p_page->p_srv, chndle, 1);

   if ((n = mg_get_error(p_page->p_srv, p_buf->p_buffer))) {
      MG_ERROR(p_buf->p_buffer + MG_RECV_HEAD);
      return mg_r_nil;
   }

   return rb_str_new(p_buf->p_buffer + MG_RECV_HEAD, p_buf->data_size - MG_RECV_HEAD);
}


static VALUE ex_ma_get(VALUE self, VALUE r_global, VALUE key)
{
   MGBUF mgbuf, *p_buf;
   int n, max, len;
   char *global;
   char ifc[4];
   MGSTR nkey[MG_MAX_KEY];
   int chndle, phndle;
   MGPAGE *p_page;
   VALUE r_nkey[MG_MAX_KEY];
   VALUE p;

   if (mg_type(key) != MG_T_LIST) {
      MG_ERROR("mg_ruby: Argument 2 to 'ma_get' must be an array");
      return mg_r_nil;
   }

   phndle = 0;
   p_page = mg_ppage(phndle);

   p_buf = &mgbuf;
   mg_buf_init(p_buf, MG_BUFSIZE, MG_BUFSIZE);

   MG_FTRACE("ma_get");

   n = mg_db_connect(p_page->p_srv, &chndle, 1);
   if (!n) {
      MG_ERROR(p_page->p_srv->error_mess);
   }

   mg_request_header(p_page->p_srv, p_buf, "G", MG_PRODUCT);
   max = mg_get_keys(key, nkey, r_nkey, NULL);
   global = mg_get_string(r_global, &p, &len);

   ifc[0] = 0;
   ifc[1] = MG_TX_DATA;
   mg_request_add(p_page->p_srv, chndle, p_buf, (unsigned char *) global, (int) strlen((char *) global), (short) ifc[0], (short) ifc[1]);

   for (n = 1; n <= max; n ++) {
      ifc[0] = 0;
      ifc[1] = MG_TX_DATA;
      mg_request_add(p_page->p_srv, chndle, p_buf, nkey[n].ps, nkey[n].size, (short) ifc[0], (short) ifc[1]);
   }

   MG_MEMCHECK("Insufficient memory to process request", 1);

   mg_db_send(p_page->p_srv, chndle, p_buf, 1);

   mg_db_receive(p_page->p_srv, chndle, p_buf, MG_BUFSIZE, 0);

   MG_MEMCHECK("Insufficient memory to process response", 0);

   mg_db_disconnect(p_page->p_srv, chndle, 1);

   if ((n = mg_get_error(p_page->p_srv, p_buf->p_buffer))) {
      MG_ERROR(p_buf->p_buffer + MG_RECV_HEAD);
      return mg_r_nil;
   }

   return rb_str_new(p_buf->p_buffer + MG_RECV_HEAD, p_buf->data_size - MG_RECV_HEAD);
}


static VALUE ex_m_kill(int argc, VALUE *argv, VALUE self)
{
   MGBUF mgbuf, *p_buf;
   int n, max;
   char ifc[4];
   int chndle, phndle;
   MGPAGE *p_page;
   MGVARGS vargs;

   if ((max = mg_get_vargs(argc, argv, &vargs, 0)) == -1)
      return mg_r_nil;

   phndle = 0;
   p_page = mg_ppage(phndle);

   p_buf = &mgbuf;
   mg_buf_init(p_buf, MG_BUFSIZE, MG_BUFSIZE);

   MG_FTRACE("m_kill");

   n = mg_db_connect(p_page->p_srv, &chndle, 1);

   if (!n) {
      MG_ERROR(p_page->p_srv->error_mess);
      return mg_r_nil;
   }

   mg_request_header(p_page->p_srv, p_buf, "K", MG_PRODUCT);

   ifc[0] = 0;
   ifc[1] = MG_TX_DATA;
   mg_request_add(p_page->p_srv, chndle, p_buf, (unsigned char *) vargs.global, (int) vargs.global_len, (short) ifc[0], (short) ifc[1]);

   for (n = 1; n < max; n ++) {
      ifc[0] = 0;
      ifc[1] = MG_TX_DATA;
      mg_request_add(p_page->p_srv, chndle, p_buf, vargs.cvars[n].ps, vargs.cvars[n].size, (short) ifc[0], (short) ifc[1]);
   }

   MG_MEMCHECK("Insufficient memory to process request", 1);

   mg_db_send(p_page->p_srv, chndle, p_buf, 1);

   mg_db_receive(p_page->p_srv, chndle, p_buf, MG_BUFSIZE, 0);

   MG_MEMCHECK("Insufficient memory to process response", 0);

   mg_db_disconnect(p_page->p_srv, chndle, 1);

   if ((n = mg_get_error(p_page->p_srv, p_buf->p_buffer))) {
      MG_ERROR(p_buf->p_buffer + MG_RECV_HEAD);
      return mg_r_nil;
   }

   return rb_str_new(p_buf->p_buffer + MG_RECV_HEAD, p_buf->data_size - MG_RECV_HEAD);
}


static VALUE ex_ma_kill(VALUE self, VALUE r_global, VALUE key)
{
   MGBUF mgbuf, *p_buf;
   int n, max, len;
   char *global;
   char ifc[4];
   MGSTR nkey[MG_MAX_KEY];
   int chndle, phndle;
   MGPAGE *p_page;
   VALUE r_nkey[MG_MAX_KEY];
   VALUE p;

   if (mg_type(key) != MG_T_LIST) {
      MG_ERROR("mg_ruby: Argument 2 to 'ma_kill' must be an array");
      return mg_r_nil;
   }

   phndle = 0;
   p_page = mg_ppage(phndle);

   p_buf = &mgbuf;
   mg_buf_init(p_buf, MG_BUFSIZE, MG_BUFSIZE);

   MG_FTRACE("ma_kill");

   n = mg_db_connect(p_page->p_srv, &chndle, 1);
   if (!n) {
      MG_ERROR(p_page->p_srv->error_mess);
      return mg_r_nil;
   }

   mg_request_header(p_page->p_srv, p_buf, "K", MG_PRODUCT);

   max = mg_get_keys(key, nkey, r_nkey, NULL);
   global = mg_get_string(r_global, &p, &len);

   ifc[0] = 0;
   ifc[1] = MG_TX_DATA;
   mg_request_add(p_page->p_srv, chndle, p_buf, (unsigned char *) global, (int) strlen((char *) global), (short) ifc[0], (short) ifc[1]);

   for (n = 1; n <= max; n ++) {
      ifc[0] = 0;
      ifc[1] = MG_TX_DATA;
      mg_request_add(p_page->p_srv, chndle, p_buf, nkey[n].ps, nkey[n].size, (short) ifc[0], (short) ifc[1]);
   }

   MG_MEMCHECK("Insufficient memory to process request", 1);

   mg_db_send(p_page->p_srv, chndle, p_buf, 1);

   mg_db_receive(p_page->p_srv, chndle, p_buf, MG_BUFSIZE, 0);

   MG_MEMCHECK("Insufficient memory to process response", 0);

   mg_db_disconnect(p_page->p_srv, chndle, 1);

   if ((n = mg_get_error(p_page->p_srv, p_buf->p_buffer))) {
      MG_ERROR(p_buf->p_buffer + MG_RECV_HEAD);
      return mg_r_nil;
   }

   return rb_str_new(p_buf->p_buffer + MG_RECV_HEAD, p_buf->data_size - MG_RECV_HEAD);
}


static VALUE ex_m_data(int argc, VALUE *argv, VALUE self)
{
   MGBUF mgbuf, *p_buf;
   int n, max;
   char ifc[4];
   int chndle, phndle;
   MGPAGE *p_page;
   MGVARGS vargs;

   if ((max = mg_get_vargs(argc, argv, &vargs, 0)) == -1)
      return mg_r_nil;

   phndle = 0;
   p_page = mg_ppage(phndle);

   p_buf = &mgbuf;
   mg_buf_init(p_buf, MG_BUFSIZE, MG_BUFSIZE);

   MG_FTRACE("m_data");

   n = mg_db_connect(p_page->p_srv, &chndle, 1);

   if (!n) {
      MG_ERROR(p_page->p_srv->error_mess);
      return mg_r_nil;
   }

   mg_request_header(p_page->p_srv, p_buf, "D", MG_PRODUCT);

   ifc[0] = 0;
   ifc[1] = MG_TX_DATA;
   mg_request_add(p_page->p_srv, chndle, p_buf, (unsigned char *) vargs.global, (int) vargs.global_len, (short) ifc[0], (short) ifc[1]);

   for (n = 1; n < max; n ++) {
      ifc[0] = 0;
      ifc[1] = MG_TX_DATA;
      mg_request_add(p_page->p_srv, chndle, p_buf, vargs.cvars[n].ps, vargs.cvars[n].size, (short) ifc[0], (short) ifc[1]);
   }

   MG_MEMCHECK("Insufficient memory to process request", 1);

   mg_db_send(p_page->p_srv, chndle, p_buf, 1);

   mg_db_receive(p_page->p_srv, chndle, p_buf, MG_BUFSIZE, 0);

   MG_MEMCHECK("Insufficient memory to process response", 0);

   mg_db_disconnect(p_page->p_srv, chndle, 1);

   if ((n = mg_get_error(p_page->p_srv, p_buf->p_buffer))) {
      MG_ERROR(p_buf->p_buffer + MG_RECV_HEAD);
      return mg_r_nil;
   }

   return rb_str_new(p_buf->p_buffer + MG_RECV_HEAD, p_buf->data_size - MG_RECV_HEAD);
}


static VALUE ex_ma_data(VALUE self, VALUE r_global, VALUE key)
{
   MGBUF mgbuf, *p_buf;
   int n, max, len;
   char *global;
   char ifc[4];
   MGSTR nkey[MG_MAX_KEY];
   int chndle, phndle;
   MGPAGE *p_page;
   VALUE r_nkey[MG_MAX_KEY];
   VALUE p;

   if (mg_type(key) != MG_T_LIST) {
      MG_ERROR("mg_ruby: Argument 2 to 'ma_data' must be an array");
      return mg_r_nil;
   }

   phndle = 0;
   p_page = mg_ppage(phndle);

   p_buf = &mgbuf;
   mg_buf_init(p_buf, MG_BUFSIZE, MG_BUFSIZE);

   MG_FTRACE("ma_data");

   n = mg_db_connect(p_page->p_srv, &chndle, 1);
   if (!n) {
      MG_ERROR(p_page->p_srv->error_mess);
      return mg_r_nil;
   }

   mg_request_header(p_page->p_srv, p_buf, "D", MG_PRODUCT);

   max = mg_get_keys(key, nkey, r_nkey, NULL);
   global = mg_get_string(r_global, &p, &len);

   ifc[0] = 0;
   ifc[1] = MG_TX_DATA;
   mg_request_add(p_page->p_srv, chndle, p_buf, (unsigned char *) global, (int) strlen((char *) global), (short) ifc[0], (short) ifc[1]);

   for (n = 1; n <= max; n ++) {
      ifc[0] = 0;
      ifc[1] = MG_TX_DATA;
      mg_request_add(p_page->p_srv, chndle, p_buf, nkey[n].ps, nkey[n].size, (short) ifc[0], (short) ifc[1]);
   }

   MG_MEMCHECK("Insufficient memory to process request", 1);

   mg_db_send(p_page->p_srv, chndle, p_buf, 1);

   mg_db_receive(p_page->p_srv, chndle, p_buf, MG_BUFSIZE, 0);

   MG_MEMCHECK("Insufficient memory to process response", 0);

   mg_db_disconnect(p_page->p_srv, chndle, 1);

   if ((n = mg_get_error(p_page->p_srv, p_buf->p_buffer))) {
      MG_ERROR(p_buf->p_buffer + MG_RECV_HEAD);
      return mg_r_nil;
   }

   return rb_str_new(p_buf->p_buffer + MG_RECV_HEAD, p_buf->data_size - MG_RECV_HEAD);
}


static VALUE ex_m_order(int argc, VALUE *argv, VALUE self)
{
   MGBUF mgbuf, *p_buf;
   int n, max;
   char ifc[4];
   int chndle, phndle;
   MGPAGE *p_page;
   MGVARGS vargs;

   if ((max = mg_get_vargs(argc, argv, &vargs, 0)) == -1)
      return mg_r_nil;

   phndle = 0;
   p_page = mg_ppage(phndle);

   p_buf = &mgbuf;
   mg_buf_init(p_buf, MG_BUFSIZE, MG_BUFSIZE);

   MG_FTRACE("m_order");

   n = mg_db_connect(p_page->p_srv, &chndle, 1);

   if (!n) {
      MG_ERROR(p_page->p_srv->error_mess);
      return mg_r_nil;
   }

   mg_request_header(p_page->p_srv, p_buf, "O", MG_PRODUCT);

   ifc[0] = 0;
   ifc[1] = MG_TX_DATA;
   mg_request_add(p_page->p_srv, chndle, p_buf, (unsigned char *) vargs.global, (int) vargs.global_len, (short) ifc[0], (short) ifc[1]);

   for (n = 1; n < max; n ++) {
      ifc[0] = 0;
      ifc[1] = MG_TX_DATA;
      mg_request_add(p_page->p_srv, chndle, p_buf, vargs.cvars[n].ps, vargs.cvars[n].size, (short) ifc[0], (short) ifc[1]);
   }

   MG_MEMCHECK("Insufficient memory to process request", 1);

   mg_db_send(p_page->p_srv, chndle, p_buf, 1);

   mg_db_receive(p_page->p_srv, chndle, p_buf, MG_BUFSIZE, 0);

   MG_MEMCHECK("Insufficient memory to process response", 0);

   mg_db_disconnect(p_page->p_srv, chndle, 1);

   if ((n = mg_get_error(p_page->p_srv, p_buf->p_buffer))) {
      MG_ERROR(p_buf->p_buffer + MG_RECV_HEAD);
      return mg_r_nil;
   }

   return rb_str_new(p_buf->p_buffer + MG_RECV_HEAD, p_buf->data_size - MG_RECV_HEAD);
}


static VALUE ex_ma_order(VALUE self, VALUE r_global, VALUE key)
{
   MGBUF mgbuf, *p_buf;
   int n, max;
   char *global = NULL;
   char ifc[4];
   MGSTR nkey[MG_MAX_KEY];
   int chndle, phndle, len;
   MGPAGE *p_page;
   VALUE p;
   VALUE r_nkey[MG_MAX_KEY];


   if (mg_type(key) != MG_T_LIST) {
      MG_ERROR("mg_ruby: Argument 2 to 'ma_order' must be an array");
      return mg_r_nil;
   }

   phndle = 0;
   p_page = mg_ppage(phndle);

   p_buf = &mgbuf;
   mg_buf_init(p_buf, MG_BUFSIZE, MG_BUFSIZE);

   MG_FTRACE("ma_order");

   n = mg_db_connect(p_page->p_srv, &chndle, 1);
   if (!n) {
      MG_ERROR(p_page->p_srv->error_mess);
      return mg_r_nil;
   }

   mg_request_header(p_page->p_srv, p_buf, "O", MG_PRODUCT);

   max = mg_get_keys(key, nkey, r_nkey, NULL);
   global = mg_get_string(r_global, &p, &len);

   ifc[0] = 0;
   ifc[1] = MG_TX_DATA;
   mg_request_add(p_page->p_srv, chndle, p_buf, (unsigned char *) global, (int) strlen((char *) global), (short) ifc[0], (short) ifc[1]);

   for (n = 1; n <= max; n ++) {
      ifc[0] = 0;
      ifc[1] = MG_TX_DATA;
      mg_request_add(p_page->p_srv, chndle, p_buf, nkey[n].ps, nkey[n].size, (short) ifc[0], (short) ifc[1]);
   }

   MG_MEMCHECK("Insufficient memory to process request", 1);

   mg_db_send(p_page->p_srv, chndle, p_buf, 1);

   mg_db_receive(p_page->p_srv, chndle, p_buf, MG_BUFSIZE, 0);

   MG_MEMCHECK("Insufficient memory to process response", 0);

   mg_db_disconnect(p_page->p_srv, chndle, 1);

   if ((n = mg_get_error(p_page->p_srv, p_buf->p_buffer))) {
      MG_ERROR(p_buf->p_buffer + MG_RECV_HEAD);
      return mg_r_nil;
   }

   p = rb_str_new(p_buf->p_buffer + MG_RECV_HEAD, p_buf->data_size - MG_RECV_HEAD);
   mg_set_list_item(key, max, p);

   return rb_str_new(p_buf->p_buffer + MG_RECV_HEAD, p_buf->data_size - MG_RECV_HEAD);
}


static VALUE ex_m_previous(int argc, VALUE *argv, VALUE self)
{
   MGBUF mgbuf, *p_buf;
   int n, max;
   char ifc[4];
   int chndle, phndle;
   MGPAGE *p_page;
   MGVARGS vargs;

   if ((max = mg_get_vargs(argc, argv, &vargs, 0)) == -1)
      return mg_r_nil;

   phndle = 0;
   p_page = mg_ppage(phndle);

   p_buf = &mgbuf;
   mg_buf_init(p_buf, MG_BUFSIZE, MG_BUFSIZE);

   MG_FTRACE("m_previous");

   n = mg_db_connect(p_page->p_srv, &chndle, 1);

   if (!n) {
      MG_ERROR(p_page->p_srv->error_mess);
      return mg_r_nil;
   }

   mg_request_header(p_page->p_srv, p_buf, "P", MG_PRODUCT);

   ifc[0] = 0;
   ifc[1] = MG_TX_DATA;
   mg_request_add(p_page->p_srv, chndle, p_buf, (unsigned char *) vargs.global, (int) vargs.global_len, (short) ifc[0], (short) ifc[1]);

   for (n = 1; n < max; n ++) {
      ifc[0] = 0;
      ifc[1] = MG_TX_DATA;
      mg_request_add(p_page->p_srv, chndle, p_buf, vargs.cvars[n].ps, vargs.cvars[n].size, (short) ifc[0], (short) ifc[1]);
   }

   MG_MEMCHECK("Insufficient memory to process request", 1);

   mg_db_send(p_page->p_srv, chndle, p_buf, 1);

   mg_db_receive(p_page->p_srv, chndle, p_buf, MG_BUFSIZE, 0);

   MG_MEMCHECK("Insufficient memory to process response", 0);

   mg_db_disconnect(p_page->p_srv, chndle, 1);

   if ((n = mg_get_error(p_page->p_srv, p_buf->p_buffer))) {
      MG_ERROR(p_buf->p_buffer + MG_RECV_HEAD);
      return mg_r_nil;
   }

   return rb_str_new(p_buf->p_buffer + MG_RECV_HEAD, p_buf->data_size - MG_RECV_HEAD);
}


static VALUE ex_ma_previous(VALUE self, VALUE r_global, VALUE key)
{
   MGBUF mgbuf, *p_buf;
   int n, max, len;
   char *global = NULL;
   char ifc[4];
   MGSTR nkey[MG_MAX_KEY];
   int chndle, phndle;
   MGPAGE *p_page;
   VALUE p;
   VALUE r_nkey[MG_MAX_KEY];


   if (mg_type(key) != MG_T_LIST) {
      MG_ERROR("mg_ruby: Argument 2 to 'ma_order' must be an array");
      return mg_r_nil;
   }

   phndle = 0;
   p_page = mg_ppage(phndle);

   p_buf = &mgbuf;
   mg_buf_init(p_buf, MG_BUFSIZE, MG_BUFSIZE);

   MG_FTRACE("ma_order");

   n = mg_db_connect(p_page->p_srv, &chndle, 1);
   if (!n) {
      MG_ERROR(p_page->p_srv->error_mess);
      return mg_r_nil;
   }

   mg_request_header(p_page->p_srv, p_buf, "P", MG_PRODUCT);

   max = mg_get_keys(key, nkey, r_nkey, NULL);
   global = mg_get_string(r_global, &p, &len);

   ifc[0] = 0;
   ifc[1] = MG_TX_DATA;
   mg_request_add(p_page->p_srv, chndle, p_buf, (unsigned char *) global, (int) strlen((char *) global), (short) ifc[0], (short) ifc[1]);

   for (n = 1; n <= max; n ++) {
      ifc[0] = 0;
      ifc[1] = MG_TX_DATA;
      mg_request_add(p_page->p_srv, chndle, p_buf, nkey[n].ps, nkey[n].size, (short) ifc[0], (short) ifc[1]);
   }

   MG_MEMCHECK("Insufficient memory to process request", 1);

   mg_db_send(p_page->p_srv, chndle, p_buf, 1);

   mg_db_receive(p_page->p_srv, chndle, p_buf, MG_BUFSIZE, 0);

   MG_MEMCHECK("Insufficient memory to process response", 0);

   mg_db_disconnect(p_page->p_srv, chndle, 1);

   if ((n = mg_get_error(p_page->p_srv, p_buf->p_buffer))) {
      MG_ERROR(p_buf->p_buffer + MG_RECV_HEAD);
      return mg_r_nil;
   }

   p = rb_str_new(p_buf->p_buffer + MG_RECV_HEAD, p_buf->data_size - MG_RECV_HEAD);
   mg_set_list_item(key, max, p);

   return rb_str_new(p_buf->p_buffer + MG_RECV_HEAD, p_buf->data_size - MG_RECV_HEAD);
}


/* v2.2.41 */
static VALUE ex_m_increment(int argc, VALUE *argv, VALUE self)
{
   MGBUF mgbuf, *p_buf;
   int n, max;
   char ifc[4];
   int chndle, phndle;
   MGPAGE *p_page;
   MGVARGS vargs;

   if ((max = mg_get_vargs(argc, argv, &vargs, 0)) == -1)
      return mg_r_nil;

   phndle = 0;
   p_page = mg_ppage(phndle);

   p_buf = &mgbuf;
   mg_buf_init(p_buf, MG_BUFSIZE, MG_BUFSIZE);

   MG_FTRACE("m_increment");

   n = mg_db_connect(p_page->p_srv, &chndle, 1);

   if (!n) {
      MG_ERROR(p_page->p_srv->error_mess);
      return mg_r_nil;
   }

   mg_request_header(p_page->p_srv, p_buf, "I", MG_PRODUCT);

   ifc[0] = 0;
   ifc[1] = MG_TX_DATA;
   mg_request_add(p_page->p_srv, chndle, p_buf, (unsigned char *) vargs.global, (int) vargs.global_len, (short) ifc[0], (short) ifc[1]);

   for (n = 1; n < max; n ++) {
      ifc[0] = 0;
      ifc[1] = MG_TX_DATA;
      mg_request_add(p_page->p_srv, chndle, p_buf, vargs.cvars[n].ps, vargs.cvars[n].size, (short) ifc[0], (short) ifc[1]);
   }

   MG_MEMCHECK("Insufficient memory to process request", 1);

   mg_db_send(p_page->p_srv, chndle, p_buf, 1);

   mg_db_receive(p_page->p_srv, chndle, p_buf, MG_BUFSIZE, 0);

   MG_MEMCHECK("Insufficient memory to process response", 0);

   mg_db_disconnect(p_page->p_srv, chndle, 1);

   if ((n = mg_get_error(p_page->p_srv, p_buf->p_buffer))) {
      MG_ERROR(p_buf->p_buffer + MG_RECV_HEAD);
      return mg_r_nil;
   }

   return rb_str_new(p_buf->p_buffer + MG_RECV_HEAD, p_buf->data_size - MG_RECV_HEAD);
}


/* v2.2.41 */
static VALUE ex_m_tstart(int argc, VALUE *argv, VALUE self)
{
   MGBUF mgbuf, *p_buf;
   int n, max;
   char ifc[4];
   int chndle, phndle;
   MGPAGE *p_page;
   MGVARGS vargs;

   if ((max = mg_get_vargs(argc, argv, &vargs, 0)) == -1)
      return mg_r_nil;

   phndle = 0;
   p_page = mg_ppage(phndle);

   p_buf = &mgbuf;
   mg_buf_init(p_buf, MG_BUFSIZE, MG_BUFSIZE);

   MG_FTRACE("m_tstart");

   n = mg_db_connect(p_page->p_srv, &chndle, 1);

   if (!n) {
      MG_ERROR(p_page->p_srv->error_mess);
      return mg_r_nil;
   }

   mg_request_header(p_page->p_srv, p_buf, "a", MG_PRODUCT);

   for (n = 0; n < max; n ++) {
      ifc[0] = 0;
      ifc[1] = MG_TX_DATA;
      mg_request_add(p_page->p_srv, chndle, p_buf, vargs.cvars[n].ps, vargs.cvars[n].size, (short) ifc[0], (short) ifc[1]);
   }

   MG_MEMCHECK("Insufficient memory to process request", 1);

   mg_db_send(p_page->p_srv, chndle, p_buf, 1);

   mg_db_receive(p_page->p_srv, chndle, p_buf, MG_BUFSIZE, 0);

   MG_MEMCHECK("Insufficient memory to process response", 0);

   mg_db_disconnect(p_page->p_srv, chndle, 1);

   if ((n = mg_get_error(p_page->p_srv, p_buf->p_buffer))) {
      MG_ERROR(p_buf->p_buffer + MG_RECV_HEAD);
      return mg_r_nil;
   }

   return rb_str_new(p_buf->p_buffer + MG_RECV_HEAD, p_buf->data_size - MG_RECV_HEAD);
}


static VALUE ex_m_tlevel(int argc, VALUE *argv, VALUE self)
{
   MGBUF mgbuf, *p_buf;
   int n, max;
   char ifc[4];
   int chndle, phndle;
   MGPAGE *p_page;
   MGVARGS vargs;

   if ((max = mg_get_vargs(argc, argv, &vargs, 0)) == -1)
      return mg_r_nil;

   phndle = 0;
   p_page = mg_ppage(phndle);

   p_buf = &mgbuf;
   mg_buf_init(p_buf, MG_BUFSIZE, MG_BUFSIZE);

   MG_FTRACE("m_tlevel");

   n = mg_db_connect(p_page->p_srv, &chndle, 1);

   if (!n) {
      MG_ERROR(p_page->p_srv->error_mess);
      return mg_r_nil;
   }

   mg_request_header(p_page->p_srv, p_buf, "b", MG_PRODUCT);

   for (n = 0; n < max; n ++) {
      ifc[0] = 0;
      ifc[1] = MG_TX_DATA;
      mg_request_add(p_page->p_srv, chndle, p_buf, vargs.cvars[n].ps, vargs.cvars[n].size, (short) ifc[0], (short) ifc[1]);
   }

   MG_MEMCHECK("Insufficient memory to process request", 1);

   mg_db_send(p_page->p_srv, chndle, p_buf, 1);

   mg_db_receive(p_page->p_srv, chndle, p_buf, MG_BUFSIZE, 0);

   MG_MEMCHECK("Insufficient memory to process response", 0);

   mg_db_disconnect(p_page->p_srv, chndle, 1);

   if ((n = mg_get_error(p_page->p_srv, p_buf->p_buffer))) {
      MG_ERROR(p_buf->p_buffer + MG_RECV_HEAD);
      return mg_r_nil;
   }

   return rb_str_new(p_buf->p_buffer + MG_RECV_HEAD, p_buf->data_size - MG_RECV_HEAD);
}


static VALUE ex_m_tcommit(int argc, VALUE *argv, VALUE self)
{
   MGBUF mgbuf, *p_buf;
   int n, max;
   char ifc[4];
   int chndle, phndle;
   MGPAGE *p_page;
   MGVARGS vargs;

   if ((max = mg_get_vargs(argc, argv, &vargs, 0)) == -1)
      return mg_r_nil;

   phndle = 0;
   p_page = mg_ppage(phndle);

   p_buf = &mgbuf;
   mg_buf_init(p_buf, MG_BUFSIZE, MG_BUFSIZE);

   MG_FTRACE("m_tcommit");

   n = mg_db_connect(p_page->p_srv, &chndle, 1);

   if (!n) {
      MG_ERROR(p_page->p_srv->error_mess);
      return mg_r_nil;
   }

   mg_request_header(p_page->p_srv, p_buf, "c", MG_PRODUCT);

   for (n = 0; n < max; n ++) {
      ifc[0] = 0;
      ifc[1] = MG_TX_DATA;
      mg_request_add(p_page->p_srv, chndle, p_buf, vargs.cvars[n].ps, vargs.cvars[n].size, (short) ifc[0], (short) ifc[1]);
   }

   MG_MEMCHECK("Insufficient memory to process request", 1);

   mg_db_send(p_page->p_srv, chndle, p_buf, 1);

   mg_db_receive(p_page->p_srv, chndle, p_buf, MG_BUFSIZE, 0);

   MG_MEMCHECK("Insufficient memory to process response", 0);

   mg_db_disconnect(p_page->p_srv, chndle, 1);

   if ((n = mg_get_error(p_page->p_srv, p_buf->p_buffer))) {
      MG_ERROR(p_buf->p_buffer + MG_RECV_HEAD);
      return mg_r_nil;
   }

   return rb_str_new(p_buf->p_buffer + MG_RECV_HEAD, p_buf->data_size - MG_RECV_HEAD);
}


static VALUE ex_m_trollback(int argc, VALUE *argv, VALUE self)
{
   MGBUF mgbuf, *p_buf;
   int n, max;
   char ifc[4];
   int chndle, phndle;
   MGPAGE *p_page;
   MGVARGS vargs;

   if ((max = mg_get_vargs(argc, argv, &vargs, 0)) == -1)
      return mg_r_nil;

   phndle = 0;
   p_page = mg_ppage(phndle);

   p_buf = &mgbuf;
   mg_buf_init(p_buf, MG_BUFSIZE, MG_BUFSIZE);

   MG_FTRACE("m_trollback");

   n = mg_db_connect(p_page->p_srv, &chndle, 1);

   if (!n) {
      MG_ERROR(p_page->p_srv->error_mess);
      return mg_r_nil;
   }

   mg_request_header(p_page->p_srv, p_buf, "d", MG_PRODUCT);

   for (n = 0; n < max; n ++) {
      ifc[0] = 0;
      ifc[1] = MG_TX_DATA;
      mg_request_add(p_page->p_srv, chndle, p_buf, vargs.cvars[n].ps, vargs.cvars[n].size, (short) ifc[0], (short) ifc[1]);
   }

   MG_MEMCHECK("Insufficient memory to process request", 1);

   mg_db_send(p_page->p_srv, chndle, p_buf, 1);

   mg_db_receive(p_page->p_srv, chndle, p_buf, MG_BUFSIZE, 0);

   MG_MEMCHECK("Insufficient memory to process response", 0);

   mg_db_disconnect(p_page->p_srv, chndle, 1);

   if ((n = mg_get_error(p_page->p_srv, p_buf->p_buffer))) {
      MG_ERROR(p_buf->p_buffer + MG_RECV_HEAD);
      return mg_r_nil;
   }

   return rb_str_new(p_buf->p_buffer + MG_RECV_HEAD, p_buf->data_size - MG_RECV_HEAD);
}


/* v2.2.41 */
static VALUE ex_m_sleep(VALUE self, VALUE r_msecs)
{
   int msecs;

   msecs = mg_get_integer(r_msecs);
   mg_sleep((unsigned long) msecs);
   msecs = 0;

   return rb_str_new2("");
}


static VALUE ex_ma_merge_to_db(VALUE self, VALUE r_global, VALUE key, VALUE records, VALUE r_options)
{
   MGBUF mgbuf, *p_buf;
   int n, max, mrec, rn, len;
   char ifc[4];
   char *global, *options, *ps;
   MGSTR nkey[MG_MAX_KEY];
   VALUE r_nkey[MG_MAX_KEY];
   VALUE p;
   VALUE temp;
   int chndle, phndle;
   MGPAGE *p_page;


   if (mg_type(key) != MG_T_LIST) {
      MG_ERROR("mg_ruby: Argument 2 to 'ma_merge_to_db' must be an array");
      return mg_r_nil;
   }
   if (mg_type(records) != MG_T_LIST) {
      MG_ERROR("mg_ruby: Argument 3 to 'ma_merge_to_db' must be an array");
      return mg_r_nil;
   }

   phndle = 0;
   p_page = mg_ppage(phndle);

   p_buf = &mgbuf;
   mg_buf_init(p_buf, MG_BUFSIZE, MG_BUFSIZE);

   MG_FTRACE("ma_merge_to_db");

   global = mg_get_string(r_global, &p, &len);
   options = mg_get_string(r_options, &p, &len);

   mrec = mg_get_array_size(records);
   max = mg_get_keys(key, nkey, r_nkey, NULL);

   n = mg_db_connect(p_page->p_srv, &chndle, 1);
   if (!n) {
      MG_ERROR(p_page->p_srv->error_mess);
      return mg_r_nil;
   }

   mg_request_header(p_page->p_srv, p_buf, "M", MG_PRODUCT);

   ifc[0] = 0;
   ifc[1] = MG_TX_DATA;
   mg_request_add(p_page->p_srv, chndle, p_buf, (unsigned char *) global, (int) strlen((char *) global), (short) ifc[0], (short) ifc[1]);

   for (n = 1; n <= max; n ++) {
      ifc[0] = 0;
      ifc[1] = MG_TX_DATA;
      mg_request_add(p_page->p_srv, chndle, p_buf, nkey[n].ps, nkey[n].size, (short) ifc[0], (short) ifc[1]);
   }

   ifc[0] = 0;
   ifc[1] = MG_TX_AREC;
   mg_request_add(p_page->p_srv, chndle, p_buf, NULL, 0, (short) ifc[0], (short) ifc[1]);

   n = 0;
   for (rn = 0; rn < mrec; rn ++) {
      p = rb_ary_entry(records, rn);
      ps = mg_get_string(p, &temp, &len);

      if (rn == 0) {
         ifc[0] = 0;
         ifc[1] = MG_TX_AREC_FORMATTED;
      }
      mg_request_add(p_page->p_srv, chndle, p_buf, (unsigned char *) ps, len, (short) ifc[0], (short) ifc[1]);
   }
   ifc[0] = 0;
   ifc[1] = MG_TX_EOD;
   mg_request_add(p_page->p_srv, chndle, p_buf, NULL, 0, (short) ifc[0], (short) ifc[1]);

   ifc[0] = 0;
   ifc[1] = MG_TX_DATA;
   mg_request_add(p_page->p_srv, chndle, p_buf, (unsigned char *) options, (int) strlen((char *) options), (short) ifc[0], (short) ifc[1]);

   MG_MEMCHECK("Insufficient memory to process request", 1);

   mg_db_send(p_page->p_srv, chndle, p_buf, 1);

   mg_db_receive(p_page->p_srv, chndle, p_buf, MG_BUFSIZE, 0);

   MG_MEMCHECK("Insufficient memory to process response", 0);

   mg_db_disconnect(p_page->p_srv, chndle, 1);

   if ((n = mg_get_error(p_page->p_srv, p_buf->p_buffer))) {
      MG_ERROR(p_buf->p_buffer + MG_RECV_HEAD);
      return mg_r_nil;
   }

   return rb_str_new(p_buf->p_buffer + MG_RECV_HEAD, p_buf->data_size - MG_RECV_HEAD);

}



static VALUE ex_ma_merge_from_db(VALUE self, VALUE r_global, VALUE key, VALUE records, VALUE r_options)
{

   MGBUF mgbuf, *p_buf;
   int n, max, mrec, len, anybyref;
   char ifc[4];
   char *global, *options;
   MGSTR nkey[MG_MAX_KEY];
   VALUE r_nkey[MG_MAX_KEY];
   VALUE p;
   int chndle, phndle;
   MGPAGE *p_page;


   if (mg_type(key) != MG_T_LIST) {
      MG_ERROR("mg_ruby: Argument 2 to 'ma_merge_from_db' must be an array");
      return mg_r_nil;
   }
   if (mg_type(records) != MG_T_LIST) {
      MG_ERROR("mg_ruby: Argument 3 to 'ma_merge_from_db' must be an array");
      return mg_r_nil;
   }

   phndle = 0;
   p_page = mg_ppage(phndle);

   p_buf = &mgbuf;
   mg_buf_init(p_buf, MG_BUFSIZE, MG_BUFSIZE);

   MG_FTRACE("ma_merge_from_db");

   global = mg_get_string(r_global, &p, &len);
   options = mg_get_string(r_options, &p, &len);

   mrec = mg_get_array_size(records);
   max = mg_get_keys(key, nkey, r_nkey, NULL);

   n = mg_db_connect(p_page->p_srv, &chndle, 1);
   if (!n) {
      MG_ERROR(p_page->p_srv->error_mess);
      return mg_r_nil;
   }

   mg_request_header(p_page->p_srv, p_buf, "m", MG_PRODUCT);

   ifc[0] = 0;
   ifc[1] = MG_TX_DATA;
   mg_request_add(p_page->p_srv, chndle, p_buf, (unsigned char *) global, (int) strlen((char *) global), (short) ifc[0], (short) ifc[1]);

   for (n = 1; n <= max; n ++) {
      ifc[0] = 0;
      ifc[1] = MG_TX_DATA;
      mg_request_add(p_page->p_srv, chndle, p_buf, nkey[n].ps, nkey[n].size, (short) ifc[0], (short) ifc[1]);
   }

   anybyref = 1;

   ifc[0] = 1;
   ifc[1] = MG_TX_AREC;
   mg_request_add(p_page->p_srv, chndle, p_buf, NULL, 0, (short) ifc[0], (short) ifc[1]);
   ifc[0] = 0;
   ifc[1] = MG_TX_EOD;
   mg_request_add(p_page->p_srv, chndle, p_buf, NULL, 0, (short) ifc[0], (short) ifc[1]);

   ifc[0] = 0;
   ifc[1] = MG_TX_DATA;
   mg_request_add(p_page->p_srv, chndle, p_buf, (unsigned char *) options, (int) strlen((char *) options), (short) ifc[0], (short) ifc[1]);

   MG_MEMCHECK("Insufficient memory to process request", 1);

   mg_db_send(p_page->p_srv, chndle, p_buf, 1);

   mg_db_receive(p_page->p_srv, chndle, p_buf, MG_BUFSIZE, 0);

   MG_MEMCHECK("Insufficient memory to process response", 0);

   mg_db_disconnect(p_page->p_srv, chndle, 1);

   if ((n = mg_get_error(p_page->p_srv, p_buf->p_buffer))) {
      MG_ERROR(p_buf->p_buffer + MG_RECV_HEAD);
      return mg_r_nil;
   }

   if (anybyref) {
      short byref, type, stop;
      int n, n1, rn, hlen, size, clen, rlen, argc, rec_len;
      unsigned char *parg, *par;

      stop = 0;
      parg = (p_buf->p_buffer + MG_RECV_HEAD);

      clen = mg_decode_size(p_buf->p_buffer, 5, MG_CHUNK_SIZE_BASE);

      rlen = 0;
      argc = 0;
      for (n = 0;; n ++) {
         hlen = mg_decode_item_header(parg, &size, (short *) &byref, (short *) &type);
         if ((hlen + size + rlen) > clen) {
            stop = 1;
            break;
         }
         parg += hlen;
         rlen += hlen;
/*
{
   unsigned char c;
   char buffer[256];
   c = *(parg + size);
   *(parg + size) = '\0';
   sprintf(buffer, "RESULT %d: argc=%d; hlen=%d; size=%d; byref=%d; type=%d; rlen=%d; clen=%d", n, argc, hlen, size, byref, type, rlen, clen);
   mg_log_event(parg, buffer);
   *(parg + size) = c;
}
*/
         parg += size;
         rlen += size;
         if (type == MG_TX_AREC) {
            par = parg;
            rn = 0;
            rec_len = 0;
            for (n1 = 0;; n1 ++) {
               hlen = mg_decode_item_header(parg, &size, (short *) &byref, (short *) &type);
               if ((hlen + size + rlen) > clen) {
                  stop = 1;
                  break;
               }
               if (type == MG_TX_EOD) {
                  parg += (hlen + size);
                  rlen += (hlen + size);
                  break;
               }
               parg += hlen;
               rlen += hlen;
               rec_len += hlen;
/*
{
   unsigned char c;
   char buffer[256];
   c = *(parg + size);
   *(parg + size) = '\0';
   sprintf(buffer, "RESULT ARRAY %d: argc=%d; hlen=%d; size=%d; byref=%d; type=%d; rlen=%d; clen=%d", n1, argc, hlen, size, byref, type, rlen, clen);
   mg_log_event(parg, buffer);
   *(parg + size) = c;
}
*/
               parg += size;
               rlen += size;
               rec_len += size;
               if (type == MG_TX_DATA) {
/*
{
   unsigned char c;
   char buffer[256];
   c = *(parg + size);
   *(parg + size) = '\0';
   sprintf(buffer, "RESULT ARRAY RECORD %d: argc=%d; hlen=%d; size=%d; byref=%d; type=%d; rlen=%d; clen=%d; rec_len=%d", n1, argc, hlen, size, byref, type, rlen, clen, rec_len);
   mg_log_event(par, buffer);
   *(parg + size) = c;
}
*/
                  p = rb_str_new(par, rec_len);
                  mg_set_list_item(records, rn ++, p);

                  par = parg;
                  rec_len = 0;
               }
            }
         }
         if (rlen >= clen || stop)
            break;
         argc ++;
      }
      if (stop) {
         MG_ERROR("ma_merge_from_db: Bad return data");
         return mg_r_nil;
      }
      return rb_str_new(p_buf->p_buffer + MG_RECV_HEAD, p_buf->data_size - MG_RECV_HEAD);
   }

   return rb_str_new(p_buf->p_buffer + MG_RECV_HEAD, p_buf->data_size - MG_RECV_HEAD);

}


static VALUE ex_m_function(int argc, VALUE *argv, VALUE self)
{
   MGBUF mgbuf, *p_buf;
   int n, max;
   char ifc[4];
   int chndle, phndle;
   MGPAGE *p_page;
   MGVARGS vargs;

   if ((max = mg_get_vargs(argc, argv, &vargs, 0)) == -1)
      return mg_r_nil;

   phndle = 0;
   p_page = mg_ppage(phndle);

   p_buf = &mgbuf;
   mg_buf_init(p_buf, MG_BUFSIZE, MG_BUFSIZE);

   MG_FTRACE("m_function");

   n = mg_db_connect(p_page->p_srv, &chndle, 1);

   if (!n) {
      MG_ERROR(p_page->p_srv->error_mess);
      return mg_r_nil;
   }

   mg_request_header(p_page->p_srv, p_buf, "X", MG_PRODUCT);

   ifc[0] = 0;
   ifc[1] = MG_TX_DATA;
   mg_request_add(p_page->p_srv, chndle, p_buf, (unsigned char *) vargs.global, (int) vargs.global_len, (short) ifc[0], (short) ifc[1]);

   for (n = 1; n < max; n ++) {
      ifc[0] = 0;
      ifc[1] = MG_TX_DATA;
      mg_request_add(p_page->p_srv, chndle, p_buf, vargs.cvars[n].ps, vargs.cvars[n].size, (short) ifc[0], (short) ifc[1]);
   }

   MG_MEMCHECK("Insufficient memory to process request", 1);

   mg_db_send(p_page->p_srv, chndle, p_buf, 1);

   mg_db_receive(p_page->p_srv, chndle, p_buf, MG_BUFSIZE, 0);

   MG_MEMCHECK("Insufficient memory to process response", 0);

   mg_db_disconnect(p_page->p_srv, chndle, 1);

   if ((n = mg_get_error(p_page->p_srv, p_buf->p_buffer))) {
      MG_ERROR(p_buf->p_buffer + MG_RECV_HEAD);
      return mg_r_nil;
   }

   return rb_str_new(p_buf->p_buffer + MG_RECV_HEAD, p_buf->data_size - MG_RECV_HEAD);
}


static VALUE ex_ma_function(VALUE self, VALUE r_fun, VALUE a_list, VALUE r_argn)
{
   MGBUF mgbuf, *p_buf;
   int n, max, an, t, argn, chndle, phndle, len, anybyref, ret_size;
   int types[32];
   int byrefs[32];
   char ifc[4];
   char bstr[256], bstr1[256];
   MGSTR properties[MG_MAX_KEY];
   char *str;
   char *fun;
   char *ret;
   VALUE a;
   VALUE pstr;
   VALUE p;
   MGPAGE *p_page;


   if (mg_type(a_list) != MG_T_LIST) {
      MG_ERROR("mg_ruby: Argument 3 to 'ma_function' must be an array");
      return mg_r_nil;
   }

   phndle = 0;
   p_page = mg_ppage(phndle);

   p_buf = &mgbuf;
   mg_buf_init(p_buf, MG_BUFSIZE, MG_BUFSIZE);

   MG_FTRACE("ma_function");

   fun = mg_get_string(r_fun, &p, &len);
   argn = mg_get_integer(r_argn);

   pstr = rb_ary_entry(a_list, 0);
   str = NULL;
   strcpy(bstr, "");
   strcpy(bstr1, "");
   if (pstr)
      str = mg_get_string(pstr, &p, &len);
   if (str) {
      strcpy(bstr, str);
   }

   max = mg_extract_substrings(properties, bstr, (int) strlen(bstr), '#', 0, 0, MG_ES_DELIM);

   anybyref = 0;
   for (an = 1; an < 32; an ++) {
      if (an < max) {
         if (properties[an].ps[0] == '1')
            types[an] = 1;
         else
            types[an] = 0;
         if (properties[an].ps[1] == '1') {
            byrefs[an] = 1;
            anybyref = 1;
         }
         else
            byrefs[an] = 0;
      }
      else {
         types[an] = 0;
         byrefs[an] = 0;
      }
   }

   chndle = 0;

   max = 0;

   n = mg_db_connect(p_page->p_srv, &chndle, 1);
   if (!n) {
      MG_ERROR(p_page->p_srv->error_mess);
      return mg_r_nil;
   }

   mg_request_header(p_page->p_srv, p_buf, "X", MG_PRODUCT);

   ifc[0] = 0;
   ifc[1] = MG_TX_DATA;
   mg_request_add(p_page->p_srv, chndle, p_buf, (unsigned char *) fun, (int) strlen((char *) fun), (short) ifc[0], (short) ifc[1]);

   for (an = 1; an <= argn; an ++) {

      str = NULL;
      pstr = rb_ary_entry(a_list, an);
      if (pstr) {

         if (byrefs[an])
            ifc[0] = 1;
         else
            ifc[0] = 0;

         t = mg_type(pstr);
         if (t == MG_T_LIST) {

            int max, n;

            ifc[1] = MG_TX_AREC;
            mg_request_add(p_page->p_srv, chndle, p_buf, NULL, 0, (short) ifc[0], (short) ifc[1]);

            ifc[1] = MG_TX_AREC_FORMATTED;

            max = mg_get_array_size(pstr);
            for (n = 0; n < max; n ++) {
               a = rb_ary_entry(pstr, n);
               str = mg_get_string(a, &p, &len);
               mg_request_add(p_page->p_srv, chndle, p_buf, (unsigned char *) str, len, (short) ifc[0], (short) ifc[1]);
            }

            ifc[1] = MG_TX_EOD;
            mg_request_add(p_page->p_srv, chndle, p_buf, NULL, 0, (short) ifc[0], (short) ifc[1]);

         }
         else {
            str = mg_get_string(pstr, &p, &n);

            ifc[1] = MG_TX_DATA;

            mg_request_add(p_page->p_srv, chndle, p_buf, (unsigned char *) str, (int) strlen((char *) str), (short) ifc[0], (short) ifc[1]);
         }
      }
   }


   mg_db_send(p_page->p_srv, chndle, p_buf, 1);
   mg_db_receive(p_page->p_srv, chndle, p_buf, MG_BUFSIZE, 0);

   MG_MEMCHECK("Insufficient memory to process response", 0);

   mg_db_disconnect(p_page->p_srv, chndle, 1);

   if ((n = mg_get_error(p_page->p_srv, p_buf->p_buffer))) {
      MG_ERROR(p_buf->p_buffer + MG_RECV_HEAD);
      return mg_r_nil;
   }

   if (anybyref) {
      short byref, type, stop;
      int n, n1, rn, hlen, size, clen, rlen, argc, rec_len, argoffs;
      unsigned char *parg, *par;

      ret = p_buf->p_buffer + MG_RECV_HEAD;
      ret_size = p_buf->data_size - MG_RECV_HEAD;

      argoffs = 2;
      stop = 0;
      parg = (p_buf->p_buffer + MG_RECV_HEAD);

      clen = mg_decode_size(p_buf->p_buffer, 5, MG_CHUNK_SIZE_BASE);

      rlen = 0;
      argc = 0;
      for (n = 0;; n ++) {
         hlen = mg_decode_item_header(parg, &size, (short *) &byref, (short *) &type);
         if ((hlen + size + rlen) > clen) {
            stop = 1;
            break;
         }
         parg += hlen;
         rlen += hlen;
         an = argc - argoffs;

         if (an > 0) {
            pstr = rb_ary_entry(a_list, an);
            if (pstr)
               t = mg_type(pstr);
         }
         else
            pstr = mg_r_nil;
/*
{
   unsigned char c;
   char buffer[256];
   c = *(parg + size);
   *(parg + size) = '\0';
   sprintf(buffer, "RESULT %d: argc=%d; hlen=%d; size=%d; byref=%d; type=%d; rlen=%d; clen=%d", n, argc, hlen, size, byref, type, rlen, clen);
   mg_log_event(parg, buffer);
   *(parg + size) = c;
}
*/
         if (argc == 0) {
               ret = parg;
               ret_size = size;
         }
         else if (pstr && type == MG_TX_DATA) {
            p = rb_str_new(parg, size);
            mg_set_list_item(a_list, an, p);
         }

         parg += size;
         rlen += size;
         if (type == MG_TX_AREC) {

            if (pstr && t == MG_T_LIST)
               mg_kill_list(pstr);

            par = parg;
            rn = 0;
            rec_len = 0;
            for (n1 = 0;; n1 ++) {
               hlen = mg_decode_item_header(parg, &size, (short *) &byref, (short *) &type);
               if ((hlen + size + rlen) > clen) {
                  stop = 1;
                  break;
               }
               if (type == MG_TX_EOD) {
                  parg += (hlen + size);
                  rlen += (hlen + size);
                  break;
               }
               parg += hlen;
               rlen += hlen;
               rec_len += hlen;
/*
{
   unsigned char c;
   char buffer[256];
   c = *(parg + size);
   *(parg + size) = '\0';
   sprintf(buffer, "RESULT ARRAY %d: argc=%d; hlen=%d; size=%d; byref=%d; type=%d; rlen=%d; clen=%d", n1, argc, hlen, size, byref, type, rlen, clen);
   mg_log_event(parg, buffer);
   *(parg + size) = c;
}
*/
               parg += size;
               rlen += size;
               rec_len += size;
               if (type == MG_TX_DATA) {
/*
{
   unsigned char c;
   char buffer[256];
   c = *(parg + size);
   *(parg + size) = '\0';
   sprintf(buffer, "RESULT ARRAY RECORD %d: argc=%d; hlen=%d; size=%d; byref=%d; type=%d; rlen=%d; clen=%d; rec_len=%d", n1, argc, hlen, size, byref, type, rlen, clen, rec_len);
   mg_log_event(par, buffer);
   *(parg + size) = c;
}
*/
                  if (pstr && t == MG_T_LIST) {
                     p = rb_str_new(par, rec_len);
                     mg_set_list_item(pstr, rn ++, p);
                  }

                  par = parg;
                  rec_len = 0;
               }
            }
         }
         if (rlen >= clen || stop)
            break;
         argc ++;
      }
      if (stop) {
         MG_ERROR("ma_function: Bad return data");
         return mg_r_nil;
      }
      return rb_str_new(ret, ret_size);
   }


   return rb_str_new(p_buf->p_buffer + MG_RECV_HEAD, p_buf->data_size - MG_RECV_HEAD);
}


static VALUE ex_m_classmethod(int argc, VALUE *argv, VALUE self)
{
   MGBUF mgbuf, *p_buf;
   int n, max;
   char ifc[4];
   int chndle, phndle;
   MGPAGE *p_page;
   MGVARGS vargs;

   if ((max = mg_get_vargs(argc, argv, &vargs, 0)) == -1)
      return mg_r_nil;

   phndle = 0;
   p_page = mg_ppage(phndle);

   p_buf = &mgbuf;
   mg_buf_init(p_buf, MG_BUFSIZE, MG_BUFSIZE);

   MG_FTRACE("m_classmethod");

   n = mg_db_connect(p_page->p_srv, &chndle, 1);

   if (!n) {
      MG_ERROR(p_page->p_srv->error_mess);
      return mg_r_nil;
   }

   mg_request_header(p_page->p_srv, p_buf, "x", MG_PRODUCT);

   ifc[0] = 0;
   ifc[1] = MG_TX_DATA;
   mg_request_add(p_page->p_srv, chndle, p_buf, (unsigned char *) vargs.global, (int) vargs.global_len, (short) ifc[0], (short) ifc[1]);

   for (n = 1; n < max; n ++) {
      ifc[0] = 0;
      ifc[1] = MG_TX_DATA;
      mg_request_add(p_page->p_srv, chndle, p_buf, vargs.cvars[n].ps, vargs.cvars[n].size, (short) ifc[0], (short) ifc[1]);
   }

   MG_MEMCHECK("Insufficient memory to process request", 1);

   mg_db_send(p_page->p_srv, chndle, p_buf, 1);

   mg_db_receive(p_page->p_srv, chndle, p_buf, MG_BUFSIZE, 0);

   MG_MEMCHECK("Insufficient memory to process response", 0);

   mg_db_disconnect(p_page->p_srv, chndle, 1);

   if ((n = mg_get_error(p_page->p_srv, p_buf->p_buffer))) {
      MG_ERROR(p_buf->p_buffer + MG_RECV_HEAD);
      return mg_r_nil;
   }

   if (!strncmp((char *) p_buf->p_buffer + 5, "co", 2)) { /* v2.3.43 */
      int oref;
      VALUE mclass;

      p_buf->p_buffer[p_buf->data_size] = '\0';
      oref = (int) strtol(p_buf->p_buffer + MG_RECV_HEAD, NULL, 10);

      mclass = rb_funcall(mg_mclass, rb_intern("new"), 2, rb_int_new(oref), rb_int_new(phndle));

      return mclass;
   }

   return rb_str_new(p_buf->p_buffer + MG_RECV_HEAD, p_buf->data_size - MG_RECV_HEAD);
}


/* v2.3.43 */
void mclass_free(void *data)
{
   mg_free(data, 0);
}


size_t mclass_size(const void *data)
{
   return sizeof(MGMCLASS);
}


VALUE mclass_alloc(VALUE self)
{
   MGMCLASS *pmclass;

   /* allocate */
   pmclass = (MGMCLASS *) mg_malloc(sizeof(MGMCLASS), 0);

   /* wrap */
   return TypedData_Wrap_Struct(self, &mclass_type, pmclass);
}


VALUE mclass_m_initialize(VALUE self, VALUE rb_oref, VALUE rb_phndle)
{
   MGMCLASS *pmclass;
   /* unwrap */

   TypedData_Get_Struct(self, MGMCLASS, &mclass_type, pmclass);

   pmclass->oref = NUM2INT(rb_oref);
   pmclass->phndle = NUM2INT(rb_phndle);

   return self;
}


static VALUE ex_m_mclass()
{
   VALUE cmclass;

   cmclass = rb_define_class("MCLASS", rb_cObject);
/*
   rb_define_method(mg_ruby, "initialize", t_init, 0);
   rb_define_method(mg_ruby, "add", t_add, 1);
*/
   rb_define_alloc_func(cmclass, mclass_alloc);

   rb_define_method(cmclass, "initialize", mclass_m_initialize, 2);
   rb_define_method(cmclass, "method", ex_mclass_method, -1);
   rb_define_method(cmclass, "getproperty", ex_mclass_getproperty, 1);
   rb_define_method(cmclass, "setproperty", ex_mclass_setproperty, 2);

   rb_define_method(cmclass, "close", ex_mclass_close, 0);
/*
   rb_define_attr(cmclass, "myvar", 1, 1);
*/

   return cmclass;
}


/* v2.3.43 */
static VALUE ex_mclass_method(int argc, VALUE *argv, VALUE self)
{
   MGBUF mgbuf, *p_buf;
   int n, max;
   char ifc[4];
   char buffer[32];
   int chndle, phndle;
   MGPAGE *p_page;
   MGVARGS vargs;
   MGMCLASS *pmclass;
   /* unwrap */

   TypedData_Get_Struct(self, MGMCLASS, &mclass_type, pmclass);

   if ((max = mg_get_vargs(argc, argv, &vargs, 0)) == -1)
      return mg_r_nil;

   phndle = pmclass->phndle;
   p_page = mg_ppage(phndle);

   p_buf = &mgbuf;
   mg_buf_init(p_buf, MG_BUFSIZE, MG_BUFSIZE);

   MG_FTRACE("mclass_method");

   n = mg_db_connect(p_page->p_srv, &chndle, 1);

   if (!n) {
      MG_ERROR(p_page->p_srv->error_mess);
      return mg_r_nil;
   }

   mg_request_header(p_page->p_srv, p_buf, "j", MG_PRODUCT);

   sprintf(buffer, "%d", pmclass->oref);
   ifc[0] = 0;
   ifc[1] = MG_TX_DATA;
   mg_request_add(p_page->p_srv, chndle, p_buf, (unsigned char *) buffer, (int) strlen((char *) buffer), (short) ifc[0], (short) ifc[1]);

   for (n = 0; n < max; n ++) {
      ifc[0] = 0;
      ifc[1] = MG_TX_DATA;
      mg_request_add(p_page->p_srv, chndle, p_buf, vargs.cvars[n].ps, vargs.cvars[n].size, (short) ifc[0], (short) ifc[1]);
   }

   MG_MEMCHECK("Insufficient memory to process request", 1);

   mg_db_send(p_page->p_srv, chndle, p_buf, 1);

   mg_db_receive(p_page->p_srv, chndle, p_buf, MG_BUFSIZE, 0);

   MG_MEMCHECK("Insufficient memory to process response", 0);

   mg_db_disconnect(p_page->p_srv, chndle, 1);

   if ((n = mg_get_error(p_page->p_srv, p_buf->p_buffer))) {
      MG_ERROR(p_buf->p_buffer + MG_RECV_HEAD);
      return mg_r_nil;
   }

   if (!strncmp((char *) p_buf->p_buffer + 5, "co", 2)) { /* v2.3.43 */
      int oref;
      VALUE mclass;

      p_buf->p_buffer[p_buf->data_size] = '\0';
      oref = (int) strtol(p_buf->p_buffer + MG_RECV_HEAD, NULL, 10);

      mclass = rb_funcall(mg_mclass, rb_intern("new"), 2, rb_int_new(oref), rb_int_new(phndle));

      return mclass;
   }

   return rb_str_new(p_buf->p_buffer + MG_RECV_HEAD, p_buf->data_size - MG_RECV_HEAD);
}


static VALUE ex_mclass_getproperty(VALUE self, VALUE r_pname)
{
   MGBUF mgbuf, *p_buf;
   int n, len;
   char ifc[4];
   char buffer[32];
   int chndle, phndle;
   MGPAGE *p_page;
   MGMCLASS *pmclass;
   char *cpname;
   VALUE rpname;

   /* unwrap */

   TypedData_Get_Struct(self, MGMCLASS, &mclass_type, pmclass);

   phndle = pmclass->phndle;
   p_page = mg_ppage(phndle);

   p_buf = &mgbuf;
   mg_buf_init(p_buf, MG_BUFSIZE, MG_BUFSIZE);

   MG_FTRACE("mclass_getproperty");

   n = mg_db_connect(p_page->p_srv, &chndle, 1);

   if (!n) {
      MG_ERROR(p_page->p_srv->error_mess);
      return mg_r_nil;
   }

   mg_request_header(p_page->p_srv, p_buf, "h", MG_PRODUCT);

   sprintf(buffer, "%d", pmclass->oref);
   ifc[0] = 0;
   ifc[1] = MG_TX_DATA;
   mg_request_add(p_page->p_srv, chndle, p_buf, (unsigned char *) buffer, (int) strlen((char *) buffer), (short) ifc[0], (short) ifc[1]);

   cpname = mg_get_string(r_pname, &rpname, &len);
   ifc[0] = 0;
   ifc[1] = MG_TX_DATA;
   mg_request_add(p_page->p_srv, chndle, p_buf, (unsigned char *) cpname, (int) len, (short) ifc[0], (short) ifc[1]);

   MG_MEMCHECK("Insufficient memory to process request", 1);

   mg_db_send(p_page->p_srv, chndle, p_buf, 1);

   mg_db_receive(p_page->p_srv, chndle, p_buf, MG_BUFSIZE, 0);

   MG_MEMCHECK("Insufficient memory to process response", 0);

   mg_db_disconnect(p_page->p_srv, chndle, 1);

   if ((n = mg_get_error(p_page->p_srv, p_buf->p_buffer))) {
      MG_ERROR(p_buf->p_buffer + MG_RECV_HEAD);
      return mg_r_nil;
   }

   if (!strncmp((char *) p_buf->p_buffer + 5, "co", 2)) { /* v2.3.43 */
      int oref;
      VALUE mclass;

      p_buf->p_buffer[p_buf->data_size] = '\0';
      oref = (int) strtol(p_buf->p_buffer + MG_RECV_HEAD, NULL, 10);

      mclass = rb_funcall(mg_mclass, rb_intern("new"), 2, rb_int_new(oref), rb_int_new(phndle));

      return mclass;
   }

   return rb_str_new(p_buf->p_buffer + MG_RECV_HEAD, p_buf->data_size - MG_RECV_HEAD);
}


static VALUE ex_mclass_setproperty(VALUE self, VALUE r_pname, VALUE r_pvalue)
{
   MGBUF mgbuf, *p_buf;
   int n, len;
   char ifc[4];
   char buffer[32];
   int chndle, phndle;
   MGPAGE *p_page;
   MGMCLASS *pmclass;
   char *cpname, *cpvalue;
   VALUE rpname, rpvalue;

   /* unwrap */

   TypedData_Get_Struct(self, MGMCLASS, &mclass_type, pmclass);

   phndle = pmclass->phndle;
   p_page = mg_ppage(phndle);

   p_buf = &mgbuf;
   mg_buf_init(p_buf, MG_BUFSIZE, MG_BUFSIZE);

   MG_FTRACE("mclass_getproperty");

   n = mg_db_connect(p_page->p_srv, &chndle, 1);

   if (!n) {
      MG_ERROR(p_page->p_srv->error_mess);
      return mg_r_nil;
   }

   mg_request_header(p_page->p_srv, p_buf, "i", MG_PRODUCT);

   sprintf(buffer, "%d", pmclass->oref);
   ifc[0] = 0;
   ifc[1] = MG_TX_DATA;
   mg_request_add(p_page->p_srv, chndle, p_buf, (unsigned char *) buffer, (int) strlen((char *) buffer), (short) ifc[0], (short) ifc[1]);

   cpname = mg_get_string(r_pname, &rpname, &len);
   ifc[0] = 0;
   ifc[1] = MG_TX_DATA;
   mg_request_add(p_page->p_srv, chndle, p_buf, (unsigned char *) cpname, (int) len, (short) ifc[0], (short) ifc[1]);

   cpvalue = mg_get_string(r_pvalue, &rpvalue, &len);
   ifc[0] = 0;
   ifc[1] = MG_TX_DATA;
   mg_request_add(p_page->p_srv, chndle, p_buf, (unsigned char *) cpvalue, (int) len, (short) ifc[0], (short) ifc[1]);

   MG_MEMCHECK("Insufficient memory to process request", 1);

   mg_db_send(p_page->p_srv, chndle, p_buf, 1);

   mg_db_receive(p_page->p_srv, chndle, p_buf, MG_BUFSIZE, 0);

   MG_MEMCHECK("Insufficient memory to process response", 0);

   mg_db_disconnect(p_page->p_srv, chndle, 1);

   if ((n = mg_get_error(p_page->p_srv, p_buf->p_buffer))) {
      MG_ERROR(p_buf->p_buffer + MG_RECV_HEAD);
      return mg_r_nil;
   }

   if (!strncmp((char *) p_buf->p_buffer + 5, "co", 2)) { /* v2.3.43 */
      int oref;
      VALUE mclass;

      p_buf->p_buffer[p_buf->data_size] = '\0';
      oref = (int) strtol(p_buf->p_buffer + MG_RECV_HEAD, NULL, 10);

      mclass = rb_funcall(mg_mclass, rb_intern("new"), 2, rb_int_new(oref), rb_int_new(phndle));

      return mclass;
   }

   return rb_str_new(p_buf->p_buffer + MG_RECV_HEAD, p_buf->data_size - MG_RECV_HEAD);
}


static VALUE ex_mclass_close(VALUE self)
{
   return rb_str_new("", 0);
}


static VALUE ex_ma_classmethod(VALUE self, VALUE r_cclass, VALUE r_cmethod, VALUE a_list, VALUE r_argn)
{
   MGBUF mgbuf, *p_buf;
   int n, max, an, t, argn, chndle, phndle, len, anybyref, ret_size;
   int types[32];
   int byrefs[32];
   char ifc[4];
   char bstr[256], bstr1[256];
   MGSTR properties[MG_MAX_KEY];
   char *str;
   char *cclass;
   char *cmethod;
   char *ret;
   VALUE a;
   VALUE pstr;
   VALUE p;
   MGPAGE *p_page;


   if (mg_type(a_list) != MG_T_LIST) {
      MG_ERROR("mg_ruby: Argument 3 to 'ma_classmethod' must be an array");
      return mg_r_nil;
   }

   phndle = 0;
   p_page = mg_ppage(phndle);

   p_buf = &mgbuf;
   mg_buf_init(p_buf, MG_BUFSIZE, MG_BUFSIZE);

   MG_FTRACE("ma_classmethod");

   cclass = mg_get_string(r_cclass, &p, &len);
   cmethod = mg_get_string(r_cmethod, &p, &len);
   argn = mg_get_integer(r_argn);

   pstr = rb_ary_entry(a_list, 0);
   str = NULL;
   strcpy(bstr, "");
   strcpy(bstr1, "");
   if (pstr)
      str = mg_get_string(pstr, &p, &len);
   if (str) {
      strcpy(bstr, str);
   }

   max = mg_extract_substrings(properties, bstr, (int) strlen(bstr), '#', 0, 0, MG_ES_DELIM);

   anybyref = 0;
   for (an = 1; an < 32; an ++) {
      if (an < max) {
         if (properties[an].ps[0] == '1')
            types[an] = 1;
         else
            types[an] = 0;
         if (properties[an].ps[1] == '1') {
            byrefs[an] = 1;
            anybyref = 1;
         }
         else
            byrefs[an] = 0;
      }
      else {
         types[an] = 0;
         byrefs[an] = 0;
      }
   }

   chndle = 0;

   max = 0;

   n = mg_db_connect(p_page->p_srv, &chndle, 1);
   if (!n) {
      MG_ERROR(p_page->p_srv->error_mess);
      return mg_r_nil;
   }

   mg_request_header(p_page->p_srv, p_buf, "x", MG_PRODUCT);

   ifc[0] = 0;
   ifc[1] = MG_TX_DATA;
   mg_request_add(p_page->p_srv, chndle, p_buf, (unsigned char *) cclass, (int) strlen((char *) cclass), (short) ifc[0], (short) ifc[1]);

   ifc[0] = 0;
   ifc[1] = MG_TX_DATA;
   mg_request_add(p_page->p_srv, chndle, p_buf, (unsigned char *) cmethod, (int) strlen((char *) cmethod), (short) ifc[0], (short) ifc[1]);

   for (an = 1; an <= argn; an ++) {

      str = NULL;
      pstr = rb_ary_entry(a_list, an);
      if (pstr) {

         if (byrefs[an])
            ifc[0] = 1;
         else
            ifc[0] = 0;

         t = mg_type(pstr);
         if (t == MG_T_LIST) {

            int max, n;

            ifc[1] = MG_TX_AREC;
            mg_request_add(p_page->p_srv, chndle, p_buf, NULL, 0, (short) ifc[0], (short) ifc[1]);

            ifc[1] = MG_TX_AREC_FORMATTED;

            max = mg_get_array_size(pstr);
            for (n = 0; n < max; n ++) {
               a = rb_ary_entry(pstr, n);
               str = mg_get_string(a, &p, &len);
               mg_request_add(p_page->p_srv, chndle, p_buf, (unsigned char *) str, len, (short) ifc[0], (short) ifc[1]);
            }

            ifc[1] = MG_TX_EOD;
            mg_request_add(p_page->p_srv, chndle, p_buf, NULL, 0, (short) ifc[0], (short) ifc[1]);

         }
         else {
            str = mg_get_string(pstr, &p, &n);

            ifc[1] = MG_TX_DATA;

            mg_request_add(p_page->p_srv, chndle, p_buf, (unsigned char *) str, (int) strlen((char *) str), (short) ifc[0], (short) ifc[1]);
         }
      }
   }

   mg_db_send(p_page->p_srv, chndle, p_buf, 1);
   mg_db_receive(p_page->p_srv, chndle, p_buf, MG_BUFSIZE, 0);

   MG_MEMCHECK("Insufficient memory to process response", 0);

   mg_db_disconnect(p_page->p_srv, chndle, 1);

   if ((n = mg_get_error(p_page->p_srv, p_buf->p_buffer))) {
      MG_ERROR(p_buf->p_buffer + MG_RECV_HEAD);
      return mg_r_nil;
   }


   if (anybyref) {
      short byref, type, stop;
      int n, n1, rn, hlen, size, clen, rlen, argc, rec_len, argoffs;
      unsigned char *parg, *par;

      ret = p_buf->p_buffer + MG_RECV_HEAD;
      ret_size = p_buf->data_size - MG_RECV_HEAD;

      argoffs = 3;
      stop = 0;
      parg = (p_buf->p_buffer + MG_RECV_HEAD);

      clen = mg_decode_size(p_buf->p_buffer, 5, MG_CHUNK_SIZE_BASE);

      rlen = 0;
      argc = 0;
      for (n = 0;; n ++) {
         hlen = mg_decode_item_header(parg, &size, (short *) &byref, (short *) &type);
         if ((hlen + size + rlen) > clen) {
            stop = 1;
            break;
         }
         parg += hlen;
         rlen += hlen;
         an = argc - argoffs;

         if (an > 0) {
            pstr = rb_ary_entry(a_list, an);
            if (pstr)
               t = mg_type(pstr);
         }
         else
            pstr = mg_r_nil;
/*
{
   unsigned char c;
   char buffer[256];
   c = *(parg + size);
   *(parg + size) = '\0';
   sprintf(buffer, "RESULT %d: argc=%d; hlen=%d; size=%d; byref=%d; type=%d; rlen=%d; clen=%d", n, argc, hlen, size, byref, type, rlen, clen);
   mg_log_event(parg, buffer);
   *(parg + size) = c;
}
*/
         if (argc == 0) {
               ret = parg;
               ret_size = size;
         }
         else if (pstr && type == MG_TX_DATA) {
            p = rb_str_new(parg, size);
            mg_set_list_item(a_list, an, p);
         }

         parg += size;
         rlen += size;
         if (type == MG_TX_AREC) {

            if (pstr && t == MG_T_LIST)
               mg_kill_list(pstr);

            par = parg;
            rn = 0;
            rec_len = 0;
            for (n1 = 0;; n1 ++) {
               hlen = mg_decode_item_header(parg, &size, (short *) &byref, (short *) &type);
               if ((hlen + size + rlen) > clen) {
                  stop = 1;
                  break;
               }
               if (type == MG_TX_EOD) {
                  parg += (hlen + size);
                  rlen += (hlen + size);
                  break;
               }
               parg += hlen;
               rlen += hlen;
               rec_len += hlen;
/*
{
   unsigned char c;
   char buffer[256];
   c = *(parg + size);
   *(parg + size) = '\0';
   sprintf(buffer, "RESULT ARRAY %d: argc=%d; hlen=%d; size=%d; byref=%d; type=%d; rlen=%d; clen=%d", n1, argc, hlen, size, byref, type, rlen, clen);
   mg_log_event(parg, buffer);
   *(parg + size) = c;
}
*/
               parg += size;
               rlen += size;
               rec_len += size;
               if (type == MG_TX_DATA) {
/*
{
   unsigned char c;
   char buffer[256];
   c = *(parg + size);
   *(parg + size) = '\0';
   sprintf(buffer, "RESULT ARRAY RECORD %d: argc=%d; hlen=%d; size=%d; byref=%d; type=%d; rlen=%d; clen=%d; rec_len=%d", n1, argc, hlen, size, byref, type, rlen, clen, rec_len);
   mg_log_event(par, buffer);
   *(parg + size) = c;
}
*/
                  if (pstr && t == MG_T_LIST) {
                     p = rb_str_new(par, rec_len);
                     mg_set_list_item(pstr, rn ++, p);
                  }

                  par = parg;
                  rec_len = 0;
               }
            }
         }
         if (rlen >= clen || stop)
            break;
         argc ++;
      }
      if (stop) {
         MG_ERROR("ma_classmethod: Bad return data");
         return mg_r_nil;
      }
      return rb_str_new(ret, ret_size);
   }

   return rb_str_new(p_buf->p_buffer + MG_RECV_HEAD, p_buf->data_size - MG_RECV_HEAD);
}


static VALUE ex_ma_html_ex(VALUE self, VALUE r_fun, VALUE a_list, VALUE r_argn)
{
   MGBUF mgbuf, *p_buf;
   int n, max, an, t, argn, chndle, phndle, len;
   int types[32];
   int byrefs[32];
   char ifc[4];
   char bstr[256], bstr1[256];
   MGSTR properties[MG_MAX_KEY];
   char *str;
   char *fun;
   VALUE a;
   VALUE pstr;
   VALUE p;
   MGPAGE *p_page;


   if (mg_type(a_list) != MG_T_LIST) {
      MG_ERROR("mg_ruby: Argument 2 to 'ma_html_ex' must be an array");
      return mg_r_nil;
   }

   phndle = 0;
   p_page = mg_ppage(phndle);

   p_buf = &mgbuf;
   mg_buf_init(p_buf, MG_BUFSIZE, MG_BUFSIZE);

   MG_FTRACE("ma_html_ex");

   fun = mg_get_string(r_fun, &p, &len);
   argn = mg_get_integer(r_argn);

   pstr = rb_ary_entry(a_list, 0);
   str = NULL;
   strcpy(bstr, "");
   strcpy(bstr1, "");
   if (pstr)
      str = mg_get_string(pstr, &p, &len);
   if (str) {
      strcpy(bstr, str);
   }

   max = mg_extract_substrings(properties, bstr, (int) strlen(bstr), '#', 0, 0, MG_ES_DELIM);

   for (an = 1; an < 32; an ++) {
      if (an < max) {
         if (properties[an].ps[0] == '1')
            types[an] = 1;
         else
            types[an] = 0;
         if (properties[an].ps[1] == '1')
            byrefs[an] = 1;
         else
            byrefs[an] = 0;
      }
      else {
         types[an] = 0;
         byrefs[an] = 0;
      }
   }

   chndle = 0;

   max = 0;

   n = mg_db_connect(p_page->p_srv, &chndle, 1);
   if (!n) {
      MG_ERROR(p_page->p_srv->error_mess);
      return mg_r_nil;
   }

   mg_request_header(p_page->p_srv, p_buf, "H", MG_PRODUCT);

   ifc[0] = 0;
   ifc[1] = MG_TX_DATA;
   mg_request_add(p_page->p_srv, chndle, p_buf, (unsigned char *) fun, (int) strlen((char *) fun), (short) ifc[0], (short) ifc[1]);

   for (an = 1; an <= argn; an ++) {

      str = NULL;
      pstr = rb_ary_entry(a_list, an);
      if (pstr) {

         if (byrefs[an])
            ifc[0] = 1;
         else
            ifc[0] = 0;

         t = mg_type(pstr);
         if (t == MG_T_LIST) {

            int max, n;

            ifc[1] = MG_TX_AREC;
            mg_request_add(p_page->p_srv, chndle, p_buf, NULL, 0, (short) ifc[0], (short) ifc[1]);

            ifc[1] = MG_TX_AREC_FORMATTED;

            max = mg_get_array_size(pstr);
            for (n = 0; n < max; n ++) {
               a = rb_ary_entry(pstr, n);
               str = mg_get_string(a, &p, &len);
               mg_request_add(p_page->p_srv, chndle, p_buf, (unsigned char *) str, len, (short) ifc[0], (short) ifc[1]);
            }

            ifc[1] = MG_TX_EOD;
            mg_request_add(p_page->p_srv, chndle, p_buf, NULL, 0, (short) ifc[0], (short) ifc[1]);

         }
         else {
            str = mg_get_string(pstr, &p, &n);

            ifc[1] = MG_TX_DATA;

            mg_request_add(p_page->p_srv, chndle, p_buf, (unsigned char *) str, (int) strlen((char *) str), (short) ifc[0], (short) ifc[1]);
         }
      }
   }

   mg_db_send(p_page->p_srv, chndle, p_buf, 1);

   return rb_int2inum((long) chndle);
}


static VALUE ex_ma_html_classmethod_ex(VALUE self, VALUE r_cclass, VALUE r_cmethod, VALUE a_list, VALUE r_argn)
{
   MGBUF mgbuf, *p_buf;
   int n, max, an, t, argn, chndle, phndle, len;
   int types[32];
   int byrefs[32];
   char ifc[4];
   char bstr[256], bstr1[256];
   MGSTR properties[MG_MAX_KEY];
   char *str;
   char *cclass;
   char *cmethod;
   VALUE a;
   VALUE pstr;
   VALUE p;
   MGPAGE *p_page;


   if (mg_type(a_list) != MG_T_LIST) {
      MG_ERROR("mg_ruby: Argument 3 to 'ma_html_classmethod_ex' must be an array");
      return mg_r_nil;
   }

   phndle = 0;
   p_page = mg_ppage(phndle);

   p_buf = &mgbuf;
   mg_buf_init(p_buf, MG_BUFSIZE, MG_BUFSIZE);

   MG_FTRACE("ma_html_classmethod_ex");

   cclass = mg_get_string(r_cclass, &p, &len);
   cmethod = mg_get_string(r_cmethod, &p, &len);
   argn = mg_get_integer(r_argn);

   pstr = rb_ary_entry(a_list, 0);
   str = NULL;
   strcpy(bstr, "");
   strcpy(bstr1, "");
   if (pstr)
      str = mg_get_string(pstr, &p, &len);
   if (str) {
      strcpy(bstr, str);
   }

   max = mg_extract_substrings(properties, bstr, (int) strlen(bstr), '#', 0, 0, MG_ES_DELIM);

   for (an = 1; an < 32; an ++) {
      if (an < max) {
         if (properties[an].ps[0] == '1')
            types[an] = 1;
         else
            types[an] = 0;
         if (properties[an].ps[1] == '1')
            byrefs[an] = 1;
         else
            byrefs[an] = 0;
      }
      else {
         types[an] = 0;
         byrefs[an] = 0;
      }
   }

   chndle = 0;

   max = 0;

   n = mg_db_connect(p_page->p_srv, &chndle, 1);
   if (!n) {
      MG_ERROR(p_page->p_srv->error_mess);
      return mg_r_nil;
   }

   mg_request_header(p_page->p_srv, p_buf, "y", MG_PRODUCT);

   ifc[0] = 0;
   ifc[1] = MG_TX_DATA;
   mg_request_add(p_page->p_srv, chndle, p_buf, (unsigned char *) cclass, (int) strlen((char *) cclass), (short) ifc[0], (short) ifc[1]);

   ifc[0] = 0;
   ifc[1] = MG_TX_DATA;
   mg_request_add(p_page->p_srv, chndle, p_buf, (unsigned char *) cmethod, (int) strlen((char *) cmethod), (short) ifc[0], (short) ifc[1]);

   for (an = 1; an <= argn; an ++) {

      str = NULL;
      pstr = rb_ary_entry(a_list, an);
      if (pstr) {

         if (byrefs[an])
            ifc[0] = 1;
         else
            ifc[0] = 0;

         t = mg_type(pstr);
         if (t == MG_T_LIST) {

            int max, n;

            ifc[1] = MG_TX_AREC;
            mg_request_add(p_page->p_srv, chndle, p_buf, NULL, 0, (short) ifc[0], (short) ifc[1]);

            ifc[1] = MG_TX_AREC_FORMATTED;

            max = (int) (pstr);
            for (n = 0; n < max; n ++) {
               a = rb_ary_entry(pstr, n);
               str = mg_get_string(a, &p, &len);
               mg_request_add(p_page->p_srv, chndle, p_buf, (unsigned char *) str, len, (short) ifc[0], (short) ifc[1]);
            }

            ifc[1] = MG_TX_EOD;
            mg_request_add(p_page->p_srv, chndle, p_buf, NULL, 0, (short) ifc[0], (short) ifc[1]);

         }
         else {
            str = mg_get_string(pstr, &p, &n);

            ifc[1] = MG_TX_DATA;

            mg_request_add(p_page->p_srv, chndle, p_buf, (unsigned char *) str, (int) strlen((char *) str), (short) ifc[0], (short) ifc[1]);
         }
      }
   }

   mg_db_send(p_page->p_srv, chndle, p_buf, 1);

   return rb_int2inum((long) chndle);
}


static VALUE ex_ma_http_ex(VALUE self, VALUE r_cgi, VALUE r_content)
{
   MGBUF mgbuf, *p_buf;
   int n, max, chndle, phndle, len;
   char ifc[4];
   char *str;
   VALUE a;
   VALUE p;
   MGPAGE *p_page;


   if (mg_type(r_cgi) != MG_T_LIST) {
      MG_ERROR("mg_ruby: Argument 1 to 'ma_http_ex' must be an array");
      return mg_r_nil;
   }

   phndle = 0;
   p_page = mg_ppage(phndle);

   p_buf = &mgbuf;
   mg_buf_init(p_buf, MG_BUFSIZE, MG_BUFSIZE);

   MG_FTRACE("ma_http_ex");

   chndle = 0;

   max = 0;

   n = mg_db_connect(p_page->p_srv, &chndle, 1);
   if (!n) {
      MG_ERROR(p_page->p_srv->error_mess);
      return mg_r_nil;
   }

   mg_request_header(p_page->p_srv, p_buf, "h", MG_PRODUCT);

   ifc[0] = 0;
   ifc[1] = MG_TX_AREC;
   mg_request_add(p_page->p_srv, chndle, p_buf, NULL, 0, (short) ifc[0], (short) ifc[1]);

   ifc[1] = MG_TX_AREC_FORMATTED;

   max = mg_get_array_size(r_cgi);

   for (n = 0; n < max; n ++) {
      a = rb_ary_entry(r_cgi, n);
      str = mg_get_string(a, &p, &len);
      mg_request_add(p_page->p_srv, chndle, p_buf, (unsigned char *) str, len, (short) ifc[0], (short) ifc[1]);
   }
   ifc[1] = MG_TX_EOD;
   mg_request_add(p_page->p_srv, chndle, p_buf, NULL, 0, (short) ifc[0], (short) ifc[1]);

   str = mg_get_string(r_content, &p, &n);

   ifc[0] = 0;
   ifc[1] = MG_TX_DATA;
   mg_request_add(p_page->p_srv, chndle, p_buf, (unsigned char *) str, (int) strlen((char *) str), (short) ifc[0], (short) ifc[1]);

   mg_db_send(p_page->p_srv, chndle, p_buf, 1);


   return rb_int2inum((long) chndle);

}


static VALUE ex_ma_arg_set(VALUE self, VALUE a_list, VALUE r_argn, VALUE a, VALUE r_by_ref)
{
   int n, max, t, argn, by_ref, typ, phndle, len;
   char buffer[MG_BUFSIZE], error[256], props[256], bstr[256], bstr1[256];
   MGSTR properties[MG_MAX_KEY];
   char *str;
   VALUE p;
   VALUE pstr;

   *props = '\0';


   phndle = 0;

   if (mg_type(a_list) != MG_T_LIST) {
      MG_ERROR("mg_ruby: Argument 1 to 'ma_arg_set' must be an array");
      return mg_r_nil;
   }

   argn = mg_get_integer(r_argn);
   by_ref = mg_get_integer(r_by_ref);

   t = mg_type(a);

   if (t == MG_T_LIST) {
      typ = 1;
   }
   else {
      typ = 0;
   }

   pstr = 0;
   if (mg_get_array_size(a_list) > 0)
      pstr = rb_ary_entry(a_list, 0);

   str = NULL;
   strcpy(bstr, "");
   strcpy(bstr1, "");

   if (pstr) {
      t = mg_type(pstr);
      if (t == MG_T_STRING) {
         str = mg_get_string(pstr, &p, &len);

      }
   }

   if (str) {
      strcpy(bstr, str);
   }

   strcpy(error, "");

   n = mg_extract_substrings(properties, bstr, (int) strlen(bstr), '#', 0, 0, MG_ES_DELIM);
/*
{
   int i;
   char buffer[256], buf1[256], tmp[256];

   sprintf(buffer, "*************************** MA SET max=%d *******************", n);
   strcpy(buf1, "");
   for (i = 0; i <= n; i ++) {
      sprintf(tmp, "i=%d; ps=%s; size=%d;", i, properties[i].ps ? properties[i].ps : "null", properties[i].size);
      strcat(buf1, tmp);
   }
   mg_log_event(buf1, buffer);
}
*/
   if (n > argn)
      max = n - 1;
   else
      max = argn;

   for (n = 1; n <= max; n ++) {
      strcat(bstr1, "#");
      if (n == argn) {
         sprintf(buffer, "%d%d", typ, by_ref);
         strcat(bstr1, buffer);
      }
      else {
         if (properties[n].ps)
            strcat(bstr1, properties[n].ps);
         else
            strcat(bstr1, "00");
      }
   }

   pstr = rb_str_new2(bstr1);
   mg_set_list_item(a_list, 0, pstr);
   mg_set_list_item(a_list, argn, a);

   strcpy(buffer, "");

   return rb_str_new2(buffer);

}


static VALUE ex_ma_get_stream_data(VALUE self, VALUE r_chndle)
{
   MGBUF mgbuf, *p_buf;
   int n, phndle, chndle;
   MGPAGE *p_page;

   chndle = mg_get_integer(r_chndle);

   if (chndle < 0) {
      MG_ERROR("Bad connection handle");
      return mg_r_nil;
   }

   phndle = 0;
   p_page = tp_page[phndle]; /* mg_ppage(phndle); */

   p_buf = &mgbuf;
   mg_buf_init(p_buf, MG_BUFSIZE, MG_BUFSIZE);

   MG_FTRACE("ma_get_stream_data");

   phndle = 0;
   n = mg_db_receive(p_page->p_srv, chndle, p_buf, MG_BUFSIZE, 0);

   if (n < 1) {
      strcpy(p_buf->p_buffer, "");
      mg_db_disconnect(p_page->p_srv, chndle, 1);
      return rb_str_new2(p_buf->p_buffer);
   }

   if ((n = mg_get_error(p_page->p_srv, p_buf->p_buffer))) {
      MG_ERROR(p_buf->p_buffer + MG_RECV_HEAD);
      return mg_r_nil;
   }

   return rb_str_new(p_buf->p_buffer + MG_RECV_HEAD, p_buf->data_size - MG_RECV_HEAD);
}



static VALUE ex_ma_return_to_client(VALUE self, VALUE r_data)
{
   char record[MG_BUFSIZE];
   int phndle, len;
   VALUE p;
   char *ps;

   phndle = 0;

   ps = mg_get_string(r_data, &p, &len);

   strcpy(record, "\x07");
   if (ps) {
      memcpy((void *) record, (void *) ps, len);
      len ++;
   }
   else
      len = 1;

   return rb_str_new2(record);

}


static VALUE ex_ma_local_set(VALUE self, VALUE records, VALUE r_index, VALUE key, VALUE data)
{
   int result, index, max, mrec, rmax, start, found, rn, n, len;
   char buffer[MG_BUFSIZE];
   MGSTR rkey[MG_MAX_KEY], nkey[MG_MAX_KEY];
   VALUE r_nkey[MG_MAX_KEY];
   VALUE r_record;
   VALUE p;
   VALUE temp;
   char * ps;
   MGBUF mgbuf, *p_buf;

   rmax = 0;

   p_buf = &mgbuf;
   mg_buf_init(p_buf, MG_BUFSIZE, MG_BUFSIZE);

   index = mg_get_integer(r_index);

   if (mg_type(records) != MG_T_LIST) {
      MG_ERROR("mg_ruby: Argument 1 to 'ma_local_set' must be an array");
      return mg_r_nil;
   }
   if (mg_type(key) != MG_T_LIST) {
      MG_ERROR("mg_ruby: Argument 3 to 'ma_local_set' must be an array");
      return mg_r_nil;
   }

   mrec = mg_get_array_size(records);
   max = mg_get_keys(key, nkey, r_nkey, NULL);

   ps = mg_get_string(data, &p, &len);

   for (n = 1; n <= max; n ++) {
      mg_request_add(NULL, -1, p_buf, nkey[n].ps, nkey[n].size, 0, MG_TX_AKEY);
   }
   mg_request_add(NULL, -1, p_buf, (unsigned char *) ps, len, 0, MG_TX_DATA);

   r_record = rb_str_new((char *) p_buf->p_buffer, p_buf->data_size);

   if (index == -2) {
      mg_set_list_item(records, mrec, r_record);
   }
   else if (index > -1) {
      mg_set_list_item(records, index, r_record);
   }
   else {
      found = 0;
      start = 0;

      for (rn = start; rn < mrec; rn ++) {
         p = rb_ary_entry(records, rn);
         ps = mg_get_string(p, &temp, &len);
         memcpy((void *) buffer, (void *) ps, len);
         rmax = mg_extract_substrings(rkey, buffer, len, '#', 1, 0, MG_ES_BLOCK);
         rmax --;
         if (rmax == max) {
            if (mg_compare_keys(nkey, rkey, max) == 0) {

               mg_set_list_item(records, rn, r_record);
               found = 1;
               break;
            }
         }
      }
      if (!found) {
         mg_set_list_item(records, mrec, r_record);
      }
   }
   result = rmax;

   return rb_str_new(p_buf->p_buffer, p_buf->data_size);

}


static VALUE  ex_ma_local_get(VALUE self, VALUE records, VALUE r_index, VALUE key)
{
   int index, max, mrec, rmax, start, rn, len;
   char record[MG_BUFSIZE], buffer[MG_BUFSIZE];
   MGSTR rkey[MG_MAX_KEY], nkey[MG_MAX_KEY];
   VALUE r_nkey[MG_MAX_KEY];
   VALUE p;
   VALUE temp;
   char * ps;
   char * result;

   rmax = 0;

   result = record;
   strcpy(record, "");

   index = mg_get_integer(r_index);

   if (mg_type(records) != MG_T_LIST) {
      MG_ERROR("mg_ruby: Argument 1 to 'ma_local_get' must be an array");
      return mg_r_nil;
   }
   if (mg_type(key) != MG_T_LIST) {
      MG_ERROR("mg_ruby: Argument 3 to 'ma_local_get' must be an array");
      return mg_r_nil;
   }

   mrec = mg_get_array_size(records);
   max = mg_get_keys(key, nkey, r_nkey, record);

   if (index > -1) {
      if (index < mrec) {
         p = rb_ary_entry(records, index);
         ps = mg_get_string(p, &temp, &len);
         memcpy((void *) buffer, (void *) ps, len);
         rmax = mg_extract_substrings(rkey, buffer, (int) strlen(buffer), '#', 1, 0, MG_ES_BLOCK);
         result = rkey[rmax].ps;
         len = rkey[rmax].size;
      }
   }
   else {
      start = 0;
      for (rn = start; rn < mrec; rn ++) {
         p = rb_ary_entry(records, rn);
         ps = mg_get_string(p, &temp, &len);
         memcpy((void *) buffer, (void *) ps, len);
         rmax = mg_extract_substrings(rkey, buffer, len, '#', 1, 0, MG_ES_BLOCK);
         rmax --;
         if (rmax == max) {
            if (mg_compare_keys(nkey, rkey, max) == 0) {
               result = rkey[rmax + 1].ps;
               len = rkey[rmax + 1].size;
               break;
            }
         }
      }
   }


   return rb_str_new(result, len);
}


static VALUE  ex_ma_local_data(VALUE self, VALUE records, VALUE r_index, VALUE key)
{
   int result, index, max, mrec, rmax, start, rn, data, subs, len;
   char record[MG_BUFSIZE], buffer[MG_BUFSIZE];
   MGSTR rkey[MG_MAX_KEY], nkey[MG_MAX_KEY];
   VALUE r_nkey[MG_MAX_KEY];
   VALUE p;
   VALUE temp;
   char * ps;

   result = 0;
   rmax = 0;

   ps = record;
   strcpy(record, "");


   index = mg_get_integer(r_index);

   if (mg_type(records) != MG_T_LIST) {
      MG_ERROR("mg_ruby: Argument 1 to 'ma_local_data' must be an array");
      return mg_r_nil;
   }
   if (mg_type(key) != MG_T_LIST) {
      MG_ERROR("mg_ruby: Argument 3 to 'ma_local_data' must be an array");
      return mg_r_nil;
   }

   mrec = mg_get_array_size(records);
   max = mg_get_keys(key, nkey, r_nkey, record);

   result = 0;
   if (index > -1) {
      if (index < mrec) {
         result = 1;
      }
   }
   else {
      start = 0;
      data = 0;
      subs = 0;
      for (rn = start; rn < mrec; rn ++) {
         p = rb_ary_entry(records, rn);
         ps = mg_get_string(p, &temp, &len);
         memcpy((void *) buffer, (void *) ps, len);
         rmax = mg_extract_substrings(rkey, buffer, (int) strlen(buffer), '#', 1, 0, MG_ES_BLOCK);
         rmax --;
         if (rmax >= max) {
            if (mg_compare_keys(nkey, rkey, max) == 0) {
               if (rmax == max)
                  data = 1;
               else
                  subs = 10;
               if (data > 0 && subs > 0)
               break;
            }
         }
      }
      result = data + subs;
   }

   return rb_int2inum((long) result);
}




static VALUE  ex_ma_local_kill(VALUE self, VALUE records, VALUE r_index, VALUE key)
{
   int result, index, max, mrec, rmax, start, rn, len;
   char record[MG_BUFSIZE], buffer[MG_BUFSIZE];
   MGSTR rkey[MG_MAX_KEY], nkey[MG_MAX_KEY];
   VALUE r_nkey[MG_MAX_KEY];
   VALUE p;
   VALUE temp;
   char * ps;

   rmax = 0;
   result = 0;

   ps = record;
   strcpy(record, "");

   index = mg_get_integer(r_index);

   if (mg_type(records) != MG_T_LIST) {
      MG_ERROR("mg_ruby: Argument 1 to 'ma_local_kill' must be an array");
      return mg_r_nil;
   }
   if (mg_type(key) != MG_T_LIST) {
      MG_ERROR("mg_ruby: Argument 3 to 'ma_local_kill' must be an array");
      return mg_r_nil;
   }

   mrec = mg_get_array_size(records);
   max = mg_get_keys(key, nkey, r_nkey, record);

   if (index > -1) {
      if (index < mrec) {
         mg_kill_list_item(records, index);
         result ++;
      }
   }
   else {
      start = 0;
      for (rn = start; rn < mrec; rn ++) {
         p = rb_ary_entry(records, rn);
         ps = mg_get_string(p, &temp, &len);
         memcpy((void *) buffer, (void *) ps, len);
         rmax = mg_extract_substrings(rkey, buffer, len, '#', 1, 0, MG_ES_BLOCK);
         rmax --;
         if (rmax >= max) {
            if (mg_compare_keys(nkey, rkey, max) == 0) {
               mg_kill_list_item(records, rn);
               result ++;
            }
         }
      }
   }

   return rb_int2inum((long) result);
}


static VALUE  ex_ma_local_order(VALUE self, VALUE records, VALUE r_index, VALUE key)
{
   int result, index, max, mrec, rmax, start, rn, found, next, len;
   char record[MG_BUFSIZE], buffer[MG_BUFSIZE];
   MGSTR rkey[MG_MAX_KEY], nkey[MG_MAX_KEY];
   VALUE r_nkey[MG_MAX_KEY];
   VALUE p;
   VALUE temp;
   char *vkey, *vrkey, *vrkey1;
   char * ps;

   rmax = 0;
   result = -1;
   ps = record;
   strcpy(record, "");

   index = mg_get_integer(r_index);

   if (mg_type(records) != MG_T_LIST) {
      MG_ERROR("mg_ruby: Argument 1 to 'ma_local_order' must be an array");
      return mg_r_nil;
   }
   if (mg_type(key) != MG_T_LIST) {
      MG_ERROR("mg_ruby: Argument 3 to 'ma_local_order' must be an array");
      return mg_r_nil;
   }

   mrec = mg_get_array_size(records);
   max = mg_get_keys(key, nkey, r_nkey, record);
   vkey = nkey[max].ps;

   start = 0;

   if (index > -1) {
      start = index;
   }

   found = 0;
   next = -1;

   for (rn = start; rn < mrec; rn ++) {
      p = rb_ary_entry(records, rn);
      ps = mg_get_string(p, &temp, &len);
      memcpy((void *) buffer, (void *) ps, len);
      rmax = mg_extract_substrings(rkey, buffer, len, '#', 1, 0, MG_ES_BLOCK);
      rmax --;
      if (rmax >= max) {
         vrkey = rkey[max].ps;
         if (mg_compare_keys(nkey, rkey, max - 1) == 0) {

            if (strlen(vkey) == 0 && strlen(vrkey) > 0) {
               p = rb_str_new2(vrkey);
               mg_set_list_item(key, max, p);
               result = rn;
               break;
            }
            if (found == 1 && strcmp(vrkey, vkey)) {
               p = rb_str_new2(vrkey);
               mg_set_list_item(key, max, p);
               result = rn;
               break;
            }
            if (strcmp(vrkey, vkey) > 0) {
               if (next == -1) {
                  next = rn;
                  vrkey1 = vrkey;
               }
            }
            if (mg_compare_keys(nkey, rkey, max) == 0) {
               found = 1;
            }
         }
      }
   }

   if (found == 0 && next != -1) {
      result = rn;
      p = rb_str_new2(vrkey1);
      mg_set_list_item(key, max, p);
   }

   if (result == -1) {
      p = rb_str_new2("");
      mg_set_list_item(key, max, p);
   }

   return rb_int2inum((long) result);
}



static VALUE  ex_ma_local_previous(VALUE self, VALUE records, VALUE r_index, VALUE key)
{
   int result, index, max, mrec, rmax, start, rn, found, next, len;
   char record[MG_BUFSIZE], buffer[MG_BUFSIZE];
   MGSTR rkey[MG_MAX_KEY], nkey[MG_MAX_KEY];
   VALUE r_nkey[MG_MAX_KEY];
   VALUE p;
   VALUE temp;
   char *vkey, *vrkey, *vrkey1;
   char * ps;

   rmax = 0;
   result = -1;
   ps = record;
   strcpy(record, "");

   index = mg_get_integer(r_index);

   if (mg_type(records) != MG_T_LIST) {
      MG_ERROR("mg_ruby: Argument 1 to 'ma_local_previous' must be an array");
      return mg_r_nil;
   }
   if (mg_type(key) != MG_T_LIST) {
      MG_ERROR("mg_ruby: Argument 3 to 'ma_local_previous' must be an array");
      return mg_r_nil;
   }

   mrec = mg_get_array_size(records);
   max = mg_get_keys(key, nkey, r_nkey, record);
   vkey = nkey[max].ps;

   start = 0;

   if (index == -1) {
      index = mrec - 1;
   }

   found = 0;
   next = -1;

   for (rn = index; rn >= start; rn --) {
      p = rb_ary_entry(records, rn);
      ps = mg_get_string(p, &temp, &len);
      memcpy((void *) buffer, (void *) ps, len);
      rmax = mg_extract_substrings(rkey, buffer, len, '#', 1, 0, MG_ES_BLOCK);
      rmax --;
      if (rmax >= max) {
         vrkey = rkey[max].ps;
         if (mg_compare_keys(nkey, rkey, max - 1) == 0) {
            if (strlen(vkey) == 0 && strlen(vrkey) > 0) {
               p = rb_str_new2(vrkey);
               mg_set_list_item(key, max, p);
               result = rn;
               break;
            }
            if (found == 1 && strcmp(vrkey, vkey)) {
               p = rb_str_new2(vrkey);
               mg_set_list_item(key, max, p);
               result = rn;
               break;
            }
            if (strcmp(vrkey, vkey) < 0) {
               if (next == -1) {
                  next = rn;
                  vrkey1 = vrkey;
               }
            }
            if (mg_compare_keys(nkey, rkey, max) == 0) {
               found = 1;
            }
         }
      }
   }

   if (found == 0 && next != -1) {
      result = rn;
      p = rb_str_new2(vrkey1);
      mg_set_list_item(key, max, p);
   }

   if (result == -1) {
      p = rb_str_new2("");
      mg_set_list_item(key, max, p);
   }

   return rb_int2inum((long) result);
}


static VALUE  ex_ma_local_sort(VALUE self, VALUE records)
{
   MGBUF mgbuf, *p_buf;
   int n, max, chndle, phndle, anybyref, len;
   char ifc[4];
   char ret[MG_BUFSIZE], buffer[MG_BUFSIZE];
   char *str;
   VALUE a;
   VALUE p;
   MGPAGE *p_page;

   if (mg_type(records) != MG_T_LIST) {
      MG_ERROR("mg_ruby: Argument 1 to 'ma_local_sort' must be an array");
      return mg_r_nil;
   }

   phndle = 0;
   p_page = mg_ppage(phndle);

   p_buf = &mgbuf;
   mg_buf_init(p_buf, MG_BUFSIZE, MG_BUFSIZE);

   MG_FTRACE("ma_local_sort");

   chndle = 0;
   max = 0;
   anybyref = 1;

   n = mg_db_connect(p_page->p_srv, &chndle, 1);
   if (!n) {
      MG_ERROR(p_page->p_srv->error_mess);
      return mg_r_nil;
   }

   mg_request_header(p_page->p_srv, p_buf, "X", MG_PRODUCT);

   ifc[0] = 0;
   ifc[1] = MG_TX_DATA;
   strcpy(buffer, "sort^%ZMGS");
   mg_request_add(p_page->p_srv, chndle, p_buf, (unsigned char *) buffer, (int) strlen((char *) buffer), (short) ifc[0], (short) ifc[1]);


   ifc[0] = 1;
   ifc[1] = MG_TX_AREC;
   mg_request_add(p_page->p_srv, chndle, p_buf, NULL, 0, (short) ifc[0], (short) ifc[1]);

   ifc[1] = MG_TX_AREC_FORMATTED;

   max = mg_get_array_size(records);
   for (n = 0; n < max; n ++) {
      a = rb_ary_entry(records, n);
      str = mg_get_string(a, &p, &len);
      mg_request_add(p_page->p_srv, chndle, p_buf, (unsigned char *) str, len, (short) ifc[0], (short) ifc[1]);
   }
   ifc[1] = MG_TX_EOD;
   mg_request_add(p_page->p_srv, chndle, p_buf, NULL, 0, (short) ifc[0], (short) ifc[1]);

   mg_db_send(p_page->p_srv, chndle, p_buf, 1);
   mg_db_receive(p_page->p_srv, chndle, p_buf, MG_BUFSIZE, 0);

   MG_MEMCHECK("Insufficient memory to process response", 0);

   mg_db_disconnect(p_page->p_srv, chndle, 1);

   if ((n = mg_get_error(p_page->p_srv, p_buf->p_buffer))) {
      MG_ERROR(p_buf->p_buffer + MG_RECV_HEAD);
      return mg_r_nil;
   }

   if (anybyref) {
      short byref, type, stop;
      int n, n1, rn, hlen, size, clen, rlen, argc, rec_len;
      unsigned char *parg, *par;

      stop = 0;
      parg = (p_buf->p_buffer + MG_RECV_HEAD);

      clen = mg_decode_size(p_buf->p_buffer, 5, MG_CHUNK_SIZE_BASE);

      rlen = 0;
      argc = 0;
      for (n = 0;; n ++) {
         hlen = mg_decode_item_header(parg, &size, (short *) &byref, (short *) &type);
         if ((hlen + size + rlen) > clen) {
            stop = 1;
            break;
         }
         parg += hlen;
         rlen += hlen;
/*
{
   unsigned char c;
   char buffer[256];
   c = *(parg + size);
   *(parg + size) = '\0';
   sprintf(buffer, "RESULT %d: argc=%d; hlen=%d; size=%d; byref=%d; type=%d; rlen=%d; clen=%d", n, argc, hlen, size, byref, type, rlen, clen);
   mg_log_event(parg, buffer);
   *(parg + size) = c;
}
*/
         parg += size;
         rlen += size;
         if (type == MG_TX_AREC) {
            par = parg;
            rn = 0;
            rec_len = 0;
            for (n1 = 0;; n1 ++) {
               hlen = mg_decode_item_header(parg, &size, (short *) &byref, (short *) &type);
               if ((hlen + size + rlen) > clen) {
                  stop = 1;
                  break;
               }
               if (type == MG_TX_EOD) {
                  parg += (hlen + size);
                  rlen += (hlen + size);
                  break;
               }
               parg += hlen;
               rlen += hlen;
               rec_len += hlen;
/*
{
   unsigned char c;
   char buffer[256];
   c = *(parg + size);
   *(parg + size) = '\0';
   sprintf(buffer, "RESULT ARRAY %d: argc=%d; hlen=%d; size=%d; byref=%d; type=%d; rlen=%d; clen=%d", n1, argc, hlen, size, byref, type, rlen, clen);
   mg_log_event(parg, buffer);
   *(parg + size) = c;
}
*/
               parg += size;
               rlen += size;
               rec_len += size;
               if (type == MG_TX_DATA) {
/*
{
   unsigned char c;
   char buffer[256];
   c = *(parg + size);
   *(parg + size) = '\0';
   sprintf(buffer, "RESULT ARRAY RECORD %d: argc=%d; hlen=%d; size=%d; byref=%d; type=%d; rlen=%d; clen=%d; rec_len=%d", n1, argc, hlen, size, byref, type, rlen, clen, rec_len);
   mg_log_event(par, buffer);
   *(parg + size) = c;
}
*/

                  p = rb_str_new(par, rec_len);
                  mg_set_list_item(records, rn ++, p);

                  par = parg;
                  rec_len = 0;
               }
            }
         }
         if (rlen >= clen || stop)
            break;
         argc ++;
      }
      if (stop) {
         MG_ERROR("ma_local_sort: Bad return data");
         return mg_r_nil;
      }
   }

   return rb_str_new2(ret);
}


#if defined(_WIN32)
__declspec(dllexport) void __cdecl Init_mg_ruby() {
#else
void Init_mg_ruby() {
#endif

   mg_ruby = rb_define_class("MG_RUBY", rb_cObject);

   mg_mclass = ex_m_mclass(); /* v2.3.43 */
/*
   rb_define_method(mg_ruby, "initialize", t_init, 0);
   rb_define_method(mg_ruby, "add", t_add, 1);
*/
   rb_define_method(mg_ruby, "m_ext_version", ex_m_ext_version, 0);

   rb_define_method(mg_ruby, "m_set_host", ex_m_set_host, 4);
   rb_define_method(mg_ruby, "m_set_uci", ex_m_set_uci, 1);
   rb_define_method(mg_ruby, "m_set_server", ex_m_set_server, 1);
   rb_define_method(mg_ruby, "m_set_timeout", ex_m_set_timeout, 1);

   rb_define_method(mg_ruby, "m_bind_server_api", ex_m_bind_server_api, 6);
   rb_define_method(mg_ruby, "m_release_server_api", ex_m_release_server_api, 0);

   rb_define_method(mg_ruby, "m_set_storage_mode", ex_m_set_storage_mode, 1);
   rb_define_method(mg_ruby, "m_get_last_error", ex_m_get_last_error, 0);

   rb_define_method(mg_ruby, "m_set", ex_m_set, -1);
   rb_define_method(mg_ruby, "ma_set", ex_ma_set, 3);
   rb_define_method(mg_ruby, "m_get", ex_m_get, -1);
   rb_define_method(mg_ruby, "ma_get", ex_ma_get, 2);
   rb_define_method(mg_ruby, "m_kill", ex_m_kill, -1);
   rb_define_method(mg_ruby, "ma_kill", ex_ma_kill, 2);
   rb_define_method(mg_ruby, "m_delete", ex_m_kill, -1);
   rb_define_method(mg_ruby, "ma_delete", ex_ma_kill, 2);
   rb_define_method(mg_ruby, "m_data", ex_m_data, -1);
   rb_define_method(mg_ruby, "ma_data", ex_ma_data, 2);
   rb_define_method(mg_ruby, "m_defined", ex_m_data, -1);
   rb_define_method(mg_ruby, "ma_defined", ex_ma_data, 2);
   rb_define_method(mg_ruby, "m_order", ex_m_order, -1);
   rb_define_method(mg_ruby, "ma_order", ex_ma_order, 2);
   rb_define_method(mg_ruby, "m_previous", ex_m_previous, -1);
   rb_define_method(mg_ruby, "ma_previous", ex_ma_previous, 2);

   /* v2.2.41 */
   rb_define_method(mg_ruby, "m_increment", ex_m_increment, -1);
   rb_define_method(mg_ruby, "m_tstart", ex_m_tstart, -1);
   rb_define_method(mg_ruby, "m_tlevel", ex_m_tlevel, -1);
   rb_define_method(mg_ruby, "m_tcommit", ex_m_tcommit, -1);
   rb_define_method(mg_ruby, "m_trollback", ex_m_trollback, -1);
   rb_define_method(mg_ruby, "m_sleep", ex_m_sleep, 1);

   rb_define_method(mg_ruby, "ma_merge_to_db", ex_ma_merge_to_db, 4);
   rb_define_method(mg_ruby, "ma_merge_from_db", ex_ma_merge_from_db, 4);

   rb_define_method(mg_ruby, "m_function", ex_m_function, -1);
   rb_define_method(mg_ruby, "ma_function", ex_ma_function, 3);
   rb_define_method(mg_ruby, "m_classmethod", ex_m_classmethod, -1);
   rb_define_method(mg_ruby, "ma_classmethod", ex_ma_classmethod, 4);

   rb_define_method(mg_ruby, "ma_html_ex", ex_ma_html_ex, 3);
   rb_define_method(mg_ruby, "ma_html_classmethod_ex", ex_ma_html_classmethod_ex, 4);
   rb_define_method(mg_ruby, "ma_http_ex", ex_ma_http_ex, 2);
   rb_define_method(mg_ruby, "ma_arg_set", ex_ma_arg_set, 4);

   rb_define_method(mg_ruby, "ma_get_stream_data", ex_ma_get_stream_data, 1);
   rb_define_method(mg_ruby, "ma_return_to_client", ex_ma_return_to_client, 1);

   rb_define_method(mg_ruby, "ma_local_set", ex_ma_local_set, 4);
   rb_define_method(mg_ruby, "ma_local_get", ex_ma_local_get, 3);
   rb_define_method(mg_ruby, "ma_local_data", ex_ma_local_data, 3);
   rb_define_method(mg_ruby, "ma_local_kill", ex_ma_local_kill, 3);
   rb_define_method(mg_ruby, "ma_local_order", ex_ma_local_order, 3);
   rb_define_method(mg_ruby, "ma_local_previous", ex_ma_local_previous, 3);
   rb_define_method(mg_ruby, "ma_local_sort", ex_ma_local_sort, 1);

   dbx_init();

}



int mg_type(VALUE item)
{
   int type, result;

   if (!item)
      return -1;

   type = TYPE(item);

   if (type == T_STRING)
      result = MG_T_STRING;
   else if (type == T_FIXNUM)
      result = MG_T_INTEGER;
   else if (type == T_BIGNUM)
      result = MG_T_INTEGER;
   else if (type == T_FLOAT)
      result = MG_T_FLOAT;
   else if (type == T_ARRAY)
      result = MG_T_LIST;
   else
      result = MG_T_VAR;

   return result;
}


int mg_get_array_size(VALUE rb_array)
{
   int result;

   result = 0;
   result = RARRAY_LEN(rb_array);
   return result;
}


int mg_get_integer(VALUE item)
{
   int t, result;
   char *ps;

   result = 0;
   t = mg_type(item);

   if (t == MG_T_INTEGER)
      result = (int) rb_num2long(item);
   else if (t == MG_T_FLOAT) {
      result = (int) rb_num2dbl(item);
   }
   else {
      ps = rb_string_value_cstr(&item);
      if (ps)
         result = (int) strtol(ps, NULL, 10);
      else
         result = 0;
   }

   return result;
}


double mg_get_float(VALUE item)
{
   int t;
   double result;
   char *ps;

   result = 0;
   t = mg_type(item);

   if (t == MG_T_INTEGER)
      result = (double) rb_num2long(item);
   else if (t == MG_T_FLOAT) {
      result = (double) rb_num2dbl(item);
   }
   else {
      ps = rb_string_value_cstr(&item);
      if (ps)
         result = (double) strtod(ps, NULL);
      else
         result = 0;
   }

   return result;
}



char * mg_get_string(VALUE item, VALUE * item_tmp, int * size)
{
   int t, x;
   double y;
   char * result;
   char buffer[64];

   *size = 0;
   result = NULL;
   t = mg_type(item);
   
   if (t == MG_T_INTEGER) {
      x = (int) rb_num2long(item);
      sprintf(buffer, "%d", x);
      *size = (int) strlen(buffer);
      *item_tmp = rb_str_new2(buffer);
      result = rb_string_value_cstr(item_tmp);

   }
   else if (t == MG_T_FLOAT) {
      y = (double) rb_num2dbl(item);
      sprintf(buffer, "%f", y);
      *size = (int) strlen(buffer);
      *item_tmp = rb_str_new2(buffer);
      result = rb_string_value_cstr(item_tmp);
   }
   else {
      result = rb_string_value_ptr(&item);
      *size = RSTRING_LEN(item);
   }

   return result;
}


int mg_get_vargs(int argc, VALUE *argv, MGVARGS *pvargs, int context)
{
   int n;

   if (context) {
      for (n = 0; n < argc; n ++) {
         pvargs->cvars[n].ps = (unsigned char *) mg_get_string(argv[n], &(pvargs->rvars[n]), &(pvargs->cvars[n].size));
      }
   }
   else {
      for (n = 0; n < argc; n ++) {
         pvargs->cvars[n].ps = (unsigned char *) mg_get_string(argv[n], &(pvargs->rvars[n]), &(pvargs->cvars[n].size));
         if (n == 0) {
            pvargs->global = (char *) pvargs->cvars[n].ps;
            pvargs->global_len = pvargs->cvars[n].size;
         }
      }
   }

   return n;
}



int mg_get_keys(VALUE keys, MGSTR * ckeys, VALUE *keys_tmp, char *krec)
{
   int kmax, max, n, len;
   VALUE p;

   kmax = 32;

   p = rb_ary_entry(keys, 0);
   max = mg_get_integer(p);


   for (n = 1; n <= max; n ++) {
      p = rb_ary_entry(keys, n);

      ckeys[n].ps = mg_get_string(p, &keys_tmp[n], &len);
      ckeys[n].size = len;
   }

   return max;
}



int mg_set_list_item(VALUE list, int index, VALUE item)
{

   rb_ary_store(list, index, item);

   return index;
}


int mg_kill_list(VALUE list)
{
   int result;

   result = (int) rb_ary_clear(list);

   return result;
}


int mg_kill_list_item(VALUE list, int index)
{
   int result;

   result = (int) rb_ary_delete_at(list, index);

   return result;
}


MGPAGE * mg_ppage(int phndle)
{
   MGPAGE *p_page;

   if (phndle >= 0 && phndle < MG_MAX_PAGE && tp_page[phndle]) {
      return tp_page[phndle];
   }

   p_page = &gpage;
   p_page->p_srv = &(p_page->srv);
   mg_ppage_init(p_page);
   tp_page[0] = p_page;

   return p_page;
}


int mg_ppage_init(MGPAGE * p_page)
{
   int n;

   p_page->p_srv->mem_error = 0;
   p_page->p_srv->storage_mode = 0;
   p_page->p_srv->timeout = 0;
   p_page->p_srv->no_retry = 0;

   strcpy(p_page->p_srv->server, "");
   strcpy(p_page->p_srv->username, "");
   strcpy(p_page->p_srv->password, "");

   strcpy(p_page->p_srv->ip_address, MG_HOST);
   p_page->p_srv->port = MG_PORT;
   strcpy(p_page->p_srv->uci, MG_UCI);

   for (n = 0; n < MG_MAXCON; n ++) {
      p_page->p_srv->pcon[n] = NULL;
   }

   return 1;
}


