#ifndef _GLUSTERD_GEO_REPLICATION_H_
#define _GLUSTERD_GEO_REPLICATION_H_

#ifndef _CONFIG_H
#define _CONFIG_H
#include "config.h"
#endif

#include "uuid.h"

#include "glusterfs.h"
#include "xlator.h"
#include "logging.h"
#include "call-stub.h"
#include "fd.h"
#include "byte-order.h"
#include "glusterd.h"
#include "protocol-common.h"


#ifndef GSYNC_CONF
#define GSYNC_CONF GEOREP"/gsyncd.conf"
#endif


typedef struct glusterd_gsync_slaves {
        char *slave;
        char *host_uuid;
        int   ret_status;
        char rmt_hostname[256];
} glusterd_gsync_slaves_t;

int
glusterd_op_stage_gsync_set (dict_t *dict, char **op_errstr);
int
glusterd_op_gsync_set (dict_t *dict, char **op_errstr, dict_t *rsp_dict);
int
glusterd_check_gsync_running (glusterd_volinfo_t *volinfo, gf_boolean_t *flag);
int
glusterd_start_gsync (glusterd_volinfo_t *master_vol, char *slave,
                      char *glusterd_uuid_str, char **op_errstr);
#endif



