/*
  Copyright (c) 2010 Gluster, Inc. <http://www.gluster.com>
  This file is part of GlusterFS.

  GlusterFS is free software; you can redistribute it and/or modify
  it under the terms of the GNU Affero General Public License as published
  by the Free Software Foundation; either version 3 of the License,
  or (at your option) any later version.

  GlusterFS is distributed in the hope that it will be useful, but
  WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  Affero General Public License for more details.

  You should have received a copy of the GNU Affero General Public License
  along with this program.  If not, see
  <http://www.gnu.org/licenses/>.
*/


#ifndef _CONFIG_H
#define _CONFIG_H
#include "config.h"
#endif

#include "rpc-clnt.h"
#include "glusterd1-xdr.h"
#include "glusterd1.h"
#include "cli1.h"

#include "compat-errno.h"
#include "glusterd-op-sm.h"
#include "glusterd-sm.h"
#include "glusterd.h"
#include "protocol-common.h"
#include "glusterd-utils.h"
#include "common-utils.h"
#include <sys/uio.h>


#define SERVER_PATH_MAX  (16 * 1024)


extern glusterd_op_info_t opinfo;

int32_t
glusterd3_1_brick_op (call_frame_t *frame, xlator_t *this,
                      void *data);
int32_t
glusterd_op_send_cli_response (glusterd_op_t op, int32_t op_ret,
                               int32_t op_errno, rpcsvc_request_t *req,
                               void *op_ctx, char *op_errstr)
{
        int32_t         ret = -1;
        gd_serialize_t  sfunc = NULL;
        void            *cli_rsp = NULL;
        dict_t          *ctx = NULL;
        char            *free_ptr = NULL;
        glusterd_conf_t *conf = NULL;

        GF_ASSERT (THIS);

        conf = THIS->private;

        GF_ASSERT (conf);

        switch (op) {
        case GD_OP_CREATE_VOLUME:
        {
                gf1_cli_create_vol_rsp rsp = {0,};
                rsp.op_ret = op_ret;
                rsp.op_errno = op_errno;
                rsp.volname = "";
                if (op_errstr)
                        rsp.op_errstr = op_errstr;
                else
                        rsp.op_errstr = "";
                cli_rsp = &rsp;
                sfunc = gf_xdr_serialize_cli_create_vol_rsp;
                break;
        }

        case GD_OP_START_VOLUME:
        {
                gf1_cli_start_vol_rsp rsp = {0,};
                rsp.op_ret = op_ret;
                rsp.op_errno = op_errno;
                rsp.volname = "";
                if (op_errstr)
                        rsp.op_errstr = op_errstr;
                else
                        rsp.op_errstr = "";
                cli_rsp = &rsp;
                sfunc = gf_xdr_serialize_cli_start_vol_rsp;
                break;
        }

        case GD_OP_STOP_VOLUME:
        {
                gf1_cli_stop_vol_rsp rsp = {0,};
                rsp.op_ret = op_ret;
                rsp.op_errno = op_errno;
                rsp.volname = "";
                if (op_errstr)
                        rsp.op_errstr = op_errstr;
                else
                        rsp.op_errstr = "";
                cli_rsp = &rsp;
                sfunc = gf_xdr_serialize_cli_stop_vol_rsp;
                break;
        }

        case GD_OP_DELETE_VOLUME:
        {
                gf1_cli_delete_vol_rsp rsp = {0,};
                rsp.op_ret = op_ret;
                rsp.op_errno = op_errno;
                rsp.volname = "";
                if (op_errstr)
                        rsp.op_errstr = op_errstr;
                else
                        rsp.op_errstr = "";
                cli_rsp = &rsp;
                sfunc = gf_xdr_serialize_cli_delete_vol_rsp;
                break;
        }

        case GD_OP_DEFRAG_VOLUME:
        {
                gf1_cli_defrag_vol_rsp rsp = {0,};
                rsp.op_ret = op_ret;
                rsp.op_errno = op_errno;
                //rsp.volname = "";
                cli_rsp = &rsp;
                sfunc = gf_xdr_serialize_cli_defrag_vol_rsp;
                break;
        }

        case GD_OP_ADD_BRICK:
        {
                gf1_cli_add_brick_rsp rsp = {0,};
                rsp.op_ret = op_ret;
                rsp.op_errno = op_errno;
                rsp.volname = "";
                if (op_errstr)
                        rsp.op_errstr = op_errstr;
                else
                        rsp.op_errstr = "";
                cli_rsp = &rsp;
                sfunc = gf_xdr_serialize_cli_add_brick_rsp;
                break;
        }

        case GD_OP_REMOVE_BRICK:
        {
                gf1_cli_remove_brick_rsp rsp = {0,};
                ctx = op_ctx;
                if (ctx &&
                    dict_get_str (ctx, "errstr", &rsp.op_errstr))
                        rsp.op_errstr = "";
                rsp.op_ret = op_ret;
                rsp.op_errno = op_errno;
                rsp.volname = "";
                cli_rsp = &rsp;
                sfunc = gf_xdr_serialize_cli_remove_brick_rsp;
                break;
        }

        case GD_OP_REPLACE_BRICK:
        {
                gf1_cli_replace_brick_rsp rsp = {0,};
                ctx = op_ctx;
                if (ctx &&
                    dict_get_str (ctx, "status-reply", &rsp.status))
                        rsp.status = "";
                rsp.op_ret = op_ret;
                rsp.op_errno = op_errno;
                if (op_errstr)
                        rsp.op_errstr = op_errstr;
                else
                        rsp.op_errstr = "";
                rsp.volname = "";
                cli_rsp = &rsp;
                sfunc = gf_xdr_serialize_cli_replace_brick_rsp;
                break;
        }

        case GD_OP_SET_VOLUME:
        {
                gf1_cli_set_vol_rsp rsp = {0,};
                rsp.op_ret = op_ret;
                rsp.op_errno = op_errno;
                rsp.volname = "";
                if (op_errstr)
                        rsp.op_errstr = op_errstr;
                else
                        rsp.op_errstr = "";
                cli_rsp = &rsp;
                sfunc = gf_xdr_serialize_cli_set_vol_rsp;
                break;
        }

        case GD_OP_RESET_VOLUME:
        {
                gf_log ("", GF_LOG_DEBUG, "Return value to CLI");
                gf1_cli_reset_vol_rsp rsp = {0,};
                rsp.op_ret = op_ret;
                rsp.op_errno = 1;
                rsp.volname = "";
                if (op_errstr)
                        rsp.op_errstr = op_errstr;
                else
                        rsp.op_errstr = "Error while resetting options";
                cli_rsp = &rsp;
                sfunc = gf_xdr_serialize_cli_reset_vol_rsp;
                break;
        }

        case GD_OP_LOG_FILENAME:
        {
                gf1_cli_log_filename_rsp rsp = {0,};
                rsp.op_ret = op_ret;
                rsp.op_errno = op_errno;
                if (op_errstr)
                        rsp.errstr = op_errstr;
                else
                        rsp.errstr = "";
                cli_rsp = &rsp;
                sfunc = gf_xdr_serialize_cli_log_filename_rsp;
                break;
        }
        case GD_OP_LOG_ROTATE:
        {
                gf1_cli_log_rotate_rsp rsp = {0,};
                rsp.op_ret = op_ret;
                rsp.op_errno = op_errno;
                if (op_errstr)
                        rsp.errstr = op_errstr;
                else
                        rsp.errstr = "";
                cli_rsp = &rsp;
                sfunc = gf_xdr_serialize_cli_log_rotate_rsp;
                break;
        }
        case GD_OP_SYNC_VOLUME:
        {
                gf1_cli_sync_volume_rsp rsp = {0,};
                rsp.op_ret = op_ret;
                rsp.op_errno = op_errno;
                if (op_errstr)
                        rsp.op_errstr = op_errstr;
                else
                        rsp.op_errstr = "";
                cli_rsp = &rsp;
                sfunc = gf_xdr_from_cli_sync_volume_rsp;
                break;
        }
        case GD_OP_GSYNC_SET:
        {
                int     type = 0;
                char    *str = NULL;
                gf1_cli_gsync_set_rsp rsp = {0,};

                ctx = op_ctx;
                rsp.op_ret = op_ret;
                rsp.op_errno = op_errno;
                rsp.op_errstr = "";
                if (ctx) {
                        ret = dict_get_str (ctx, "errstr", &str);
                        if (ret == 0)
                                rsp.op_errstr = str;
                        ret = dict_get_int32 (ctx, "type", &type);
                        if (ret == 0)
                                rsp.type = type;
                        ret = dict_set_str (ctx, "glusterd_workdir", conf->workdir);
                        /* swallow error here, that will be re-triggered in cli */

                        ret = dict_allocate_and_serialize (ctx,
                                                           &rsp.dict.dict_val,
                                                           (size_t*)&rsp.dict.dict_len);

                        if (ret == 0)
                                free_ptr = rsp.dict.dict_val;

                }
                if (op_errstr)
                        rsp.op_errstr = op_errstr;
                cli_rsp = &rsp;
                sfunc = gf_xdr_serialize_cli_gsync_set_rsp;
                break;
        }
        case GD_OP_RENAME_VOLUME:
        case GD_OP_START_BRICK:
        case GD_OP_STOP_BRICK:
        case GD_OP_LOG_LOCATE:
        {
                gf_log ("", GF_LOG_DEBUG, "not supported op %d", op);
                break;
        }
        case GD_OP_PROFILE_VOLUME:
        {
                gf1_cli_stats_volume_rsp rsp = {0,};
                rsp.op_ret = op_ret;
                rsp.op_errno = op_errno;
                if (op_errstr)
                        rsp.op_errstr = op_errstr;
                else
                        rsp.op_errstr = "";
                ctx = op_ctx;
                dict_allocate_and_serialize (ctx,
                             &rsp.stats_info.stats_info_val,
                        (size_t*)&rsp.stats_info.stats_info_len);
                free_ptr = rsp.stats_info.stats_info_val;
                cli_rsp = &rsp;
                sfunc = gf_xdr_from_cli_stats_volume_rsp;
                break;
        }

        case GD_OP_QUOTA:
        {
                int32_t               type;
                char                 *str    = NULL;
                char                 *errstr = NULL;
                gf1_cli_quota_rsp     rsp    = {0,};

                rsp.op_ret = op_ret;
                rsp.op_errno = op_errno;
                rsp.volname = "";

                ctx = op_ctx;

                if (op_errstr)
                        rsp.op_errstr = op_errstr;
                else {
                        ret = dict_get_str (ctx, "errstr", &errstr);
                        if (ret == 0)
                                rsp.op_errstr = errstr;
                        else
                                rsp.op_errstr = "";
                }

                rsp.limit_list = "";

                if (op_ret == 0 && ctx) {
                        ret = dict_get_str (ctx, "volname", &str);
                        if (ret == 0)
                                rsp.volname = str;

                        ret = dict_get_int32 (ctx, "type", &type);
                        if (ret == 0)
                                rsp.type = type;
                        else
                                rsp.type = 0;

                        if (type == GF_QUOTA_OPTION_TYPE_LIST) {
                                 ret = dict_get_str (ctx,"limit_list", &str);

                                 if (ret == 0)
                                         rsp.limit_list = str;
                        }
                }
                cli_rsp = &rsp;
                sfunc = gf_xdr_serialize_cli_quota_rsp;
                break;
        }

        case GD_OP_NONE:
        case GD_OP_MAX:
        {
                gf_log ("", GF_LOG_ERROR, "invalid operation %d", op);
                break;
        }
        }

        ret = glusterd_submit_reply (req, cli_rsp, NULL, 0, NULL,
                                     sfunc);

        if (free_ptr)
                GF_FREE (free_ptr);
        gf_log ("", GF_LOG_DEBUG, "Returning %d", ret);
        return ret;
}

int
glusterd3_1_probe_cbk (struct rpc_req *req, struct iovec *iov,
                        int count, void *myframe)
{
        gd1_mgmt_probe_rsp    rsp   = {{0},};
        glusterd_conf_t       *conf = NULL;
        int                   ret   = 0;
        glusterd_peerinfo_t           *peerinfo = NULL;
        glusterd_friend_sm_event_t    *event = NULL;
        glusterd_probe_ctx_t          *ctx = NULL;

        conf  = THIS->private;

        if (-1 == req->rpc_status) {
                goto out;
        }

        ret = gd_xdr_to_mgmt_probe_rsp (*iov, &rsp);
        if (ret < 0) {
                gf_log ("", GF_LOG_ERROR, "error");
                //rsp.op_ret   = -1;
                //rsp.op_errno = EINVAL;
                goto out;
        }

        gf_log ("glusterd", GF_LOG_INFO,
                "Received probe resp from uuid: %s, host: %s",
                uuid_utoa (rsp.uuid), rsp.hostname);
        if (rsp.op_ret != 0) {
                ctx = ((call_frame_t *)myframe)->local;
                ((call_frame_t *)myframe)->local = NULL;

                GF_ASSERT (ctx);

                if (ctx->req) {
                        glusterd_xfer_cli_probe_resp (ctx->req, rsp.op_ret,
                                                      rsp.op_errno,
                                                      ctx->hostname, ctx->port);
                }

                glusterd_destroy_probe_ctx (ctx);
                (void) glusterd_friend_remove (rsp.uuid, rsp.hostname);
                ret = rsp.op_ret;
                goto out;
        }
        ret = glusterd_friend_find (rsp.uuid, rsp.hostname, &peerinfo);
        if (ret) {
                GF_ASSERT (0);
        }

        uuid_copy (peerinfo->uuid, rsp.uuid);

        ret = glusterd_friend_sm_new_event
                        (GD_FRIEND_EVENT_INIT_FRIEND_REQ, &event);

        if (ret) {
                gf_log ("glusterd", GF_LOG_ERROR,
                         "Unable to get event");
                goto out;
        }

        event->peerinfo = peerinfo;
        event->ctx      = ((call_frame_t *)myframe)->local;
        ((call_frame_t *)myframe)->local = NULL;
        ret = glusterd_friend_sm_inject_event (event);


        if (!ret) {
                glusterd_friend_sm ();
                glusterd_op_sm ();
        }

        gf_log ("glusterd", GF_LOG_INFO, "Received resp to probe req");

out:
        if (rsp.hostname)
                free (rsp.hostname);//malloced by xdr
        GLUSTERD_STACK_DESTROY (((call_frame_t *)myframe));
        return ret;
}

int
glusterd3_1_friend_add_cbk (struct rpc_req * req, struct iovec *iov,
                            int count, void *myframe)
{
        gd1_mgmt_friend_rsp           rsp   = {{0},};
        glusterd_conf_t               *conf = NULL;
        int                           ret   = -1;
        glusterd_friend_sm_event_t        *event = NULL;
        glusterd_friend_sm_event_type_t    event_type = GD_FRIEND_EVENT_NONE;
        glusterd_peerinfo_t           *peerinfo = NULL;
        int32_t                       op_ret = -1;
        int32_t                       op_errno = -1;
        glusterd_probe_ctx_t          *ctx = NULL;
        glusterd_friend_update_ctx_t  *ev_ctx = NULL;

        conf  = THIS->private;

        if (-1 == req->rpc_status) {
                rsp.op_ret   = -1;
                rsp.op_errno = EINVAL;
                goto out;
        }

        ret = gd_xdr_to_mgmt_friend_rsp (*iov, &rsp);
        if (ret < 0) {
                gf_log ("", GF_LOG_ERROR, "error");
                rsp.op_ret   = -1;
                rsp.op_errno = EINVAL;
                goto out;
        }

        op_ret = rsp.op_ret;
        op_errno = rsp.op_errno;

        gf_log ("glusterd", GF_LOG_INFO,
                "Received %s from uuid: %s, host: %s, port: %d",
                (op_ret)?"RJT":"ACC", uuid_utoa (rsp.uuid), rsp.hostname, rsp.port);

        ret = glusterd_friend_find (rsp.uuid, rsp.hostname, &peerinfo);

        if (ret) {
                gf_log ("", GF_LOG_ERROR, "received friend add response from"
                        " unknown peer uuid: %s", uuid_utoa (rsp.uuid));
                goto out;
        }

        if (op_ret)
                event_type = GD_FRIEND_EVENT_RCVD_RJT;
        else
                event_type = GD_FRIEND_EVENT_RCVD_ACC;

        ret = glusterd_friend_sm_new_event (event_type, &event);

        if (ret) {
                gf_log ("glusterd", GF_LOG_ERROR,
                         "Unable to get event");
                goto out;
        }
        event->peerinfo = peerinfo;
        ev_ctx = GF_CALLOC (1, sizeof (*ev_ctx),
                                gf_gld_mt_friend_update_ctx_t);
        if (!ev_ctx) {
                ret = -1;
                goto out;
        }

        uuid_copy (ev_ctx->uuid, rsp.uuid);
        ev_ctx->hostname = gf_strdup (rsp.hostname);

        event->ctx = ev_ctx;
        ret = glusterd_friend_sm_inject_event (event);

        if (ret)
                goto out;

out:
        ctx = ((call_frame_t *)myframe)->local;
        ((call_frame_t *)myframe)->local = NULL;

        GF_ASSERT (ctx);

        if (ctx->req)//reverse probe doesnt have req
                ret = glusterd_xfer_cli_probe_resp (ctx->req, op_ret, op_errno,
                                                    ctx->hostname, ctx->port);
        if (!ret) {
                glusterd_friend_sm ();
                glusterd_op_sm ();
        }
        if (ctx)
                glusterd_destroy_probe_ctx (ctx);
        if (rsp.hostname)
                free (rsp.hostname);//malloced by xdr
        GLUSTERD_STACK_DESTROY (((call_frame_t *)myframe));
        return ret;
}

int
glusterd3_1_friend_remove_cbk (struct rpc_req * req, struct iovec *iov,
                               int count, void *myframe)
{
        gd1_mgmt_friend_rsp             rsp   = {{0},};
        glusterd_conf_t                 *conf = NULL;
        int                             ret   = -1;
        glusterd_friend_sm_event_t      *event = NULL;
        glusterd_friend_sm_event_type_t event_type = GD_FRIEND_EVENT_NONE;
        glusterd_peerinfo_t             *peerinfo = NULL;
        int32_t                         op_ret = -1;
        int32_t                         op_errno = -1;
        glusterd_probe_ctx_t            *ctx = NULL;

        conf  = THIS->private;
        GF_ASSERT (conf);

        ctx = ((call_frame_t *)myframe)->local;
        ((call_frame_t *)myframe)->local = NULL;
        GF_ASSERT (ctx);

        if (-1 == req->rpc_status) {
                rsp.op_ret   = -1;
                rsp.op_errno = EINVAL;
                goto inject;
        }

        ret = gd_xdr_to_mgmt_friend_rsp (*iov, &rsp);
        if (ret < 0) {
                gf_log ("", GF_LOG_ERROR, "error");
                rsp.op_ret   = -1;
                rsp.op_errno = EINVAL;
                goto respond;
        }

        op_ret = rsp.op_ret;
        op_errno = rsp.op_errno;

        gf_log ("glusterd", GF_LOG_INFO,
                "Received %s from uuid: %s, host: %s, port: %d",
                (op_ret)?"RJT":"ACC", uuid_utoa (rsp.uuid), rsp.hostname, rsp.port);

inject:
        ret = glusterd_friend_find (rsp.uuid, ctx->hostname, &peerinfo);

        if (ret) {
                //can happen as part of rpc clnt connection cleanup
                //when the frame timeout happens after 30 minutes
                goto respond;
        }

        event_type = GD_FRIEND_EVENT_REMOVE_FRIEND;

        ret = glusterd_friend_sm_new_event (event_type, &event);

        if (ret) {
                gf_log ("glusterd", GF_LOG_ERROR,
                         "Unable to get event");
                goto respond;
        }
        event->peerinfo = peerinfo;

        ret = glusterd_friend_sm_inject_event (event);

        if (ret)
                goto respond;

        glusterd_friend_sm ();
        glusterd_op_sm ();

        op_ret = 0;


respond:
        ret = glusterd_xfer_cli_deprobe_resp (ctx->req, op_ret, op_errno,
                                              ctx->hostname);
        if (!ret) {
                glusterd_friend_sm ();
                glusterd_op_sm ();
        }

        if (ctx) {
                glusterd_broadcast_friend_delete (ctx->hostname, NULL);
                glusterd_destroy_probe_ctx (ctx);
        }

        if (rsp.hostname)
                free (rsp.hostname);//malloced by xdr
        GLUSTERD_STACK_DESTROY (((call_frame_t *)myframe));
        return ret;
}

int32_t
glusterd3_1_friend_update_cbk (struct rpc_req *req, struct iovec *iov,
                              int count, void *myframe)
{
        gd1_mgmt_cluster_lock_rsp     rsp   = {{0},};
        int                           ret   = -1;
        int32_t                       op_ret = 0;
        char                          str[50] = {0,};

        GF_ASSERT (req);

        if (-1 == req->rpc_status) {
                rsp.op_ret   = -1;
                rsp.op_errno = EINVAL;
                goto out;
        }

/*        ret = gd_xdr_to_mgmt_friend_update_rsp (*iov, &rsp);
        if (ret < 0) {
                gf_log ("", GF_LOG_ERROR, "error");
                rsp.op_ret   = -1;
                rsp.op_errno = EINVAL;
                goto out;
        }
        uuid_unparse (rsp.uuid, str);

        op_ret = rsp.op_ret;
*/
        gf_log ("glusterd", GF_LOG_INFO,
                "Received %s from uuid: %s",
                (op_ret)?"RJT":"ACC", str);

out:
        GLUSTERD_STACK_DESTROY (((call_frame_t *)myframe));
        return ret;
}

int32_t
glusterd3_1_cluster_lock_cbk (struct rpc_req *req, struct iovec *iov,
                              int count, void *myframe)
{
        gd1_mgmt_cluster_lock_rsp     rsp   = {{0},};
        int                           ret   = -1;
        int32_t                       op_ret = -1;
        glusterd_op_sm_event_type_t   event_type = GD_OP_EVENT_NONE;
        glusterd_peerinfo_t           *peerinfo = NULL;

        GF_ASSERT (req);

        if (-1 == req->rpc_status) {
                rsp.op_ret   = -1;
                rsp.op_errno = EINVAL;
                goto out;
        }

        ret = gd_xdr_to_mgmt_cluster_lock_rsp (*iov, &rsp);
        if (ret < 0) {
                gf_log ("", GF_LOG_ERROR, "error");
                rsp.op_ret   = -1;
                rsp.op_errno = EINVAL;
                goto out;
        }

        op_ret = rsp.op_ret;

        gf_log ("glusterd", GF_LOG_INFO,
                "Received %s from uuid: %s",
                (op_ret)?"RJT":"ACC", uuid_utoa (rsp.uuid));

        ret = glusterd_friend_find (rsp.uuid, NULL, &peerinfo);

        if (ret) {
                gf_log ("", GF_LOG_CRITICAL, "Lock response received from "
                        "unknown peer: %s", uuid_utoa (rsp.uuid));
        }

        if (op_ret) {
                event_type = GD_OP_EVENT_RCVD_RJT;
                opinfo.op_ret = op_ret;
        } else {
                event_type = GD_OP_EVENT_RCVD_ACC;
        }

        ret = glusterd_op_sm_inject_event (event_type, NULL);

        if (!ret) {
                glusterd_friend_sm ();
                glusterd_op_sm ();
        }

out:
        GLUSTERD_STACK_DESTROY (((call_frame_t *)myframe));
        return ret;
}

int32_t
glusterd3_1_cluster_unlock_cbk (struct rpc_req *req, struct iovec *iov,
                                 int count, void *myframe)
{
        gd1_mgmt_cluster_lock_rsp     rsp   = {{0},};
        int                           ret   = -1;
        int32_t                       op_ret = -1;
        glusterd_op_sm_event_type_t   event_type = GD_OP_EVENT_NONE;
        glusterd_peerinfo_t           *peerinfo = NULL;


        GF_ASSERT (req);

        if (-1 == req->rpc_status) {
                rsp.op_ret   = -1;
                rsp.op_errno = EINVAL;
                goto out;
        }

        ret = gd_xdr_to_mgmt_cluster_unlock_rsp (*iov, &rsp);
        if (ret < 0) {
                gf_log ("", GF_LOG_ERROR, "error");
                rsp.op_ret   = -1;
                rsp.op_errno = EINVAL;
                goto out;
        }

        op_ret = rsp.op_ret;

        gf_log ("glusterd", GF_LOG_INFO,
                "Received %s from uuid: %s",
                (op_ret)?"RJT":"ACC", uuid_utoa (rsp.uuid));

        ret = glusterd_friend_find (rsp.uuid, NULL, &peerinfo);

        if (ret) {
                gf_log ("", GF_LOG_CRITICAL, "Unlock response received from "
                        "unknown peer %s", uuid_utoa (rsp.uuid));
        }

        if (op_ret) {
                event_type = GD_OP_EVENT_RCVD_RJT;
                opinfo.op_ret = op_ret;
        } else {
                event_type = GD_OP_EVENT_RCVD_ACC;
        }

        ret = glusterd_op_sm_inject_event (event_type, NULL);

        if (!ret) {
                glusterd_friend_sm ();
                glusterd_op_sm ();
        }

out:
        GLUSTERD_STACK_DESTROY (((call_frame_t *)myframe));
        return ret;
}
static int32_t
glusterd_append_status_dicts (dict_t *dst, dict_t *src)
{
        int              dst_count = 0;
        int              src_count = 0;
        int              i = 0;
        int              ret = 0;
        char             mst[PATH_MAX] = {0,};
        char             slv[PATH_MAX] = {0, };
        char             sts[PATH_MAX] = {0, };
        char             *mst_val = NULL;
        char             *slv_val = NULL;
        char             *sts_val = NULL;

        GF_ASSERT (dst);

        if (src == NULL)
                goto out;

        ret = dict_get_int32 (dst, "gsync-count", &dst_count);
        if (ret)
                dst_count = 0;

        ret = dict_get_int32 (src, "gsync-count", &src_count);
        if (ret || !src_count) {
                gf_log ("", GF_LOG_DEBUG, "Source brick empty");
                ret = 0;
                goto out;
        }

        for (i = 1; i <= src_count; i++) {
                snprintf (mst, sizeof(mst), "master%d", i);
                snprintf (slv, sizeof(slv), "slave%d", i);
                snprintf (sts, sizeof(sts), "status%d", i);

                ret = dict_get_str (src, mst, &mst_val);
                if (ret)
                        goto out;

                ret = dict_get_str (src, slv, &slv_val);
                if (ret)
                        goto out;

                ret = dict_get_str (src, sts, &sts_val);
                if (ret)
                        goto out;

                snprintf (mst, sizeof(mst), "master%d", i+dst_count);
                snprintf (slv, sizeof(slv), "slave%d", i+dst_count);
                snprintf (sts, sizeof(sts), "status%d", i+dst_count);

                ret = dict_set_dynstr (dst, mst, gf_strdup (mst_val));
                if (ret)
                        goto out;

                ret = dict_set_dynstr (dst, slv, gf_strdup (slv_val));
                if (ret)
                        goto out;

                ret = dict_set_dynstr (dst, sts, gf_strdup (sts_val));
                if (ret)
                        goto out;

        }

        ret = dict_set_int32 (dst, "gsync-count", dst_count+src_count);

 out:
        gf_log ("", GF_LOG_DEBUG, "Returning %d", ret);
        return ret;

}

static int32_t
glusterd_gsync_use_rsp_dict (dict_t *rsp_dict, char *op_errstr)
{
        dict_t             *ctx = NULL;
        int                ret = 0;

        ctx = glusterd_op_get_ctx (GD_OP_GSYNC_SET);
        if (!ctx) {
                gf_log ("", GF_LOG_ERROR,
                        "Operation Context is not present");
                GF_ASSERT (0);
        }

        if (rsp_dict) {
                ret = glusterd_append_status_dicts (ctx, rsp_dict);
                if (ret)
                        goto out;
        }
        if (strcmp ("", op_errstr)) {
                ret = dict_set_dynstr (ctx, "errstr", gf_strdup(op_errstr));
                if (ret)
                        goto out;
        }

        ret = 0;
 out:
        gf_log ("", GF_LOG_DEBUG, "Returning %d ", ret);
        return ret;
}
static int32_t
glusterd_rb_use_rsp_dict (dict_t *rsp_dict)
{
        int32_t  src_port = 0;
        int32_t  dst_port = 0;
        int      ret      = 0;
        dict_t  *ctx      = NULL;


        ctx = glusterd_op_get_ctx (GD_OP_REPLACE_BRICK);
        if (!ctx) {
                gf_log ("", GF_LOG_ERROR,
                        "Operation Context is not present");
                GF_ASSERT (0);
        }

        if (rsp_dict) {
                ret = dict_get_int32 (rsp_dict, "src-brick-port", &src_port);
                if (ret == 0) {
                        gf_log ("", GF_LOG_DEBUG,
                                "src-brick-port=%d found", src_port);
                }

                ret = dict_get_int32 (rsp_dict, "dst-brick-port", &dst_port);
                if (ret == 0) {
                        gf_log ("", GF_LOG_DEBUG,
                                "dst-brick-port=%d found", dst_port);
                }

        }

        if (src_port) {
                ret = dict_set_int32 (ctx, "src-brick-port",
                                      src_port);
                if (ret) {
                        gf_log ("", GF_LOG_DEBUG,
                                "Could not set src-brick");
                        goto out;
                }
        }

        if (dst_port) {
                ret = dict_set_int32 (ctx, "dst-brick-port",
                                      dst_port);
                if (ret) {
                        gf_log ("", GF_LOG_DEBUG,
                                "Could not set dst-brick");
                        goto out;
                }

        }

out:
        return ret;

}

int32_t
glusterd3_1_stage_op_cbk (struct rpc_req *req, struct iovec *iov,
                          int count, void *myframe)
{
        gd1_mgmt_stage_op_rsp         rsp   = {{0},};
        int                           ret   = -1;
        int32_t                       op_ret = -1;
        glusterd_op_sm_event_type_t   event_type = GD_OP_EVENT_NONE;
        glusterd_peerinfo_t           *peerinfo = NULL;
        dict_t                        *dict   = NULL;
        char                          err_str[2048] = {0};
        char                          *peer_str = NULL;

        GF_ASSERT (req);

        if (-1 == req->rpc_status) {
                rsp.op_ret   = -1;
                rsp.op_errno = EINVAL;
                rsp.op_errstr = "error";
                goto out;
        }

        ret = gd_xdr_to_mgmt_stage_op_rsp (*iov, &rsp);
        if (ret < 0) {
                gf_log ("", GF_LOG_ERROR, "error");
                rsp.op_ret   = -1;
                rsp.op_errno = EINVAL;
                rsp.op_errstr = "error";
                goto out;
        }

        if (rsp.dict.dict_len) {
                /* Unserialize the dictionary */
                dict  = dict_new ();

                ret = dict_unserialize (rsp.dict.dict_val,
                                        rsp.dict.dict_len,
                                        &dict);
                if (ret < 0) {
                        gf_log ("glusterd", GF_LOG_ERROR,
                                "failed to "
                                "unserialize rsp-buffer to dictionary");
			event_type = GD_OP_EVENT_RCVD_RJT;
                        goto out;
                } else {
                        dict->extra_stdfree = rsp.dict.dict_val;
                }
        }

        op_ret = rsp.op_ret;

        gf_log ("glusterd", GF_LOG_INFO,
                "Received %s from uuid: %s",
                (op_ret)?"RJT":"ACC", uuid_utoa (rsp.uuid));

        ret = glusterd_friend_find (rsp.uuid, NULL, &peerinfo);

        if (ret) {
                gf_log ("", GF_LOG_CRITICAL, "Stage response received from "
                        "unknown peer: %s", uuid_utoa (rsp.uuid));
        }

        if (op_ret) {
                event_type = GD_OP_EVENT_RCVD_RJT;
                opinfo.op_ret = op_ret;
                if (strcmp ("", rsp.op_errstr)) {
                        opinfo.op_errstr = gf_strdup (rsp.op_errstr);
                } else {
                        if (peerinfo)
                                peer_str = peerinfo->hostname;
                        else
                                peer_str = uuid_utoa (rsp.uuid);
                        snprintf (err_str, sizeof (err_str), "Operation failed "
                                  "on %s", peer_str);
                        opinfo.op_errstr = gf_strdup (err_str);
                }
                if (!opinfo.op_errstr) {
                        gf_log ("", GF_LOG_ERROR, "memory allocation failed");
                        ret = -1;
                        goto out;
                }
        } else {
                event_type = GD_OP_EVENT_RCVD_ACC;
        }

        switch (rsp.op) {
        case GD_OP_REPLACE_BRICK:
                glusterd_rb_use_rsp_dict (dict);
                break;
        }

        ret = glusterd_op_sm_inject_event (event_type, NULL);

        if (!ret) {
                glusterd_friend_sm ();
                glusterd_op_sm ();
        }

out:
        if (rsp.op_errstr && strcmp (rsp.op_errstr, "error"))
                free (rsp.op_errstr); //malloced by xdr
        if (dict) {
                if (!dict->extra_stdfree && rsp.dict.dict_val)
                        free (rsp.dict.dict_val); //malloced by xdr
                dict_unref (dict);
        } else {
                if (rsp.dict.dict_val)
                        free (rsp.dict.dict_val); //malloced by xdr
        }
        GLUSTERD_STACK_DESTROY (((call_frame_t *)myframe));
        return ret;
}

static int32_t
glusterd_sync_use_rsp_dict (dict_t *rsp_dict)
{
        int      ret      = 0;

        GF_ASSERT (rsp_dict);

        if (!rsp_dict) {
                goto out;
        }

        ret = glusterd_import_friend_volumes (rsp_dict);
out:
        return ret;

}

void
_profile_volume_add_friend_rsp (dict_t *this, char *key, data_t *value,
                               void *data)
{
        char    new_key[256] = {0};
        glusterd_pr_brick_rsp_conv_t *rsp_ctx = NULL;
        data_t  *new_value = NULL;
        int     brick_count = 0;
        char    brick_key[256];

        if (strcmp (key, "count") == 0)
                return;
        sscanf (key, "%d%s", &brick_count, brick_key);
        rsp_ctx = data;
        new_value = data_copy (value);
        GF_ASSERT (new_value);
        snprintf (new_key, sizeof (new_key), "%d%s",
                  rsp_ctx->count + brick_count, brick_key);
        dict_set (rsp_ctx->dict, new_key, new_value);
}

int
glusterd_profile_volume_use_rsp_dict (dict_t *rsp_dict)
{
        int     ret = 0;
        glusterd_pr_brick_rsp_conv_t rsp_ctx = {0};
        int32_t brick_count = 0;
        int32_t count = 0;
        dict_t  *ctx_dict = NULL;
        glusterd_op_t   op = GD_OP_NONE;

        GF_ASSERT (rsp_dict);

        ret = dict_get_int32 (rsp_dict, "count", &brick_count);
        if (ret) {
                ret = 0; //no bricks in the rsp
                goto out;
        }

        op = glusterd_op_get_op ();
        GF_ASSERT (GD_OP_PROFILE_VOLUME == op);
        ctx_dict = glusterd_op_get_ctx (op);

        ret = dict_get_int32 (ctx_dict, "count", &count);
        rsp_ctx.count = count;
        rsp_ctx.dict = ctx_dict;
        dict_foreach (rsp_dict, _profile_volume_add_friend_rsp, &rsp_ctx);
        dict_del (ctx_dict, "count");
        ret = dict_set_int32 (ctx_dict, "count", count + brick_count);
out:
        return ret;
}

int32_t
glusterd3_1_commit_op_cbk (struct rpc_req *req, struct iovec *iov,
                          int count, void *myframe)
{
        gd1_mgmt_commit_op_rsp         rsp   = {{0},};
        int                           ret   = -1;
        int32_t                       op_ret = -1;
        glusterd_op_sm_event_type_t   event_type = GD_OP_EVENT_NONE;
        glusterd_peerinfo_t           *peerinfo = NULL;
        dict_t                        *dict = NULL;
        char                          err_str[2048] = {0};
        char                          *peer_str = NULL;


        GF_ASSERT (req);

        if (-1 == req->rpc_status) {
                rsp.op_ret   = -1;
                rsp.op_errno = EINVAL;
                rsp.op_errstr = "error";
		event_type = GD_OP_EVENT_RCVD_RJT;
                goto out;
        }

        ret = gd_xdr_to_mgmt_commit_op_rsp (*iov, &rsp);
        if (ret < 0) {
                gf_log ("", GF_LOG_ERROR, "error");
                rsp.op_ret   = -1;
                rsp.op_errno = EINVAL;
                rsp.op_errstr = "error";
		event_type = GD_OP_EVENT_RCVD_RJT;
                goto out;
        }

        if (rsp.dict.dict_len) {
                /* Unserialize the dictionary */
                dict  = dict_new ();

                ret = dict_unserialize (rsp.dict.dict_val,
                                        rsp.dict.dict_len,
                                        &dict);
                if (ret < 0) {
                        gf_log ("glusterd", GF_LOG_ERROR,
                                "failed to "
                                "unserialize rsp-buffer to dictionary");
			event_type = GD_OP_EVENT_RCVD_RJT;
                        goto out;
                } else {
                        dict->extra_stdfree = rsp.dict.dict_val;
                }
        }

        op_ret = rsp.op_ret;

        gf_log ("glusterd", GF_LOG_INFO,
                "Received %s from uuid: %s",
                (op_ret)?"RJT":"ACC", uuid_utoa (rsp.uuid));

        ret = glusterd_friend_find (rsp.uuid, NULL, &peerinfo);

        if (ret) {
                gf_log ("", GF_LOG_CRITICAL, "Commit response received from "
                        "unknown peer: %s", uuid_utoa (rsp.uuid));
        }

        if (op_ret) {
                event_type = GD_OP_EVENT_RCVD_RJT;
                opinfo.op_ret = op_ret;
                if (strcmp ("", rsp.op_errstr)) {
                        opinfo.op_errstr = gf_strdup(rsp.op_errstr);
                } else {
                        if (peerinfo)
                                peer_str = peerinfo->hostname;
                        else
                                peer_str = uuid_utoa (rsp.uuid);
                        snprintf (err_str, sizeof (err_str), "Operation failed "
                                  "on %s", peer_str);
                        opinfo.op_errstr = gf_strdup (err_str);
                }
                if (!opinfo.op_errstr) {
                        gf_log ("", GF_LOG_ERROR, "memory allocation failed");
                        ret = -1;
                        goto out;
                }
        } else {
                event_type = GD_OP_EVENT_RCVD_ACC;
                switch (rsp.op) {
                case GD_OP_REPLACE_BRICK:
                        ret = glusterd_rb_use_rsp_dict (dict);
                        if (ret)
                                goto out;
                break;

                case GD_OP_SYNC_VOLUME:
                        ret = glusterd_sync_use_rsp_dict (dict);
                        if (ret)
                                goto out;
                break;

                case GD_OP_PROFILE_VOLUME:
                        ret = glusterd_profile_volume_use_rsp_dict (dict);
                        if (ret)
                                goto out;
                break;

                case GD_OP_GSYNC_SET:
                        ret = glusterd_gsync_use_rsp_dict (dict, rsp.op_errstr);
                        if (ret)
                                goto out;
                break;

                default:
                break;
                }
        }

out:
        ret = glusterd_op_sm_inject_event (event_type, NULL);

        if (!ret) {
                glusterd_friend_sm ();
                glusterd_op_sm ();
        }

        if (dict)
                dict_unref (dict);
        if (rsp.op_errstr && strcmp (rsp.op_errstr, "error"))
                free (rsp.op_errstr); //malloced by xdr
        GLUSTERD_STACK_DESTROY (((call_frame_t *)myframe));
        return ret;
}



int32_t
glusterd3_1_probe (call_frame_t *frame, xlator_t *this,
                   void *data)
{
        gd1_mgmt_probe_req      req = {{0},};
        int                     ret = 0;
        int                     port = 0;
        char                    *hostname = NULL;
        glusterd_peerinfo_t     *peerinfo = NULL;
        glusterd_conf_t         *priv = NULL;
        dict_t                  *dict = NULL;

        if (!frame || !this ||  !data) {
                ret = -1;
                goto out;
        }

        dict = data;
        priv = this->private;

        GF_ASSERT (priv);
        ret = dict_get_str (dict, "hostname", &hostname);
        if (ret)
                goto out;
        ret = dict_get_int32 (dict, "port", &port);
        if (ret)
                port = GF_DEFAULT_BASE_PORT;

        ret = dict_get_ptr (dict, "peerinfo", VOID (&peerinfo));
        if (ret)
                goto out;

        uuid_copy (req.uuid, priv->uuid);
        req.hostname = gf_strdup (hostname);
        req.port = port;

        ret = glusterd_submit_request (peerinfo->rpc, &req, frame, peerinfo->mgmt,
                                       GD_MGMT_PROBE_QUERY,
                                       NULL, gd_xdr_from_mgmt_probe_req,
                                       this, glusterd3_1_probe_cbk);

out:
        if (req.hostname)
                GF_FREE (req.hostname);
        gf_log ("glusterd", GF_LOG_DEBUG, "Returning %d", ret);
        return ret;
}


int32_t
glusterd3_1_friend_add (call_frame_t *frame, xlator_t *this,
                        void *data)
{
        gd1_mgmt_friend_req         req      = {{0},};
        int                         ret      = 0;
        glusterd_peerinfo_t        *peerinfo = NULL;
        glusterd_conf_t            *priv     = NULL;
        glusterd_friend_sm_event_t *event    = NULL;
        glusterd_friend_req_ctx_t  *ctx      = NULL;
        dict_t                     *vols     = NULL;


        if (!frame || !this || !data) {
                ret = -1;
                goto out;
        }

        event = data;
        priv = this->private;

        GF_ASSERT (priv);

        ctx = event->ctx;

        peerinfo = event->peerinfo;

        ret = glusterd_build_volume_dict (&vols);
        if (ret)
                goto out;

        uuid_copy (req.uuid, priv->uuid);
        req.hostname = peerinfo->hostname;
        req.port = peerinfo->port;

        ret = dict_allocate_and_serialize (vols, &req.vols.vols_val,
                                           (size_t *)&req.vols.vols_len);
        if (ret)
                goto out;

        ret = glusterd_submit_request (peerinfo->rpc, &req, frame, peerinfo->mgmt,
                                       GD_MGMT_FRIEND_ADD,
                                       NULL, gd_xdr_from_mgmt_friend_req,
                                       this, glusterd3_1_friend_add_cbk);


out:
        if (req.vols.vols_val)
                GF_FREE (req.vols.vols_val);

        if (vols)
                dict_unref (vols);

        gf_log ("glusterd", GF_LOG_DEBUG, "Returning %d", ret);
        return ret;
}

int32_t
glusterd3_1_friend_remove (call_frame_t *frame, xlator_t *this,
                           void *data)
{
        gd1_mgmt_friend_req             req = {{0},};
        int                             ret = 0;
        glusterd_peerinfo_t             *peerinfo = NULL;
        glusterd_conf_t                 *priv = NULL;
        glusterd_friend_sm_event_t      *event = NULL;

        if (!frame || !this || !data) {
                ret = -1;
                goto out;
        }

        event = data;
        priv = this->private;

        GF_ASSERT (priv);

        peerinfo = event->peerinfo;

        uuid_copy (req.uuid, priv->uuid);
        req.hostname = peerinfo->hostname;
        req.port = peerinfo->port;
        ret = glusterd_submit_request (peerinfo->rpc, &req, frame, peerinfo->mgmt,
                                       GD_MGMT_FRIEND_REMOVE,
                                       NULL, gd_xdr_from_mgmt_friend_req,
                                       this, glusterd3_1_friend_remove_cbk);

out:
        gf_log ("glusterd", GF_LOG_DEBUG, "Returning %d", ret);
        return ret;
}


int32_t
glusterd3_1_friend_update (call_frame_t *frame, xlator_t *this,
                           void *data)
{
        gd1_mgmt_friend_update  req         = {{0},};
        int                     ret         = 0;
        glusterd_conf_t        *priv        = NULL;
        dict_t                 *friends     = NULL;
        char                   *dict_buf    = NULL;
        size_t                  len         = -1;
        call_frame_t           *dummy_frame = NULL;
        glusterd_peerinfo_t    *peerinfo    = NULL;

        priv = this->private;
        GF_ASSERT (priv);

        friends = data;
        if (!friends)
                goto out;

        ret = dict_get_ptr (friends, "peerinfo", VOID(&peerinfo));
        if (ret)
                goto out;

        ret = dict_allocate_and_serialize (friends, &dict_buf, (size_t *)&len);
        if (ret)
                goto out;

        req.friends.friends_val = dict_buf;
        req.friends.friends_len = len;

        uuid_copy (req.uuid, priv->uuid);

        dummy_frame = create_frame (this, this->ctx->pool);
        ret = glusterd_submit_request (peerinfo->rpc, &req, dummy_frame,
                                       peerinfo->mgmt,
                                       GD_MGMT_FRIEND_UPDATE,
                                       NULL, gd_xdr_from_mgmt_friend_update,
                                       this, glusterd3_1_friend_update_cbk);

out:
        if (req.friends.friends_val)
                GF_FREE (req.friends.friends_val);

        gf_log ("glusterd", GF_LOG_DEBUG, "Returning %d", ret);
        return ret;
}

int32_t
glusterd3_1_cluster_lock (call_frame_t *frame, xlator_t *this,
                          void *data)
{
        gd1_mgmt_cluster_lock_req       req = {{0},};
        int                             ret = -1;
        glusterd_peerinfo_t             *peerinfo = NULL;
        glusterd_conf_t                 *priv = NULL;
        call_frame_t                    *dummy_frame = NULL;

        if (!this)
                goto out;

        peerinfo = data;

        priv = this->private;
        GF_ASSERT (priv);

        glusterd_get_uuid (&req.uuid);

        dummy_frame = create_frame (this, this->ctx->pool);
        if (!dummy_frame)
                goto out;

        ret = glusterd_submit_request (peerinfo->rpc, &req, dummy_frame,
                                       peerinfo->mgmt, GD_MGMT_CLUSTER_LOCK,
                                       NULL,
                                       gd_xdr_from_mgmt_cluster_lock_req,
                                       this, glusterd3_1_cluster_lock_cbk);
out:
        gf_log ("glusterd", GF_LOG_DEBUG, "Returning %d", ret);
        return ret;
}

int32_t
glusterd3_1_cluster_unlock (call_frame_t *frame, xlator_t *this,
                            void *data)
{
        gd1_mgmt_cluster_lock_req       req = {{0},};
        int                             ret = -1;
        glusterd_peerinfo_t             *peerinfo = NULL;
        glusterd_conf_t                 *priv = NULL;
        call_frame_t                    *dummy_frame = NULL;

        if (!this ) {
                ret = -1;
                goto out;
        }
        peerinfo = data;
        priv = this->private;
        GF_ASSERT (priv);

        glusterd_get_uuid (&req.uuid);

        dummy_frame = create_frame (this, this->ctx->pool);
        if (!dummy_frame)
                goto out;

        ret = glusterd_submit_request (peerinfo->rpc, &req, dummy_frame,
                                       peerinfo->mgmt, GD_MGMT_CLUSTER_UNLOCK,
                                       NULL,
                                       gd_xdr_from_mgmt_cluster_unlock_req,
                                       this, glusterd3_1_cluster_unlock_cbk);
out:
        gf_log ("glusterd", GF_LOG_DEBUG, "Returning %d", ret);
        return ret;
}

int32_t
glusterd3_1_stage_op (call_frame_t *frame, xlator_t *this,
                      void *data)
{
        gd1_mgmt_stage_op_req           req = {{0,},};
        int                             ret = -1;
        glusterd_peerinfo_t             *peerinfo = NULL;
        glusterd_conf_t                 *priv = NULL;
        call_frame_t                    *dummy_frame = NULL;
        dict_t                          *dict = NULL;
        gf_boolean_t                    is_alloc = _gf_true;

        if (!this) {
                goto out;
        }

        dict = data;

        priv = this->private;
        GF_ASSERT (priv);

        ret = dict_get_ptr (dict, "peerinfo", VOID (&peerinfo));
        if (ret)
                goto out;

        //peerinfo should not be in payload
        dict_del (dict, "peerinfo");

        glusterd_get_uuid (&req.uuid);
        req.op = glusterd_op_get_op ();

        if (GD_OP_DELETE_VOLUME == req.op) {
                ret = dict_get_str (dict, "volname", &req.buf.buf_val);
                if (ret)
                        goto out;
                req.buf.buf_len = strlen (req.buf.buf_val);
                is_alloc = _gf_false;
        } else {
                ret = dict_allocate_and_serialize (dict, &req.buf.buf_val,
                                                   (size_t *)&req.buf.buf_len);

                if (ret)
                        goto out;
        }

        dummy_frame = create_frame (this, this->ctx->pool);
        if (!dummy_frame)
                goto out;

        ret = glusterd_submit_request (peerinfo->rpc, &req, dummy_frame,
                                       peerinfo->mgmt, GD_MGMT_STAGE_OP,
                                       NULL,
                                       gd_xdr_from_mgmt_stage_op_req,
                                       this, glusterd3_1_stage_op_cbk);

out:
        if ((_gf_true == is_alloc) && req.buf.buf_val)
                GF_FREE (req.buf.buf_val);

        gf_log ("glusterd", GF_LOG_DEBUG, "Returning %d", ret);
        return ret;
}

int32_t
glusterd3_1_commit_op (call_frame_t *frame, xlator_t *this,
                      void *data)
{
        gd1_mgmt_commit_op_req  req         = {{0,},};
        int                     ret         = -1;
        glusterd_peerinfo_t    *peerinfo    = NULL;
        glusterd_conf_t        *priv        = NULL;
        call_frame_t           *dummy_frame = NULL;
        dict_t                 *dict        = NULL;
        gf_boolean_t            is_alloc    = _gf_true;

        if (!this) {
                goto out;
        }

        dict = data;
        priv = this->private;
        GF_ASSERT (priv);

        ret = dict_get_ptr (dict, "peerinfo", VOID (&peerinfo));
        if (ret)
                goto out;

        //peerinfo should not be in payload
        dict_del (dict, "peerinfo");

        glusterd_get_uuid (&req.uuid);
        req.op = glusterd_op_get_op ();

        if (GD_OP_DELETE_VOLUME == req.op) {
                ret = dict_get_str (dict, "volname", &req.buf.buf_val);
                if (ret)
                        goto out;
                req.buf.buf_len = strlen (req.buf.buf_val);
                is_alloc = _gf_false;
        } else {
                ret = dict_allocate_and_serialize (dict, &req.buf.buf_val,
                                                   (size_t *)&req.buf.buf_len);

                if (ret)
                        goto out;
        }

        dummy_frame = create_frame (this, this->ctx->pool);
        if (!dummy_frame)
                goto out;

        ret = glusterd_submit_request (peerinfo->rpc, &req, dummy_frame,
                                       peerinfo->mgmt, GD_MGMT_COMMIT_OP,
                                       NULL,
                                       gd_xdr_from_mgmt_commit_op_req,
                                       this, glusterd3_1_commit_op_cbk);

out:
        if ((_gf_true == is_alloc) && req.buf.buf_val)
                GF_FREE (req.buf.buf_val);

        gf_log ("glusterd", GF_LOG_DEBUG, "Returning %d", ret);
        return ret;
}

int32_t
glusterd_start_brick_disconnect_timer (glusterd_op_brick_rsp_ctx_t *ev_ctx)
{
        struct timeval       timeout = {0, };
        int32_t              ret = -1;
        xlator_t             *this = NULL;
        glusterd_brickinfo_t *brickinfo = NULL;

        timeout.tv_sec  = 5;
        timeout.tv_usec = 0;
        brickinfo = ev_ctx->brickinfo;
        GF_ASSERT (brickinfo);
        this = THIS;
        GF_ASSERT (this);

        brickinfo->timer = gf_timer_call_after (this->ctx, timeout,
                                                glusterd_op_brick_disconnect,
                                                (void *) ev_ctx);

        ret = 0;

        return ret;
}

int32_t
glusterd3_1_brick_op_cbk (struct rpc_req *req, struct iovec *iov,
                          int count, void *myframe)
{
        gd1_mgmt_brick_op_rsp         rsp   = {0};
        int                           ret   = -1;
        int32_t                       op_ret = -1;
        glusterd_op_sm_event_type_t   event_type = GD_OP_EVENT_NONE;
        call_frame_t                  *frame = NULL;
        glusterd_op_brick_rsp_ctx_t   *ev_ctx = NULL;
        int32_t                       op = -1;
        dict_t                        *dict = NULL;

        GF_ASSERT (req);
        frame = myframe;

        if (-1 == req->rpc_status) {
                rsp.op_ret   = -1;
                rsp.op_errno = EINVAL;
                rsp.op_errstr = "error";
		event_type = GD_OP_EVENT_RCVD_RJT;
                goto out;
        }

        ret =  gd_xdr_to_mgmt_brick_op_rsp (*iov, &rsp);
        if (ret < 0) {
                gf_log ("", GF_LOG_ERROR, "error");
                rsp.op_ret   = -1;
                rsp.op_errno = EINVAL;
                rsp.op_errstr = strdup ("Unable to decode response");
		event_type = GD_OP_EVENT_RCVD_RJT;
                goto out;
        }

        if (rsp.output.output_len) {
                /* Unserialize the dictionary */
                dict  = dict_new ();

                ret = dict_unserialize (rsp.output.output_val,
                                        rsp.output.output_len,
                                        &dict);
                if (ret < 0) {
                        gf_log ("glusterd", GF_LOG_ERROR,
                                "failed to "
                                "unserialize rsp-buffer to dictionary");
			event_type = GD_OP_EVENT_RCVD_RJT;
                        goto out;
                } else {
                        dict->extra_stdfree = rsp.output.output_val;
                }
        }

        op_ret = rsp.op_ret;

out:
        ev_ctx = GF_CALLOC (1, sizeof (*ev_ctx), gf_gld_mt_brick_rsp_ctx_t);
        GF_ASSERT (ev_ctx);
        if (op_ret) {
                event_type = GD_OP_EVENT_RCVD_RJT;
                ev_ctx->op_ret = op_ret;
                ev_ctx->op_errstr = gf_strdup(rsp.op_errstr);
        } else {
                event_type = GD_OP_EVENT_RCVD_ACC;
        }
        ev_ctx->brickinfo = frame->cookie;
        ev_ctx->rsp_dict  = dict;
        ev_ctx->commit_ctx = frame->local;
        op = glusterd_op_get_op ();
        if ((op == GD_OP_STOP_VOLUME) ||
           (op == GD_OP_REMOVE_BRICK)) {
                ret = glusterd_start_brick_disconnect_timer (ev_ctx);
        } else {
                ret = glusterd_op_sm_inject_event (event_type, ev_ctx);
                if (!ret) {
                        glusterd_friend_sm ();
                        glusterd_op_sm ();
                }
        }

        if (ret && dict)
                dict_unref (dict);
        if (rsp.op_errstr && strcmp (rsp.op_errstr, "error"))
                free (rsp.op_errstr); //malloced by xdr
        GLUSTERD_STACK_DESTROY (frame);
        return ret;
}


struct rpc_clnt_procedure glusterd3_1_clnt_mgmt_actors[GD_MGMT_MAXVALUE] = {
        [GD_MGMT_NULL]        = {"NULL", NULL },
        [GD_MGMT_PROBE_QUERY]  = { "PROBE_QUERY",  glusterd3_1_probe},
        [GD_MGMT_FRIEND_ADD]  = { "FRIEND_ADD",  glusterd3_1_friend_add },
        [GD_MGMT_CLUSTER_LOCK] = {"CLUSTER_LOCK", glusterd3_1_cluster_lock},
        [GD_MGMT_CLUSTER_UNLOCK] = {"CLUSTER_UNLOCK", glusterd3_1_cluster_unlock},
        [GD_MGMT_STAGE_OP] = {"STAGE_OP", glusterd3_1_stage_op},
        [GD_MGMT_COMMIT_OP] = {"COMMIT_OP", glusterd3_1_commit_op},
        [GD_MGMT_FRIEND_REMOVE]  = { "FRIEND_REMOVE",  glusterd3_1_friend_remove},
        [GD_MGMT_FRIEND_UPDATE]  = { "FRIEND_UPDATE",  glusterd3_1_friend_update},
};

struct rpc_clnt_procedure glusterd3_1_fs_mgmt_actors[GD_MGMT_MAXVALUE] = {
        [GD_MGMT_NULL]        = {"NULL", NULL },
        [GD_MGMT_BRICK_OP] = {"BRICK_OP", glusterd3_1_brick_op},
};

struct rpc_clnt_program glusterd3_1_mgmt_prog = {
        .progname  = "Mgmt 3.1",
        .prognum   = GLUSTERD1_MGMT_PROGRAM,
        .progver   = GLUSTERD1_MGMT_VERSION,
        .proctable = glusterd3_1_clnt_mgmt_actors,
        .numproc   = GLUSTERD1_MGMT_PROCCNT,
};

struct rpc_clnt_procedure gd_clnt_mgmt_actors[GLUSTERD_MGMT_MAXVALUE] = {
        [GLUSTERD_MGMT_NULL]           = {"NULL", NULL },
        [GLUSTERD_MGMT_PROBE_QUERY]    = {"PROBE_QUERY", glusterd3_1_probe},
        [GLUSTERD_MGMT_FRIEND_ADD]     = {"FRIEND_ADD", glusterd3_1_friend_add},
        [GLUSTERD_MGMT_CLUSTER_LOCK]   = {"CLUSTER_LOCK", glusterd3_1_cluster_lock},
        [GLUSTERD_MGMT_CLUSTER_UNLOCK] = {"CLUSTER_UNLOCK", glusterd3_1_cluster_unlock},
        [GLUSTERD_MGMT_STAGE_OP]       = {"STAGE_OP", glusterd3_1_stage_op},
        [GLUSTERD_MGMT_COMMIT_OP]      = {"COMMIT_OP", glusterd3_1_commit_op},
        [GLUSTERD_MGMT_FRIEND_REMOVE]  = {"FRIEND_REMOVE", glusterd3_1_friend_remove},
        [GLUSTERD_MGMT_FRIEND_UPDATE]  = {"FRIEND_UPDATE", glusterd3_1_friend_update},
};

struct rpc_clnt_program gd_clnt_mgmt_prog = {
        .progname  = "glusterd clnt mgmt",
        .prognum   = GD_MGMT_PROGRAM,
        .progver   = GD_MGMT_VERSION,
        .numproc   = GD_MGMT_PROCCNT,
        .proctable = gd_clnt_mgmt_actors,
};

struct rpc_clnt_program glusterd_glusterfs_3_1_mgmt_prog = {
        .progname  = "GlusterFS Mops",
        .prognum   = GLUSTERFS_PROGRAM,
        .progver   = GLUSTERFS_VERSION,
        .proctable = glusterd3_1_fs_mgmt_actors,
        .numproc   = GLUSTERFS_PROCCNT,
};

int32_t
glusterd3_1_brick_op (call_frame_t *frame, xlator_t *this,
                      void *data)
{
        gd1_mgmt_brick_op_req           *req = NULL;
        int                             ret = 0;
        glusterd_conf_t                 *priv = NULL;
        call_frame_t                    *dummy_frame = NULL;
        char                            *op_errstr = NULL;
        int                             pending_bricks = 0;
        glusterd_pending_node_t         *pending_brick;
        glusterd_brickinfo_t            *brickinfo = NULL;
        glusterd_req_ctx_t               *req_ctx = NULL;

        if (!this) {
                ret = -1;
                goto out;
        }
        priv = this->private;
        GF_ASSERT (priv);

        req_ctx = data;
        GF_ASSERT (req_ctx);
        INIT_LIST_HEAD (&opinfo.pending_bricks);
        ret = glusterd_op_bricks_select (req_ctx->op, req_ctx->dict, &op_errstr);

        if (ret) {
                gf_log ("", GF_LOG_ERROR, "Brick Op failed");
                opinfo.op_errstr = op_errstr;
                goto out;
        }

        list_for_each_entry (pending_brick, &opinfo.pending_bricks, list) {
                dummy_frame = create_frame (this, this->ctx->pool);
                brickinfo = pending_brick->node;

                if (!dummy_frame)
                        continue;
                if (_gf_false == glusterd_is_brick_started (brickinfo))
                        continue;

                ret = glusterd_brick_op_build_payload (req_ctx->op, brickinfo,
                                                (gd1_mgmt_brick_op_req **)&req,
                                                 req_ctx->dict);

                if (ret)
                        goto out;

                dummy_frame->local = data;
                dummy_frame->cookie = brickinfo;
                ret = glusterd_submit_request (brickinfo->rpc, req, dummy_frame,
                                               &glusterd_glusterfs_3_1_mgmt_prog,
                                               req->op, NULL,
                                               gd_xdr_from_mgmt_brick_op_req,
                                               this, glusterd3_1_brick_op_cbk);
                if (req) {
                        if (req->input.input_val)
                                GF_FREE (req->input.input_val);
                        GF_FREE (req);
                        req = NULL;
                }
                if (!ret)
                        pending_bricks++;
        }

        gf_log ("glusterd", GF_LOG_DEBUG, "Sent op req to %d bricks",
                                            pending_bricks);
        opinfo.brick_pending_count = pending_bricks;

out:
        if (ret) {
                glusterd_op_sm_inject_event (GD_OP_EVENT_RCVD_RJT, data);
                opinfo.op_ret = ret;
        }
        gf_log ("glusterd", GF_LOG_DEBUG, "Returning %d", ret);
        return ret;
}
