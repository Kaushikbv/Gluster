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

#ifndef GSYNC_CONF
#define GSYNC_CONF GEOREP"/gsyncd.conf"
#endif
#define DEFAULT_LOG_FILE_DIRECTORY      DATADIR "/log/glusterfs"

#include "cli.h"
#include "compat-errno.h"
#include "cli-cmd.h"
#include <sys/uio.h>
#include <stdlib.h>
#include <sys/mount.h>
#include "cli1-xdr.h"
#include "cli1.h"
#include "protocol-common.h"
#include "cli-mem-types.h"
#include "compat.h"

#include "glusterfs3.h"
#include "portmap.h"

extern rpc_clnt_prog_t *cli_rpc_prog;
extern int              cli_op_ret;
extern int              connected;

char *cli_volume_type[] = {"Distribute",
                           "Stripe",
                           "Replicate",
                           "Distributed-Stripe",
                           "Distributed-Replicate",
};


char *cli_volume_status[] = {"Created",
                             "Started",
                             "Stopped"
};

int32_t
gf_cli3_1_get_volume (call_frame_t *frame, xlator_t *this,
                      void *data);


rpc_clnt_prog_t cli_handshake_prog = {
        .progname  = "cli handshake",
        .prognum   = GLUSTER_HNDSK_PROGRAM,
        .progver   = GLUSTER_HNDSK_VERSION,
};

rpc_clnt_prog_t cli_pmap_prog = {
        .progname   = "cli portmap",
        .prognum    = GLUSTER_PMAP_PROGRAM,
        .progver    = GLUSTER_PMAP_VERSION,
};


int
gf_cli3_1_probe_cbk (struct rpc_req *req, struct iovec *iov,
                        int count, void *myframe)
{
        gf1_cli_probe_rsp    rsp   = {0,};
        int                   ret   = 0;

        if (-1 == req->rpc_status) {
                goto out;
        }

        ret = gf_xdr_to_cli_probe_rsp (*iov, &rsp);
        if (ret < 0) {
                gf_log ("", GF_LOG_ERROR, "error");
                //rsp.op_ret   = -1;
                //rsp.op_errno = EINVAL;
                goto out;
        }

        gf_log ("cli", GF_LOG_INFO, "Received resp to probe");
	 if (!rsp.op_ret) {
	 	switch (rsp.op_errno) {
		 	case GF_PROBE_SUCCESS:
		      		cli_out ("Probe successful");
		      		break;
	 	 	case GF_PROBE_LOCALHOST:
		      		cli_out ("Probe on localhost not needed");
		      		break;
			case GF_PROBE_FRIEND:
				cli_out ("Probe on host %s port %d already"
					 " in peer list", rsp.hostname, rsp.port);
				break;
		 	default:
		      		cli_out ("Probe returned with unknown errno %d",
					rsp.op_errno);
		      		break;
	 	}
	 }

        if (rsp.op_ret) {
                switch (rsp.op_errno) {
                        case GF_PROBE_ANOTHER_CLUSTER:
                                cli_out ("%s is already part of "
                                         "another cluster", rsp.hostname);
                                break;
                        case GF_PROBE_VOLUME_CONFLICT:
                                cli_out ("Atleast one volume on %s conflicts "
                                         "with existing volumes in the "
                                         "cluster", rsp.hostname);
                                break;
                        case GF_PROBE_UNKNOWN_PEER:
                                cli_out ("%s responded with 'unknown peer' error, "
                                         "this could happen if %s doesn't have"
                                         " localhost in its peer database",
                                         rsp.hostname, rsp.hostname);
                                break;
                        case GF_PROBE_ADD_FAILED:
                                cli_out ("Failed to add peer information "
                                         "on %s" , rsp.hostname);
                                break;

                        default:
                                cli_out ("Probe unsuccessful\nProbe returned "
                                         "with unknown errno %d", rsp.op_errno);
                                break;
                }
                gf_log ("glusterd",GF_LOG_ERROR,"Probe failed with op_ret %d"
                        " and op_errno %d", rsp.op_ret, rsp.op_errno);
        }
        ret = rsp.op_ret;

out:
        cli_cmd_broadcast_response (ret);
        return ret;
}

int
gf_cli3_1_deprobe_cbk (struct rpc_req *req, struct iovec *iov,
                       int count, void *myframe)
{
        gf1_cli_deprobe_rsp    rsp   = {0,};
        int                   ret   = 0;

        if (-1 == req->rpc_status) {
                goto out;
        }

        ret = gf_xdr_to_cli_deprobe_rsp (*iov, &rsp);
        if (ret < 0) {
                gf_log ("", GF_LOG_ERROR, "error");
                //rsp.op_ret   = -1;
                //rsp.op_errno = EINVAL;
                goto out;
        }

        gf_log ("cli", GF_LOG_INFO, "Received resp to deprobe");
        if (rsp.op_ret) {
                switch (rsp.op_errno) {
                        case GF_DEPROBE_LOCALHOST:
                                cli_out ("%s is localhost",
                                         rsp.hostname);
                                break;
                        case GF_DEPROBE_NOT_FRIEND:
                                cli_out ("%s is not part of cluster",
                                         rsp.hostname);
                                break;
                        case GF_DEPROBE_BRICK_EXIST:
                                cli_out ("Brick(s) with the peer %s exist in "
                                         "cluster", rsp.hostname);
                                break;
                        default:
                                cli_out ("Detach unsuccessful\nDetach returned "
                                         "with unknown errno %d",
                                         rsp.op_errno);
                                break;
                }
                gf_log ("glusterd",GF_LOG_ERROR,"Detach failed with op_ret %d"
                        " and op_errno %d", rsp.op_ret, rsp.op_errno);
        } else {
                cli_out ("Detach successful");
        }


        ret = rsp.op_ret;

out:
        cli_cmd_broadcast_response (ret);
        return ret;
}

int
gf_cli3_1_list_friends_cbk (struct rpc_req *req, struct iovec *iov,
                             int count, void *myframe)
{
        gf1_cli_peer_list_rsp      rsp   = {0,};
        int                        ret   = 0;
        dict_t                     *dict = NULL;
        char                       *uuid_buf = NULL;
        char                       *hostname_buf = NULL;
        int32_t                    i = 1;
        char                       key[256] = {0,};
        char                       *state = NULL;
        int32_t                    port = 0;
        int32_t                    connected = 0;
        char                       *connected_str = NULL;

        if (-1 == req->rpc_status) {
                goto out;
        }

        ret = gf_xdr_to_cli_peer_list_rsp (*iov, &rsp);
        if (ret < 0) {
                gf_log ("", GF_LOG_ERROR, "error");
                //rsp.op_ret   = -1;
                //rsp.op_errno = EINVAL;
                goto out;
        }


        gf_log ("cli", GF_LOG_INFO, "Received resp to list: %d",
                rsp.op_ret);

        ret = rsp.op_ret;

        if (!rsp.op_ret) {

                if (!rsp.friends.friends_len) {
                        cli_out ("No peers present");
                        ret = 0;
                        goto out;
                }

                dict = dict_new ();

                if (!dict) {
                        ret = -1;
                        goto out;
                }

                ret = dict_unserialize (rsp.friends.friends_val,
                                        rsp.friends.friends_len,
                                        &dict);

                if (ret) {
                        gf_log ("", GF_LOG_ERROR,
                                        "Unable to allocate memory");
                        goto out;
                }

                ret = dict_get_int32 (dict, "count", &count);

                if (ret) {
                        goto out;
                }

                cli_out ("Number of Peers: %d", count);

                while ( i <= count) {
                        snprintf (key, 256, "friend%d.uuid", i);
                        ret = dict_get_str (dict, key, &uuid_buf);
                        if (ret)
                                goto out;

                        snprintf (key, 256, "friend%d.hostname", i);
                        ret = dict_get_str (dict, key, &hostname_buf);
                        if (ret)
                                goto out;

                        snprintf (key, 256, "friend%d.connected", i);
                        ret = dict_get_int32 (dict, key, &connected);
                        if (ret)
                                goto out;
                        if (connected)
                                connected_str = "Connected";
                        else
                                connected_str = "Disconnected";

                        snprintf (key, 256, "friend%d.port", i);
                        ret = dict_get_int32 (dict, key, &port);
                        if (ret)
                                goto out;

                        snprintf (key, 256, "friend%d.state", i);
                        ret = dict_get_str (dict, key, &state);
                        if (ret)
                                goto out;

                        if (!port) {
                                cli_out ("\nHostname: %s\nUuid: %s\nState: %s "
                                         "(%s)",
                                         hostname_buf, uuid_buf, state,
                                         connected_str);
                        } else {
                                cli_out ("\nHostname: %s\nPort: %d\nUuid: %s\n"
                                         "State: %s (%s)", hostname_buf, port,
                                         uuid_buf, state, connected_str);
                        }
                        i++;
                }
        } else {
                ret = -1;
                goto out;
        }


        ret = 0;

out:
        cli_cmd_broadcast_response (ret);
        if (ret)
                cli_out ("Peer status unsuccessful");

        if (dict)
                dict_destroy (dict);

        return ret;
}

void
cli_out_options ( char *substr, char *optstr, char *valstr)
{
        char                    *ptr1 = NULL;
        char                    *ptr2 = NULL;

        ptr1 = substr;
        ptr2 = optstr;

        while (ptr1)
        {
                if (*ptr1 != *ptr2)
                        break;
                ptr1++;
                ptr2++;
                if (!ptr1)
                        return;
                if (!ptr2)
                        return;
        }

        if (*ptr2 == '\0')
                return;
        cli_out ("%s: %s",ptr2 , valstr);
}


int
gf_cli3_1_get_volume_cbk (struct rpc_req *req, struct iovec *iov,
                             int count, void *myframe)
{
        gf1_cli_get_vol_rsp        rsp   = {0,};
        int                        ret   = 0;
        dict_t                     *dict = NULL;
        char                       *volname = NULL;
        int32_t                    i = 0;
        char                       key[1024] = {0,};
        int32_t                    status = 0;
        int32_t                    type = 0;
        int32_t                    brick_count = 0;
        int32_t                    sub_count = 0;
        int32_t                    vol_type = 0;
        char                       *brick = NULL;
        int32_t                    j = 1;
        cli_local_t                *local = NULL;
        int32_t                    transport = 0;
        data_pair_t                *pairs = NULL;
        char                       *ptr = NULL;
        data_t                     *value = NULL;
        int                        opt_count = 0;
        int                        k = 0;
        char                       err_str[2048] = {0};

        snprintf (err_str, sizeof (err_str), "Volume info unsuccessful");
        if (-1 == req->rpc_status) {
                goto out;
        }

        ret = gf_xdr_to_cli_get_vol_rsp (*iov, &rsp);
        if (ret < 0) {
                gf_log ("", GF_LOG_ERROR, "error");
                //rsp.op_ret   = -1;
                //rsp.op_errno = EINVAL;
                goto out;
        }


        gf_log ("cli", GF_LOG_INFO, "Received resp to get vol: %d",
                rsp.op_ret);

        if (!rsp.op_ret) {

                if (!rsp.volumes.volumes_len) {
                        cli_out ("No volumes present");
                        ret = 0;
                        goto out;
                }

                dict = dict_new ();

                if (!dict) {
                        ret = -1;
                        goto out;
                }

                ret = dict_unserialize (rsp.volumes.volumes_val,
                                        rsp.volumes.volumes_len,
                                        &dict);

                if (ret) {
                        gf_log ("", GF_LOG_ERROR,
                                        "Unable to allocate memory");
                        goto out;
                }

                ret = dict_get_int32 (dict, "count", &count);

                if (ret) {
                        goto out;
                }

                local = ((call_frame_t *)myframe)->local;
                //cli_out ("Number of Volumes: %d", count);

                if (!count && (local->u.get_vol.flags ==
                                        GF_CLI_GET_NEXT_VOLUME)) {
                        local->u.get_vol.volname = NULL;
                        ret = 0;
                        goto out;
                } else if (!count && (local->u.get_vol.flags ==
                                        GF_CLI_GET_VOLUME)) {
                        snprintf (err_str, sizeof (err_str),
                                  "Volume %s does not exist",
                                  local->u.get_vol.volname);
                        ret = -1;
                        goto out;
                }

                while ( i < count) {
                        cli_out ("");
                        snprintf (key, 256, "volume%d.name", i);
                        ret = dict_get_str (dict, key, &volname);
                        if (ret)
                                goto out;

                        snprintf (key, 256, "volume%d.type", i);
                        ret = dict_get_int32 (dict, key, &type);
                        if (ret)
                                goto out;

                        snprintf (key, 256, "volume%d.status", i);
                        ret = dict_get_int32 (dict, key, &status);
                        if (ret)
                                goto out;

                        snprintf (key, 256, "volume%d.brick_count", i);
                        ret = dict_get_int32 (dict, key, &brick_count);
                        if (ret)
                                goto out;

                        snprintf (key, 256, "volume%d.sub_count", i);
                        ret = dict_get_int32 (dict, key, &sub_count);
                        if (ret)
                                goto out;

                        snprintf (key, 256, "volume%d.transport", i);
                        ret = dict_get_int32 (dict, key, &transport);
                        if (ret)
                                goto out;

                        vol_type = type;

                        // Stripe
                        if ((type == 1) && (sub_count < brick_count))
                                vol_type = 3;

                        // Replicate
                        if ((type == 2) && (sub_count < brick_count))
                                vol_type = 4;

                        cli_out ("Volume Name: %s", volname);
                        cli_out ("Type: %s", cli_volume_type[vol_type]);
                        cli_out ("Status: %s", cli_volume_status[status], brick_count);
                        if ((sub_count > 1) && (brick_count > sub_count))
                                cli_out ("Number of Bricks: %d x %d = %d",
                                         brick_count / sub_count, sub_count,
                                         brick_count);
                        else
                                cli_out ("Number of Bricks: %d", brick_count);

                        cli_out ("Transport-type: %s",
                                 ((transport == 0)?"tcp":
                                   (transport == 1)?"rdma":
                                  "tcp,rdma"));
                        j = 1;


                        GF_FREE (local->u.get_vol.volname);
                        local->u.get_vol.volname = gf_strdup (volname);

                        if (brick_count)
                                cli_out ("Bricks:");

                        while ( j <= brick_count) {
                                snprintf (key, 1024, "volume%d.brick%d",
                                          i, j);
                                ret = dict_get_str (dict, key, &brick);
                                if (ret)
                                        goto out;
                                cli_out ("Brick%d: %s", j, brick);
                                j++;
                        }
                        pairs = dict->members_list;
                        if (!pairs) {
                                ret = -1;
                                goto out;
                        }

                        snprintf (key, 256, "volume%d.opt_count",i);
                        ret = dict_get_int32 (dict, key, &opt_count);
                        if (ret)
                            goto out;

                        if (!opt_count)
                            goto out;

                        cli_out ("Options Reconfigured:");
                        k = 0;
                        while ( k < opt_count) {

                                snprintf (key, 256, "volume%d.option.",i);
                                while (pairs) {
                                        ptr = strstr (pairs->key, "option.");
                                        if (ptr) {
                                                value = pairs->value;
                                                if (!value) {
                                                        ret = -1;
                                                        goto out;
                                                }
                                                cli_out_options (key, pairs->key,
                                                                 value->data);
                                        }
                                        pairs = pairs->next;
                                }
                                k++;
                       }

                       i++;
                }


        } else {
                ret = -1;
                goto out;
        }


        ret = 0;

out:
        cli_cmd_broadcast_response (ret);
        if (ret)
                cli_out (err_str);

        if (dict)
                dict_destroy (dict);

        gf_log ("", GF_LOG_INFO, "Returning: %d", ret);
        return ret;
}

int
gf_cli3_1_create_volume_cbk (struct rpc_req *req, struct iovec *iov,
                             int count, void *myframe)
{
        gf1_cli_create_vol_rsp  rsp   = {0,};
        int                     ret   = 0;
        cli_local_t             *local = NULL;
        char                    *volname = NULL;
        dict_t                  *dict = NULL;

        if (-1 == req->rpc_status) {
                goto out;
        }

        local = ((call_frame_t *) (myframe))->local;
        ((call_frame_t *) (myframe))->local = NULL;

        ret = gf_xdr_to_cli_create_vol_rsp (*iov, &rsp);
        if (ret < 0) {
                gf_log ("", GF_LOG_ERROR, "error");
                goto out;
        }

        dict = local->u.create_vol.dict;

        ret = dict_get_str (dict, "volname", &volname);

        gf_log ("cli", GF_LOG_INFO, "Received resp to create volume");
	if (rsp.op_ret && strcmp (rsp.op_errstr, ""))
	        cli_out ("%s", rsp.op_errstr);
        else
                cli_out ("Creation of volume %s has been %s", volname,
                                (rsp.op_ret) ? "unsuccessful":
                                "successful. Please start the volume to "
                                "access data.");
        ret = rsp.op_ret;

out:
        cli_cmd_broadcast_response (ret);
        if (dict)
                dict_unref (dict);
        if (local)
                cli_local_wipe (local);
        if (rsp.volname)
                free (rsp.volname);
        if (rsp.op_errstr)
                free (rsp.op_errstr);
        return ret;
}

int
gf_cli3_1_delete_volume_cbk (struct rpc_req *req, struct iovec *iov,
                             int count, void *myframe)
{
        gf1_cli_delete_vol_rsp  rsp   = {0,};
        int                     ret   = 0;
        cli_local_t             *local = NULL;
        char                    *volname = NULL;
        call_frame_t            *frame = NULL;

        if (-1 == req->rpc_status) {
                goto out;
        }

        ret = gf_xdr_to_cli_delete_vol_rsp (*iov, &rsp);
        if (ret < 0) {
                gf_log ("", GF_LOG_ERROR, "error");
                goto out;
        }

        frame = myframe;
        local = frame->local;
        frame->local = NULL;

        if (local)
                volname = local->u.delete_vol.volname;


        gf_log ("cli", GF_LOG_INFO, "Received resp to delete volume");

        if (rsp.op_ret && strcmp (rsp.op_errstr, ""))
                cli_out (rsp.op_errstr);
        else
                cli_out ("Deleting volume %s has been %s", volname,
                         (rsp.op_ret) ? "unsuccessful": "successful");
        ret = rsp.op_ret;

out:
        cli_cmd_broadcast_response (ret);
        cli_local_wipe (local);
        if (rsp.volname)
                free (rsp.volname);
        gf_log ("", GF_LOG_INFO, "Returning with %d", ret);
        return ret;
}

int
gf_cli3_1_start_volume_cbk (struct rpc_req *req, struct iovec *iov,
                             int count, void *myframe)
{
        gf1_cli_start_vol_rsp   rsp   = {0,};
        int                     ret   = 0;
        cli_local_t             *local = NULL;
        char                    *volname = NULL;
        call_frame_t            *frame = NULL;

        if (-1 == req->rpc_status) {
                goto out;
        }

        ret = gf_xdr_to_cli_start_vol_rsp (*iov, &rsp);
        if (ret < 0) {
                gf_log ("", GF_LOG_ERROR, "error");
                goto out;
        }

        frame = myframe;

        if (frame) {
                local = frame->local;
                frame->local = NULL;
        }

        if (local)
                volname = local->u.start_vol.volname;

        gf_log ("cli", GF_LOG_INFO, "Received resp to start volume");

        if (rsp.op_ret && strcmp (rsp.op_errstr, ""))
                cli_out ("%s", rsp.op_errstr);
        else
                cli_out ("Starting volume %s has been %s", volname,
                        (rsp.op_ret) ? "unsuccessful": "successful");

        ret = rsp.op_ret;

out:
        cli_cmd_broadcast_response (ret);
        if (local)
                cli_local_wipe (local);
        if (rsp.volname)
                free (rsp.volname);
        if (rsp.op_errstr)
                free (rsp.op_errstr);
        return ret;
}

int
gf_cli3_1_stop_volume_cbk (struct rpc_req *req, struct iovec *iov,
                             int count, void *myframe)
{
        gf1_cli_stop_vol_rsp  rsp   = {0,};
        int                   ret   = 0;
        cli_local_t           *local = NULL;
        char                  *volname = NULL;
        call_frame_t          *frame = NULL;

        if (-1 == req->rpc_status) {
                goto out;
        }

        ret = gf_xdr_to_cli_stop_vol_rsp (*iov, &rsp);
        if (ret < 0) {
                gf_log ("", GF_LOG_ERROR, "error");
                goto out;
        }

        frame = myframe;

        if (frame)
                local = frame->local;

        if (local)
                volname = local->u.start_vol.volname;

        gf_log ("cli", GF_LOG_INFO, "Received resp to stop volume");

        if (rsp.op_ret && strcmp (rsp.op_errstr, ""))
                cli_out (rsp.op_errstr);
        else
                cli_out ("Stopping volume %s has been %s", volname,
                        (rsp.op_ret) ? "unsuccessful": "successful");
        ret = rsp.op_ret;

out:
        cli_cmd_broadcast_response (ret);
        if (rsp.op_errstr)
                free (rsp.op_errstr);
        if (rsp.volname)
                free (rsp.volname);
        return ret;
}

int
gf_cli3_1_defrag_volume_cbk (struct rpc_req *req, struct iovec *iov,
                             int count, void *myframe)
{
        gf2_cli_defrag_vol_rsp  rsp     = {0,};
        cli_local_t             *local   = NULL;
        char                    *volname = NULL;
        call_frame_t            *frame   = NULL;
        int                      cmd     = 0;
        int                      ret     = 0;

        if (-1 == req->rpc_status) {
                goto out;
        }

        ret = gf_xdr_to_cli_defrag_vol_rsp_v2 (*iov, &rsp);
        if (ret < 0) {
                gf_log ("", GF_LOG_ERROR, "error");
                goto out;
        }

        frame = myframe;

        if (frame)
                local = frame->local;

        if (local) {
                volname = local->u.defrag_vol.volname;
                cmd = local->u.defrag_vol.cmd;
        }
        if ((cmd == GF_DEFRAG_CMD_START) ||
            (cmd == GF_DEFRAG_CMD_START_LAYOUT_FIX) ||
            (cmd == GF_DEFRAG_CMD_START_MIGRATE_DATA)) {
                if (rsp.op_ret && strcmp (rsp.op_errstr, ""))
                        cli_out (rsp.op_errstr);
                else
                        cli_out ("starting rebalance on volume %s has been %s",
                                 volname, (rsp.op_ret) ? "unsuccessful":
                                 "successful");
        }
        if (cmd == GF_DEFRAG_CMD_STOP) {
                if (rsp.op_ret == -1) {
                        if (strcmp (rsp.op_errstr, ""))
                                cli_out (rsp.op_errstr);
                        else
                                cli_out ("rebalance volume %s stop failed",
                                         volname);
                } else {
                        cli_out ("stopped rebalance process of volume %s \n"
                                 "(after rebalancing %"PRId64" files totaling "
                                 "%"PRId64" bytes)", volname, rsp.files, rsp.size);
                }
        }
        if (cmd == GF_DEFRAG_CMD_STATUS) {
                if (rsp.op_ret == -1) {
                        if (strcmp (rsp.op_errstr, ""))
                                cli_out (rsp.op_errstr);
                        else
                                cli_out ("failed to get the status of "
                                         "rebalance process");
                } else {
                        char *status = "unknown";
                        if (rsp.op_errno == 0)
                                status = "not started";
                        if (rsp.op_errno == 1)
                                status = "step 1: layout fix in progress";
                        if (rsp.op_errno == 2)
                                status = "step 2: data migration in progress";
                        if (rsp.op_errno == 3)
                                status = "stopped";
                        if (rsp.op_errno == 4)
                                status = "completed";
                        if (rsp.op_errno == 5)
                                status = "failed";
                        if (rsp.op_errno == 6)
                                status = "step 1: layout fix complete";
                        if (rsp.op_errno == 7)
                                status = "step 2: data migration complete";

                        if (rsp.files && (rsp.op_errno == 1)) {
                                cli_out ("rebalance %s: fixed layout %"PRId64,
                                         status, rsp.files);
                                goto done;
                        }
                        if (rsp.files && (rsp.op_errno == 6)) {
                                cli_out ("rebalance %s: fixed layout %"PRId64,
                                         status, rsp.files);
                                goto done;
                        }
                        if (rsp.files) {
                                cli_out ("rebalance %s: rebalanced %"PRId64
                                         " files of size %"PRId64" (total files"
                                         " scanned %"PRId64")", status,
                                         rsp.files, rsp.size, rsp.lookedup_files);
                                goto done;
                        }

                        cli_out ("rebalance %s", status);
                }
        }

done:
        if (volname)
                GF_FREE (volname);

        ret = rsp.op_ret;

out:
        if (rsp.op_errstr)
                free (rsp.op_errstr); //malloced by xdr
        if (rsp.volname)
                free (rsp.volname); //malloced by xdr
        cli_cmd_broadcast_response (ret);
        return ret;
}

int
gf_cli3_1_rename_volume_cbk (struct rpc_req *req, struct iovec *iov,
                             int count, void *myframe)
{
        gf1_cli_rename_vol_rsp  rsp   = {0,};
        int                     ret   = 0;

        if (-1 == req->rpc_status) {
                goto out;
        }

        ret = gf_xdr_to_cli_rename_vol_rsp (*iov, &rsp);
        if (ret < 0) {
                gf_log ("", GF_LOG_ERROR, "error");
                goto out;
        }


        gf_log ("cli", GF_LOG_INFO, "Received resp to probe");
        cli_out ("Rename volume %s", (rsp.op_ret) ? "unsuccessful":
                                        "successful");

        ret = rsp.op_ret;

out:
        cli_cmd_broadcast_response (ret);
        return ret;
}

int
gf_cli3_1_reset_volume_cbk (struct rpc_req *req, struct iovec *iov,
                             int count, void *myframe)
{
        gf1_cli_reset_vol_rsp  rsp   = {0,};
        int                  ret   = 0;

        if (-1 == req->rpc_status) {
                goto out;
        }

        ret = gf_xdr_to_cli_reset_vol_rsp (*iov, &rsp);
        if (ret < 0) {
                gf_log ("", GF_LOG_ERROR, "error");
                goto out;
        }

        gf_log ("cli", GF_LOG_INFO, "Received resp to reset");

        if (rsp.op_ret &&  strcmp (rsp.op_errstr, ""))
                cli_out ("%s", rsp.op_errstr);
        else
                cli_out ("reset volume %s", (rsp.op_ret) ? "unsuccessful":
                                "successful");

        ret = rsp.op_ret;

out:
                cli_cmd_broadcast_response (ret);
        return ret;
}

int
gf_cli3_1_set_volume_cbk (struct rpc_req *req, struct iovec *iov,
                             int count, void *myframe)
{
        gf1_cli_set_vol_rsp  rsp   = {0,};
        int                  ret   = 0;

        if (-1 == req->rpc_status) {
                goto out;
        }

        ret = gf_xdr_to_cli_set_vol_rsp (*iov, &rsp);
        if (ret < 0) {
                gf_log ("", GF_LOG_ERROR, "error");
                goto out;
        }

        gf_log ("cli", GF_LOG_INFO, "Received resp to set");

        if (rsp.op_ret &&  strcmp (rsp.op_errstr, ""))
                cli_out ("%s", rsp.op_errstr);
        else
                cli_out ("Set volume %s", (rsp.op_ret) ? "unsuccessful":
                                                         "successful");

        ret = rsp.op_ret;

out:
        cli_cmd_broadcast_response (ret);
        return ret;
}

int
gf_cli3_1_add_brick_cbk (struct rpc_req *req, struct iovec *iov,
                             int count, void *myframe)
{
        gf1_cli_add_brick_rsp       rsp   = {0,};
        int                         ret   = 0;

        if (-1 == req->rpc_status) {
                goto out;
        }

        ret = gf_xdr_to_cli_add_brick_rsp (*iov, &rsp);
        if (ret < 0) {
                gf_log ("", GF_LOG_ERROR, "error");
                goto out;
        }


        gf_log ("cli", GF_LOG_INFO, "Received resp to add brick");

        if (rsp.op_ret && strcmp (rsp.op_errstr, ""))
                cli_out ("%s", rsp.op_errstr);
        else
                cli_out ("Add Brick %s", (rsp.op_ret) ? "unsuccessful":
                                                        "successful");
        ret = rsp.op_ret;

out:
        cli_cmd_broadcast_response (ret);
        if (rsp.volname)
                free (rsp.volname);
        if (rsp.op_errstr)
                free (rsp.op_errstr);
        return ret;
}


int
gf_cli3_1_remove_brick_cbk (struct rpc_req *req, struct iovec *iov,
                             int count, void *myframe)
{
        gf1_cli_remove_brick_rsp        rsp   = {0,};
        int                             ret   = 0;

        if (-1 == req->rpc_status) {
                goto out;
        }

        ret = gf_xdr_to_cli_remove_brick_rsp (*iov, &rsp);
        if (ret < 0) {
                gf_log ("", GF_LOG_ERROR, "error");
                goto out;
        }

        gf_log ("cli", GF_LOG_INFO, "Received resp to remove brick");

        if (rsp.op_ret && strcmp (rsp.op_errstr, ""))
                cli_out ("%s", rsp.op_errstr);
        else
                cli_out ("Remove Brick %s", (rsp.op_ret) ? "unsuccessful":
                                                           "successful");

        ret = rsp.op_ret;

out:
        cli_cmd_broadcast_response (ret);
        if (rsp.volname)
                free (rsp.volname);
        if (rsp.op_errstr)
                free (rsp.op_errstr);
        return ret;
}



int
gf_cli3_1_replace_brick_cbk (struct rpc_req *req, struct iovec *iov,
                             int count, void *myframe)
{
        gf1_cli_replace_brick_rsp        rsp              = {0,};
        int                              ret              = 0;
        cli_local_t                     *local            = NULL;
        call_frame_t                    *frame            = NULL;
        dict_t                          *dict             = NULL;
        char                            *src_brick        = NULL;
        char                            *dst_brick        = NULL;
        char                            *status_reply     = NULL;
        gf1_cli_replace_op               replace_op       = 0;
        char                            *rb_operation_str = NULL;

        if (-1 == req->rpc_status) {
                goto out;
        }

        frame = (call_frame_t *) myframe;

        ret = gf_xdr_to_cli_replace_brick_rsp (*iov, &rsp);
        if (ret < 0) {
                gf_log ("", GF_LOG_ERROR, "error");
                goto out;
        }

        local = frame->local;
        GF_ASSERT (local);
        dict = local->u.replace_brick.dict;

        ret = dict_get_int32 (dict, "operation", (int32_t *)&replace_op);
        if (ret) {
                gf_log ("", GF_LOG_DEBUG,
                        "dict_get on operation failed");
                goto out;
        }

        switch (replace_op) {
        case GF_REPLACE_OP_START:
                if (rsp.op_ret)
                        rb_operation_str = "replace-brick failed to start";
                else
                        rb_operation_str = "replace-brick started successfully";
                break;

        case GF_REPLACE_OP_STATUS:

                status_reply = rsp.status;
                if (rsp.op_ret || ret)
                        rb_operation_str = "replace-brick status unknown";
                else
                        rb_operation_str = status_reply;

                break;

        case GF_REPLACE_OP_PAUSE:
                if (rsp.op_ret)
                        rb_operation_str = "replace-brick pause failed";
                else
                        rb_operation_str = "replace-brick paused successfully";
                break;

        case GF_REPLACE_OP_ABORT:
                if (rsp.op_ret)
                        rb_operation_str = "replace-brick abort failed";
                else
                        rb_operation_str = "replace-brick aborted successfully";
                break;

        case GF_REPLACE_OP_COMMIT:
        case GF_REPLACE_OP_COMMIT_FORCE:
                ret = dict_get_str (dict, "src-brick", &src_brick);
                if (ret) {
                        gf_log ("", GF_LOG_DEBUG,
                                "dict_get on src-brick failed");
                        goto out;
                }

                ret = dict_get_str (dict, "dst-brick", &dst_brick);
                if (ret) {
                        gf_log ("", GF_LOG_DEBUG,
                                "dict_get on dst-brick failed");
                        goto out;
                }


                if (rsp.op_ret || ret)
                        rb_operation_str = "replace-brick commit failed";
                else
                        rb_operation_str = "replace-brick commit successful";

                break;

        default:
                gf_log ("", GF_LOG_DEBUG,
                        "Unknown operation");
                break;
        }

        if (rsp.op_ret && (strcmp (rsp.op_errstr, ""))) {
                rb_operation_str = rsp.op_errstr;
        }

        gf_log ("cli", GF_LOG_INFO, "Received resp to replace brick");
        cli_out ("%s",
                 rb_operation_str ? rb_operation_str : "Unknown operation");

        ret = rsp.op_ret;

out:
        if (local) {
                dict_unref (local->u.replace_brick.dict);
                GF_FREE (local->u.replace_brick.volname);
                cli_local_wipe (local);
        }

        cli_cmd_broadcast_response (ret);
        return ret;
}

static int
gf_cli3_1_log_filename_cbk (struct rpc_req *req, struct iovec *iov,
                            int count, void *myframe)
{
        gf1_cli_log_filename_rsp        rsp   = {0,};
        int                             ret   = -1;

        if (-1 == req->rpc_status) {
                goto out;
        }

        ret = gf_xdr_to_cli_log_filename_rsp (*iov, &rsp);
        if (ret < 0) {
                gf_log ("", GF_LOG_ERROR, "error");
                goto out;
        }

        gf_log ("cli", GF_LOG_DEBUG, "Received resp to log filename");

        if (rsp.op_ret && strcmp (rsp.errstr, ""))
                cli_out (rsp.errstr);
        else
                cli_out ("log filename : %s",
                         (rsp.op_ret) ? "unsuccessful": "successful");

        ret = rsp.op_ret;

out:
        cli_cmd_broadcast_response (ret);
        return ret;
}

static int
gf_cli3_1_log_locate_cbk (struct rpc_req *req, struct iovec *iov,
                          int count, void *myframe)
{
        gf1_cli_log_locate_rsp rsp   = {0,};
        int                    ret   = -1;

        if (-1 == req->rpc_status) {
                goto out;
        }

        ret = gf_xdr_to_cli_log_locate_rsp (*iov, &rsp);
        if (ret < 0) {
                gf_log ("", GF_LOG_ERROR, "error");
                goto out;
        }

        gf_log ("cli", GF_LOG_DEBUG, "Received resp to log locate");
        cli_out ("log file location: %s", rsp.path);

        ret = rsp.op_ret;

out:
        cli_cmd_broadcast_response (ret);
        return ret;
}

static int
gf_cli3_1_log_rotate_cbk (struct rpc_req *req, struct iovec *iov,
                          int count, void *myframe)
{
        gf1_cli_log_rotate_rsp rsp   = {0,};
        int                    ret   = -1;

        if (-1 == req->rpc_status) {
                goto out;
        }

        ret = gf_xdr_to_cli_log_rotate_rsp (*iov, &rsp);
        if (ret < 0) {
                gf_log ("", GF_LOG_ERROR, "error");
                goto out;
        }

        gf_log ("cli", GF_LOG_DEBUG, "Received resp to log rotate");

        if (rsp.op_ret && strcmp (rsp.errstr, ""))
                cli_out (rsp.errstr);
        else
                cli_out ("log rotate %s", (rsp.op_ret) ? "unsuccessful":
                                                         "successful");

        ret = rsp.op_ret;

out:
        cli_cmd_broadcast_response (ret);
        return ret;
}

static int
gf_cli3_1_sync_volume_cbk (struct rpc_req *req, struct iovec *iov,
                           int count, void *myframe)
{
        gf1_cli_sync_volume_rsp        rsp   = {0,};
        int                            ret   = -1;

        if (-1 == req->rpc_status) {
                goto out;
        }

        ret = gf_xdr_to_cli_sync_volume_rsp (*iov, &rsp);
        if (ret < 0) {
                gf_log ("", GF_LOG_ERROR, "error");
                goto out;
        }

        gf_log ("cli", GF_LOG_DEBUG, "Received resp to sync");

        if (rsp.op_ret && strcmp (rsp.op_errstr, ""))
                cli_out (rsp.op_errstr);
        else
                cli_out ("volume sync: %s",
                         (rsp.op_ret) ? "unsuccessful": "successful");
        ret = rsp.op_ret;

out:
        cli_cmd_broadcast_response (ret);
        return ret;
}

int32_t
print_limit_list (char *volname, char *limit_list)
{
        int64_t  size            = 0;
        int64_t  limit_value     = 0;
        int32_t  i, j, k;
        int32_t  len = 0, ret    = -1;
        char     path [PATH_MAX] = {0, };
        char     ret_str [1024]  = {0, };
        char     value [1024]    = {0, };
        char     mountdir []     = "/tmp/mntXXXXXX";
        char     cmd_str [PATH_MAX + 1024] = {0, };

        GF_VALIDATE_OR_GOTO ("cli", volname, out);
        GF_VALIDATE_OR_GOTO ("cli", limit_list, out);

        if (!connected)
                goto out;

        if (mkdtemp (mountdir) == NULL) {
                gf_log ("cli", GF_LOG_WARNING, "failed to create a temporary "
                        "mount directory");
                ret = -1;
                goto out;
        }

        /* Mount a temporary client to fetch the disk usage
         * of the directory on which the limit is set.
         */
        snprintf (cmd_str, sizeof (cmd_str), GFS_PREFIX "/sbin/glusterfs -s localhost "
                  "--volfile-id %s %s", volname, mountdir);

        ret = system (cmd_str);
        if (ret) {
                gf_log ("cli", GF_LOG_WARNING, "failed to mount glusterfs client");
                ret = -1;
                goto rm_dir;
        }

        len = strlen (limit_list);
        if (len == 0) {
                cli_out ("quota limit not set ");
                goto unmount;
        }

        i = 0;

        cli_out ("\tpath\t\t  limit_set\t     size");
        cli_out ("-----------------------------------------------------------"
                 "-----------------------");
        while (i < len) {
                j = 0;
                k = 0;

                while (limit_list [i] != ':') {
                        path [k++] = limit_list [i++];
                }
                path [k] = '\0';

                i++;  //skip ':'

                while (limit_list [i] != ',' && limit_list [i] != '\0') {
                        value [j++] = limit_list[i++];
                }
                value [j] = '\0';

                memset (&cmd_str, 0, sizeof (cmd_str));
                snprintf (cmd_str, sizeof (cmd_str), "%s/%s", mountdir, path);

                ret = getxattr (cmd_str, "trusted.limit.list", (void *) ret_str, 4096);
                if (ret < 0) {
                        cli_out ("%-20s %10s", path, value);
                } else {
                        sscanf (ret_str, "%"PRId64",%"PRId64, &size,
                                &limit_value);
                        cli_out ("%-20s %10"PRId64" %20"PRId64, path,
                                 limit_value, size);
                }
                i++;
        }

unmount:
        memset (&cmd_str, 0, sizeof (cmd_str));

#if GF_LINUX_HOST_OS
        usleep (200000);
        snprintf (cmd_str, sizeof (cmd_str), "umount -l %s", mountdir);
#else
        snprintf (cmd_str, sizeof (cmd_str), "umount %s", mountdir);
#endif
        ret = system (cmd_str);
        if (ret)
                gf_log ("cli", GF_LOG_WARNING, "error executing: %s", cmd_str);
rm_dir:
        rmdir (mountdir);
out:
        return ret;
}

int
gf_cli3_1_quota_cbk (struct rpc_req *req, struct iovec *iov,
                     int count, void *myframe)
{
        gf1_cli_quota_rsp  rsp        = {0,};
        int                ret        = 0;

        if (-1 == req->rpc_status) {
                goto out;
        }

        ret = gf_xdr_to_cli_quota_rsp (*iov, &rsp);
        if (ret < 0) {
                gf_log ("", GF_LOG_ERROR, "error");
                goto out;
        }

        if (rsp.op_ret &&
            strcmp (rsp.op_errstr, "") == 0) {
                cli_out ("command unsuccessful %s", rsp.op_errstr);
                goto out;
        }

        if (rsp.type == GF_QUOTA_OPTION_TYPE_LIST) {
                if (rsp.limit_list)
                        print_limit_list (rsp.volname, rsp.limit_list);
        } else {
                gf_log ("cli", GF_LOG_INFO, "Received resp to quota command ");
                if (rsp.op_errstr)
                        cli_out ("%s", rsp.op_errstr);
                else
                        cli_out ("%s", "successful");
        }

out:
        ret = rsp.op_ret;

        cli_cmd_broadcast_response (ret);
        return ret;
}

int
gf_cli3_1_getspec_cbk (struct rpc_req *req, struct iovec *iov,
                       int count, void *myframe)
{
        gf_getspec_rsp          rsp   = {0,};
        int                     ret   = 0;
        char                   *spec  = NULL;

        if (-1 == req->rpc_status) {
                goto out;
        }

        ret = xdr_to_getspec_rsp (*iov, &rsp);
        if (ret < 0 || rsp.op_ret == -1) {
                gf_log ("", GF_LOG_ERROR, "error");
                goto out;
        }

        gf_log ("cli", GF_LOG_INFO, "Received resp to getspec");

        spec = GF_MALLOC (rsp.op_ret + 1, cli_mt_char);
        if (!spec) {
                gf_log("", GF_LOG_ERROR, "out of memory");
                goto out;
        }
        memcpy (spec, rsp.spec, rsp.op_ret);
        spec[rsp.op_ret] = '\0';
        cli_out ("%s", spec);
        GF_FREE (spec);

        ret = 0;

out:
        cli_cmd_broadcast_response (ret);
        return ret;
}

int
gf_cli3_1_pmap_b2p_cbk (struct rpc_req *req, struct iovec *iov,
                        int count, void *myframe)
{
        pmap_port_by_brick_rsp rsp = {0,};
        int                     ret   = 0;
        char                   *spec  = NULL;

        if (-1 == req->rpc_status) {
                goto out;
        }

        ret = xdr_to_pmap_port_by_brick_rsp (*iov, &rsp);
        if (ret < 0 || rsp.op_ret == -1) {
                gf_log ("", GF_LOG_ERROR, "error");
                goto out;
        }

        gf_log ("cli", GF_LOG_INFO, "Received resp to pmap b2p");

        cli_out ("%d", rsp.port);
        GF_FREE (spec);

        ret = rsp.op_ret;

out:
        cli_cmd_broadcast_response (ret);
        return ret;
}


int32_t
gf_cli3_1_probe (call_frame_t *frame, xlator_t *this,
                 void *data)
{
        gf1_cli_probe_req  req      = {0,};
        int                ret      = 0;
        dict_t            *dict     = NULL;
        char              *hostname = NULL;
        int                port     = 0;

        if (!frame || !this ||  !data) {
                ret = -1;
                goto out;
        }

        dict = data;
        ret = dict_get_str (dict, "hostname", &hostname);
        if (ret)
                goto out;

        ret = dict_get_int32 (dict, "port", &port);
        if (ret)
                port = CLI_GLUSTERD_PORT;

        req.hostname = hostname;
        req.port     = port;

        ret = cli_cmd_submit (&req, frame, cli_rpc_prog,
                              GLUSTER_CLI_PROBE, NULL, gf_xdr_from_cli_probe_req,
                              this, gf_cli3_1_probe_cbk);

out:
        gf_log ("cli", GF_LOG_DEBUG, "Returning %d", ret);
        return ret;
}

int32_t
gf_cli3_1_deprobe (call_frame_t *frame, xlator_t *this,
                   void *data)
{
        gf1_cli_deprobe_req  req      = {0,};
        int                  ret      = 0;
        dict_t              *dict     = NULL;
        char                *hostname = NULL;
        int                  port     = 0;

        if (!frame || !this ||  !data) {
                ret = -1;
                goto out;
        }

        dict = data;
        ret = dict_get_str (dict, "hostname", &hostname);
        if (ret)
                goto out;

        ret = dict_get_int32 (dict, "port", &port);
        if (ret)
                port = CLI_GLUSTERD_PORT;

        req.hostname = hostname;
        req.port     = port;

        ret = cli_cmd_submit (&req, frame, cli_rpc_prog,
                              GLUSTER_CLI_DEPROBE, NULL,
                              gf_xdr_from_cli_deprobe_req,
                              this, gf_cli3_1_deprobe_cbk);

out:
        gf_log ("cli", GF_LOG_DEBUG, "Returning %d", ret);
        return ret;
}

int32_t
gf_cli3_1_list_friends (call_frame_t *frame, xlator_t *this,
                        void *data)
{
        gf1_cli_peer_list_req   req = {0,};
        int                     ret = 0;

        if (!frame || !this) {
                ret = -1;
                goto out;
        }

        req.flags = GF_CLI_LIST_ALL;

        ret = cli_cmd_submit (&req, frame, cli_rpc_prog,
                              GLUSTER_CLI_LIST_FRIENDS, NULL,
                              gf_xdr_from_cli_peer_list_req,
                              this, gf_cli3_1_list_friends_cbk);

out:
        gf_log ("cli", GF_LOG_DEBUG, "Returning %d", ret);
        return ret;
}

int32_t
gf_cli3_1_get_next_volume (call_frame_t *frame, xlator_t *this,
                           void *data)
{

        int                             ret = 0;
        cli_cmd_volume_get_ctx_t        *ctx = NULL;
        cli_local_t                     *local = NULL;

        if (!frame || !this || !data) {
                ret = -1;
                goto out;
        }

        ctx = data;

        ret = gf_cli3_1_get_volume (frame, this, data);

        local = frame->local;

        if (!local || !local->u.get_vol.volname) {
                cli_out ("No volumes present");
                goto out;
        }

        ctx->volname = local->u.get_vol.volname;

        while (ctx->volname) {
                ret = gf_cli3_1_get_volume (frame, this, ctx);
                if (ret)
                        goto out;
                ctx->volname = local->u.get_vol.volname;
        }

out:
        return ret;
}

int32_t
gf_cli3_1_get_volume (call_frame_t *frame, xlator_t *this,
                      void *data)
{
        gf1_cli_get_vol_req             req = {0,};
        int                             ret = 0;
        cli_cmd_volume_get_ctx_t        *ctx = NULL;
        dict_t                          *dict = NULL;

        if (!frame || !this || !data) {
                ret = -1;
                goto out;
        }

        ctx = data;
        req.flags = ctx->flags;

        dict = dict_new ();
        if (!dict)
                goto out;

        if (ctx->volname) {
                ret = dict_set_str (dict, "volname", ctx->volname);
                if (ret)
                        goto out;
        }

        ret = dict_allocate_and_serialize (dict,
                                           &req.dict.dict_val,
                                           (size_t *)&req.dict.dict_len);

        ret = cli_cmd_submit (&req, frame, cli_rpc_prog,
                              GLUSTER_CLI_GET_VOLUME, NULL,
                              gf_xdr_from_cli_get_vol_req,
                              this, gf_cli3_1_get_volume_cbk);

out:
        gf_log ("cli", GF_LOG_DEBUG, "Returning %d", ret);
        return ret;
}


int32_t
gf_cli3_1_create_volume (call_frame_t *frame, xlator_t *this,
                         void *data)
{
        gf1_cli_create_vol_req  req = {0,};
        int                     ret = 0;
        dict_t                  *dict = NULL;
        cli_local_t             *local = NULL;

        if (!frame || !this ||  !data) {
                ret = -1;
                goto out;
        }

        dict = dict_ref ((dict_t *)data);

        ret = dict_get_str (dict, "volname", &req.volname);

        if (ret)
                goto out;

        ret = dict_get_int32 (dict, "type", (int32_t *)&req.type);

        if (ret)
                goto out;

        ret = dict_get_int32 (dict, "count", &req.count);
        if (ret)
                goto out;

        ret = dict_allocate_and_serialize (dict,
                                           &req.bricks.bricks_val,
                                           (size_t *)&req.bricks.bricks_len);
        if (ret < 0) {
                gf_log (this->name, GF_LOG_DEBUG,
                        "failed to get serialized length of dict");
                goto out;
        }

        local = cli_local_get ();

        if (local) {
                local->u.create_vol.dict = dict_ref (dict);
                frame->local = local;
        }

        ret = cli_cmd_submit (&req, frame, cli_rpc_prog,
                              GLUSTER_CLI_CREATE_VOLUME, NULL,
                              gf_xdr_from_cli_create_vol_req,
                              this, gf_cli3_1_create_volume_cbk);



out:
        gf_log ("cli", GF_LOG_DEBUG, "Returning %d", ret);

        if (dict)
                dict_unref (dict);

        if (req.bricks.bricks_val) {
                GF_FREE (req.bricks.bricks_val);
        }

        return ret;
}

int32_t
gf_cli3_1_delete_volume (call_frame_t *frame, xlator_t *this,
                         void *data)
{
        gf1_cli_delete_vol_req  req = {0,};
        int                     ret = 0;
        cli_local_t             *local = NULL;

        if (!frame || !this ||  !data) {
                ret = -1;
                goto out;
        }

        local = cli_local_get ();

        if (local) {
                local->u.delete_vol.volname = data;
                frame->local = local;
        }

        req.volname = data;

        ret = cli_cmd_submit (&req, frame, cli_rpc_prog,
                              GLUSTER_CLI_DELETE_VOLUME, NULL,
                              gf_xdr_from_cli_delete_vol_req,
                              this, gf_cli3_1_delete_volume_cbk);

out:
        gf_log ("cli", GF_LOG_DEBUG, "Returning %d", ret);

        return ret;
}

int32_t
gf_cli3_1_start_volume (call_frame_t *frame, xlator_t *this,
                         void *data)
{
        gf1_cli_start_vol_req   *req = NULL;
        int                     ret = 0;
        cli_local_t             *local = NULL;

        if (!frame || !this ||  !data) {
                ret = -1;
                goto out;
        }

        req = data;
        local = cli_local_get ();

        if (local) {
                local->u.start_vol.volname = req->volname;
                local->u.start_vol.flags = req->flags;
                frame->local = local;
        }

        ret = cli_cmd_submit (req, frame, cli_rpc_prog,
                              GLUSTER_CLI_START_VOLUME, NULL,
                              gf_xdr_from_cli_start_vol_req,
                              this, gf_cli3_1_start_volume_cbk);

out:
        gf_log ("cli", GF_LOG_DEBUG, "Returning %d", ret);

        return ret;
}

int32_t
gf_cli3_1_stop_volume (call_frame_t *frame, xlator_t *this,
                         void *data)
{
        gf1_cli_stop_vol_req   req = {0,};
        int                    ret = 0;
        cli_local_t            *local = NULL;

        if (!frame || !this ||  !data) {
                ret = -1;
                goto out;
        }

        req = *((gf1_cli_stop_vol_req*)data);
        local = cli_local_get ();

        if (local) {
                local->u.stop_vol.volname = req.volname;
                local->u.stop_vol.flags = req.flags;
                frame->local = local;
        }

        ret = cli_cmd_submit (&req, frame, cli_rpc_prog,
                              GLUSTER_CLI_STOP_VOLUME, NULL,
                              gf_xdr_from_cli_stop_vol_req,
                              this, gf_cli3_1_stop_volume_cbk);

out:
        gf_log ("cli", GF_LOG_DEBUG, "Returning %d", ret);

        return ret;
}

int32_t
gf_cli3_1_defrag_volume (call_frame_t *frame, xlator_t *this,
                         void *data)
{
        gf1_cli_defrag_vol_req  req     = {0,};
        int                     ret     = 0;
        cli_local_t            *local   = NULL;
        char                   *volname = NULL;
        char                   *cmd_str = NULL;
        dict_t                 *dict    = NULL;

        if (!frame || !this ||  !data) {
                ret = -1;
                goto out;
        }

        dict = data;

        ret = dict_get_str (dict, "volname", &volname);
        if (ret)
                gf_log ("", GF_LOG_DEBUG, "error");

        ret = dict_get_str (dict, "command", &cmd_str);
        if (ret) {
                gf_log ("", GF_LOG_DEBUG, "error");
                goto out;
        }

        if (strcmp (cmd_str, "start") == 0) {
                req.cmd = GF_DEFRAG_CMD_START;
                ret = dict_get_str (dict, "start-type", &cmd_str);
                if (!ret) {
                        if (strcmp (cmd_str, "fix-layout") == 0) {
                                req.cmd = GF_DEFRAG_CMD_START_LAYOUT_FIX;
                        }
                        if (strcmp (cmd_str, "migrate-data") == 0) {
                                req.cmd = GF_DEFRAG_CMD_START_MIGRATE_DATA;
                        }
                }
                goto done;
        }
        if (strcmp (cmd_str, "stop") == 0) {
                req.cmd = GF_DEFRAG_CMD_STOP;
                goto done;
        }
        if (strcmp (cmd_str, "status") == 0) {
                req.cmd = GF_DEFRAG_CMD_STATUS;
        }

done:
        local = cli_local_get ();

        if (local) {
                local->u.defrag_vol.volname = gf_strdup (volname);
                local->u.defrag_vol.cmd = req.cmd;
                frame->local = local;
        }

        req.volname = volname;

        ret = cli_cmd_submit (&req, frame, cli_rpc_prog,
                              GLUSTER_CLI_DEFRAG_VOLUME, NULL,
                              gf_xdr_from_cli_defrag_vol_req,
                              this, gf_cli3_1_defrag_volume_cbk);

out:
        gf_log ("cli", GF_LOG_DEBUG, "Returning %d", ret);

        return ret;
}

int32_t
gf_cli3_1_rename_volume (call_frame_t *frame, xlator_t *this,
                         void *data)
{
        gf1_cli_rename_vol_req  req = {0,};
        int                     ret = 0;
        dict_t                  *dict = NULL;

        if (!frame || !this ||  !data) {
                ret = -1;
                goto out;
        }

        dict = data;

        ret = dict_get_str (dict, "old-volname", &req.old_volname);

        if (ret)
                goto out;

        ret = dict_get_str (dict, "new-volname", &req.new_volname);

        if (ret)
                goto out;

        ret = cli_cmd_submit (&req, frame, cli_rpc_prog,
                              GLUSTER_CLI_RENAME_VOLUME, NULL,
                              gf_xdr_from_cli_rename_vol_req,
                              this, gf_cli3_1_rename_volume_cbk);

out:
        gf_log ("cli", GF_LOG_DEBUG, "Returning %d", ret);

        return ret;
}

int32_t
gf_cli3_1_reset_volume (call_frame_t *frame, xlator_t *this, 
                        void *data)
{
        gf1_cli_reset_vol_req     req = {0,};
        int                     ret = 0;
        dict_t                  *dict = NULL;

        if (!frame || !this ||  !data) {
                ret = -1;
                goto out;
        }

        dict = data;

        ret = dict_get_str (dict, "volname", &req.volname);

        if (ret)
                goto out;

        ret = dict_allocate_and_serialize (dict,
                                           &req.dict.dict_val,
                                           (size_t *)&req.dict.dict_len);
        if (ret < 0) {
                gf_log (this->name, GF_LOG_ERROR,
                        "failed to get serialized length of dict");
                goto out;
        }


        ret = cli_cmd_submit (&req, frame, cli_rpc_prog,
                               GLUSTER_CLI_RESET_VOLUME, NULL,
                               gf_xdr_from_cli_reset_vol_req,
                               this, gf_cli3_1_reset_volume_cbk);

out:
                gf_log ("cli", GF_LOG_DEBUG, "Returning %d", ret);

        return ret;
}

int32_t
gf_cli3_1_set_volume (call_frame_t *frame, xlator_t *this,
                         void *data)
{
        gf1_cli_set_vol_req     req = {0,};
        int                     ret = 0;
        dict_t                  *dict = NULL;

        if (!frame || !this ||  !data) {
                ret = -1;
                goto out;
        }

        dict = data;

        ret = dict_get_str (dict, "volname", &req.volname);

        if (ret)
                goto out;

        ret = dict_allocate_and_serialize (dict,
                                           &req.dict.dict_val,
                                           (size_t *)&req.dict.dict_len);
        if (ret < 0) {
                gf_log (this->name, GF_LOG_DEBUG,
                        "failed to get serialized length of dict");
                goto out;
        }


        ret = cli_cmd_submit (&req, frame, cli_rpc_prog,
                              GLUSTER_CLI_SET_VOLUME, NULL,
                              gf_xdr_from_cli_set_vol_req,
                              this, gf_cli3_1_set_volume_cbk);

out:
        gf_log ("cli", GF_LOG_DEBUG, "Returning %d", ret);

        return ret;
}

int32_t
gf_cli3_1_add_brick (call_frame_t *frame, xlator_t *this,
                         void *data)
{
        gf1_cli_add_brick_req  req = {0,};
        int                     ret = 0;
        dict_t                  *dict = NULL;

        if (!frame || !this ||  !data) {
                ret = -1;
                goto out;
        }

        dict = data;

        ret = dict_get_str (dict, "volname", &req.volname);

        if (ret)
                goto out;

        ret = dict_get_int32 (dict, "count", &req.count);
        if (ret)
                goto out;


        ret = dict_allocate_and_serialize (dict,
                                           &req.bricks.bricks_val,
                                           (size_t *)&req.bricks.bricks_len);
        if (ret < 0) {
                gf_log (this->name, GF_LOG_DEBUG,
                        "failed to get serialized length of dict");
                goto out;
        }

        ret = cli_cmd_submit (&req, frame, cli_rpc_prog,
                              GLUSTER_CLI_ADD_BRICK, NULL,
                              gf_xdr_from_cli_add_brick_req,
                              this, gf_cli3_1_add_brick_cbk);

out:
        gf_log ("cli", GF_LOG_DEBUG, "Returning %d", ret);

        if (req.bricks.bricks_val) {
                GF_FREE (req.bricks.bricks_val);
        }

        return ret;
}

int32_t
gf_cli3_1_remove_brick (call_frame_t *frame, xlator_t *this,
                         void *data)
{
        gf1_cli_remove_brick_req  req = {0,};
        int                       ret = 0;
        dict_t                    *dict = NULL;

        if (!frame || !this ||  !data) {
                ret = -1;
                goto out;
        }

        dict = data;

        ret = dict_get_str (dict, "volname", &req.volname);

        if (ret)
                goto out;

        ret = dict_get_int32 (dict, "count", &req.count);

        if (ret)
                goto out;

        ret = dict_allocate_and_serialize (dict,
                                           &req.bricks.bricks_val,
                                           (size_t *)&req.bricks.bricks_len);
        if (ret < 0) {
                gf_log (this->name, GF_LOG_DEBUG,
                        "failed to get serialized length of dict");
                goto out;
        }

        ret = cli_cmd_submit (&req, frame, cli_rpc_prog,
                              GLUSTER_CLI_REMOVE_BRICK, NULL,
                              gf_xdr_from_cli_remove_brick_req,
                              this, gf_cli3_1_remove_brick_cbk);


out:
        gf_log ("cli", GF_LOG_DEBUG, "Returning %d", ret);

        if (req.bricks.bricks_val) {
                GF_FREE (req.bricks.bricks_val);
        }

        return ret;
}

int32_t
gf_cli3_1_replace_brick (call_frame_t *frame, xlator_t *this,
                         void *data)
{
        gf1_cli_replace_brick_req   req        = {0,};
        int                         ret        = 0;
        cli_local_t                *local      = NULL;
        dict_t                     *dict       = NULL;
        char                       *src_brick  = NULL;
        char                       *dst_brick  = NULL;

        if (!frame || !this ||  !data) {
                ret = -1;
                goto out;
        }

	dict = data;

        local = cli_local_get ();
        if (!local) {
                ret = -1;
                gf_log (this->name, GF_LOG_ERROR,
                        "Out of memory");
                goto out;
        }

        local->u.replace_brick.dict = dict_ref (dict);
        frame->local                = local;

        ret = dict_get_int32 (dict, "operation", (int32_t *)&req.op);
        if (ret) {
                gf_log (this->name, GF_LOG_DEBUG,
                        "dict_get on operation failed");
                goto out;
        }
        ret = dict_get_str (dict, "volname", &req.volname);
        if (ret) {
                gf_log (this->name, GF_LOG_DEBUG,
                        "dict_get on volname failed");
                goto out;
        }

        local->u.replace_brick.volname = gf_strdup (req.volname);
        if (!local->u.replace_brick.volname) {
                gf_log (this->name, GF_LOG_ERROR,
                        "Out of memory");
                ret = -1;
                goto out;
        }

        ret = dict_get_str (dict, "src-brick", &src_brick);
        if (ret) {
                gf_log (this->name, GF_LOG_DEBUG,
                        "dict_get on src-brick failed");
                goto out;
        }

        ret = dict_get_str (dict, "dst-brick", &dst_brick);
        if (ret) {
                gf_log (this->name, GF_LOG_DEBUG,
                        "dict_get on dst-brick failed");
                goto out;
        }

        gf_log (this->name, GF_LOG_DEBUG,
                "Recevied command replace-brick %s with "
                "%s with operation=%d", src_brick,
                dst_brick, req.op);


        ret = dict_allocate_and_serialize (dict,
                                           &req.bricks.bricks_val,
                                           (size_t *)&req.bricks.bricks_len);
        if (ret < 0) {
                gf_log (this->name, GF_LOG_DEBUG,
                        "failed to get serialized length of dict");
                goto out;
        }

        ret = cli_cmd_submit (&req, frame, cli_rpc_prog,
                              GLUSTER_CLI_REPLACE_BRICK, NULL,
                              gf_xdr_from_cli_replace_brick_req,
                              this, gf_cli3_1_replace_brick_cbk);

out:
        gf_log ("cli", GF_LOG_DEBUG, "Returning %d", ret);

        if (req.bricks.bricks_val) {
                GF_FREE (req.bricks.bricks_val);
        }

        return ret;
}

int32_t
gf_cli3_1_log_filename (call_frame_t *frame, xlator_t *this,
                        void *data)
{
        gf1_cli_log_filename_req  req = {0,};
        int                       ret = 0;
        dict_t                   *dict = NULL;

        if (!frame || !this ||  !data) {
                ret = -1;
                goto out;
        }

        dict = data;

        ret = dict_get_str (dict, "volname", &req.volname);
        if (ret)
                goto out;

        ret = dict_get_str (dict, "brick", &req.brick);
        if (ret)
                req.brick = "";

        ret = dict_get_str (dict, "path", &req.path);
        if (ret)
                goto out;

        ret = cli_cmd_submit (&req, frame, cli_rpc_prog,
                              GLUSTER_CLI_LOG_FILENAME, NULL,
                              gf_xdr_from_cli_log_filename_req,
                              this, gf_cli3_1_log_filename_cbk);

out:
        gf_log ("cli", GF_LOG_DEBUG, "Returning %d", ret);

        return ret;
}


int32_t
gf_cli3_1_log_locate (call_frame_t *frame, xlator_t *this,
                      void *data)
{
        gf1_cli_log_locate_req  req = {0,};
        int                     ret = 0;
        dict_t                 *dict = NULL;

        if (!frame || !this ||  !data) {
                ret = -1;
                goto out;
        }

        dict = data;

        ret = dict_get_str (dict, "volname", &req.volname);
        if (ret)
                goto out;

        ret = dict_get_str (dict, "brick", &req.brick);
        if (ret)
                req.brick = "";

        ret = cli_cmd_submit (&req, frame, cli_rpc_prog,
                              GLUSTER_CLI_LOG_LOCATE, NULL,
                              gf_xdr_from_cli_log_locate_req,
                              this, gf_cli3_1_log_locate_cbk);

out:
        gf_log ("cli", GF_LOG_DEBUG, "Returning %d", ret);

        return ret;
}

int32_t
gf_cli3_1_log_rotate (call_frame_t *frame, xlator_t *this,
                      void *data)
{
        gf1_cli_log_locate_req  req = {0,};
        int                       ret = 0;
        dict_t                   *dict = NULL;

        if (!frame || !this ||  !data) {
                ret = -1;
                goto out;
        }

        dict = data;

        ret = dict_get_str (dict, "volname", &req.volname);
        if (ret)
                goto out;

        ret = dict_get_str (dict, "brick", &req.brick);
        if (ret)
                req.brick = "";

        ret = cli_cmd_submit (&req, frame, cli_rpc_prog,
                              GLUSTER_CLI_LOG_ROTATE, NULL,
                              gf_xdr_from_cli_log_rotate_req,
                              this, gf_cli3_1_log_rotate_cbk);


out:
        gf_log ("cli", GF_LOG_DEBUG, "Returning %d", ret);

        return ret;
}

int32_t
gf_cli3_1_sync_volume (call_frame_t *frame, xlator_t *this,
                       void *data)
{
        int               ret = 0;

        if (!frame || !this || !data) {
                ret = -1;
                goto out;
        }

        ret = cli_cmd_submit ((gf1_cli_sync_volume_req*)data, frame,
                              cli_rpc_prog, GLUSTER_CLI_SYNC_VOLUME,
                              NULL, gf_xdr_from_cli_sync_volume_req,
                              this, gf_cli3_1_sync_volume_cbk);

out:
        gf_log ("cli", GF_LOG_DEBUG, "Returning %d", ret);

        return ret;
}

int32_t
gf_cli3_1_getspec (call_frame_t *frame, xlator_t *this,
                         void *data)
{
        gf_getspec_req          req = {0,};
        int                     ret = 0;
        dict_t                  *dict = NULL;

        if (!frame || !this ||  !data) {
                ret = -1;
                goto out;
        }

        dict = data;

        ret = dict_get_str (dict, "volid", &req.key);
        if (ret)
                goto out;

        ret = cli_cmd_submit (&req, frame, &cli_handshake_prog,
                              GF_HNDSK_GETSPEC, NULL,
                              xdr_from_getspec_req,
                              this, gf_cli3_1_getspec_cbk);

out:
        gf_log ("cli", GF_LOG_DEBUG, "Returning %d", ret);

        return ret;
}

int32_t
gf_cli3_1_quota (call_frame_t *frame, xlator_t *this,
                 void *data)
{
        gf1_cli_quota_req   req = {0,};
        int                 ret = 0;
        dict_t             *dict = NULL;

        if (!frame || !this ||  !data) {
                ret = -1;
                goto out;
        }

        dict = data;

        ret = dict_get_str (dict, "volname", &req.volname);

        if (ret)
                goto out;

        ret = dict_allocate_and_serialize (dict,
                                           &req.dict.dict_val,
                                           (size_t *)&req.dict.dict_len);
        if (ret < 0) {
                gf_log (this->name, GF_LOG_ERROR,
                        "failed to get serialized length of dict");
                goto out;
        }


        ret = cli_cmd_submit (&req, frame, cli_rpc_prog,
                               GLUSTER_CLI_QUOTA, NULL,
                               gf_xdr_from_cli_quota_req,
                               this, gf_cli3_1_quota_cbk);

        GF_FREE (req.dict.dict_val);
out:
        return ret;
}

int32_t
gf_cli3_1_pmap_b2p (call_frame_t *frame, xlator_t *this, void *data)
{
        pmap_port_by_brick_req  req = {0,};
        int                     ret = 0;
        dict_t                  *dict = NULL;

        if (!frame || !this ||  !data) {
                ret = -1;
                goto out;
        }

        dict = data;

        ret = dict_get_str (dict, "brick", &req.brick);
        if (ret)
                goto out;

        ret = cli_cmd_submit (&req, frame, &cli_pmap_prog,
                              GF_PMAP_PORTBYBRICK, NULL,
                              xdr_from_pmap_port_by_brick_req,
                              this, gf_cli3_1_pmap_b2p_cbk);

out:
        gf_log ("cli", GF_LOG_DEBUG, "Returning %d", ret);

        return ret;
}

static int
gf_cli3_1_fsm_log_cbk (struct rpc_req *req, struct iovec *iov,
                       int count, void *myframe)
{
        gf1_cli_fsm_log_rsp        rsp   = {0,};
        int                        ret   = -1;
        dict_t                     *dict = NULL;
        int                        tr_count = 0;
        char                       key[256] = {0};
        int                        i = 0;
        char                       *old_state = NULL;
        char                       *new_state = NULL;
        char                       *event = NULL;
        char                       *time = NULL;

        if (-1 == req->rpc_status) {
                goto out;
        }

        ret = gf_xdr_to_cli_fsm_log_rsp (*iov, &rsp);
        if (ret < 0) {
                gf_log ("", GF_LOG_ERROR, "error");
                goto out;
        }

        if (rsp.op_ret) {
                if (strcmp (rsp.op_errstr, "")) {
                        cli_out (rsp.op_errstr);
                } else if (rsp.op_ret) {
                        cli_out ("fsm log unsuccessful");
                }
                ret = rsp.op_ret;
                goto out;
        }

        dict = dict_new ();
        if (!dict) {
                ret = -1;
                goto out;
        }

        ret = dict_unserialize (rsp.fsm_log.fsm_log_val,
                                rsp.fsm_log.fsm_log_len,
                                &dict);

        if (ret) {
                cli_out ("bad response");
                goto out;
        }

        ret = dict_get_int32 (dict, "count", &tr_count);
        if (tr_count)
                cli_out("number of transitions: %d", tr_count);
        else
                cli_out("No transitions");
        for (i = 0; i < tr_count; i++) {
                memset (key, 0, sizeof (key));
                snprintf (key, sizeof (key), "log%d-old-state", i);
                ret = dict_get_str (dict, key, &old_state);
                if (ret)
                        goto out;

                memset (key, 0, sizeof (key));
                snprintf (key, sizeof (key), "log%d-event", i);
                ret = dict_get_str (dict, key, &event);
                if (ret)
                        goto out;

                memset (key, 0, sizeof (key));
                snprintf (key, sizeof (key), "log%d-new-state", i);
                ret = dict_get_str (dict, key, &new_state);
                if (ret)
                        goto out;

                memset (key, 0, sizeof (key));
                snprintf (key, sizeof (key), "log%d-time", i);
                ret = dict_get_str (dict, key, &time);
                if (ret)
                        goto out;
                cli_out ("Old State: [%s]\n"
                         "New State: [%s]\n"
                         "Event    : [%s]\n"
                         "timestamp: [%s]\n", old_state, new_state, event, time);
        }

        ret = rsp.op_ret;

out:
        cli_cmd_broadcast_response (ret);
        return ret;
}

int32_t
gf_cli3_1_fsm_log (call_frame_t *frame, xlator_t *this, void *data)
{
        int                        ret = -1;
        gf1_cli_fsm_log_req        req = {0,};

        GF_ASSERT (frame);
        GF_ASSERT (this);
        GF_ASSERT (data);

        if (!frame || !this || !data)
                goto out;
        req.name = data;
        ret = cli_cmd_submit (&req, frame, cli_rpc_prog,
                              GLUSTER_CLI_FSM_LOG, NULL,
                              gf_xdr_from_cli_fsm_log_req,
                              this, gf_cli3_1_fsm_log_cbk);

out:
        gf_log ("cli", GF_LOG_DEBUG, "Returning %d", ret);

        return ret;
}


int
gf_cli3_1_gsync_config_command (dict_t *dict)
{
        char  cmd[PATH_MAX] = {0,};
        char *subop         = NULL;
        char *gwd           = NULL;
        char *slave         = NULL;
        char *master        = NULL;
        char *op_name       = NULL;

        if (dict_get_str (dict, "subop", &subop) != 0)
                return -1;

        if (strcmp (subop, "get") != 0 && strcmp (subop, "get-all") != 0) {
                cli_out (GEOREP" config updated successfully");
                return 0;
        }

        if (dict_get_str (dict, "glusterd_workdir", &gwd) != 0 ||
            dict_get_str (dict, "slave", &slave) != 0)
                return -1;

        if (dict_get_str (dict, "master", &master) != 0)
                master = "";
        if (dict_get_str (dict, "op_name", &op_name) != 0)
                op_name = "";

        snprintf (cmd, PATH_MAX,
                  GSYNCD_PREFIX"/gsyncd -c %s/"GSYNC_CONF" %s%s %s --config-%s%s%s",
                  gwd, *master ? ":" : "", master, slave,
                  subop, *op_name ? " " : "", op_name);

        return system (cmd) ? -1 : 0;
}

int
gf_cli3_1_gsync_out_status (dict_t *dict)
{
        int              gsync_count = 0;
        int              i = 0;
        int              ret = 0;
        char             mst[PATH_MAX] = {0, };
        char             slv[PATH_MAX]= {0, };
        char             sts[PATH_MAX] = {0, };
        char             hyphens[81] = {0, };
        char             *mst_val = NULL;
        char             *slv_val = NULL;
        char             *sts_val = NULL;

        cli_out ("%-20s %-50s %-10s", "MASTER", "SLAVE", "STATUS");

        for (i=0; i<sizeof(hyphens)-1; i++)
                hyphens[i] = '-';

        cli_out ("%s", hyphens);


        ret = dict_get_int32 (dict, "gsync-count", &gsync_count);
        if (ret) {
                gf_log ("cli", GF_LOG_INFO, "No active geo-replication sessions"
                        "present for the selected");
                ret = 0;
                goto out;
        }

        for (i = 1; i <= gsync_count; i++) {
                snprintf (mst, sizeof(mst), "master%d", i);
                snprintf (slv, sizeof(slv), "slave%d", i);
                snprintf (sts, sizeof(sts), "status%d", i);

                ret = dict_get_str (dict, mst, &mst_val);
                if (ret)
                        goto out;

                ret = dict_get_str (dict, slv, &slv_val);
                if (ret)
                        goto out;

                ret = dict_get_str (dict, sts, &sts_val);
                if (ret)
                        goto out;

                cli_out ("%-20s %-50s %-10s", mst_val,
                         slv_val, sts_val);

        }

 out:
        return ret;

}

int
gf_cli3_1_gsync_set_cbk (struct rpc_req *req, struct iovec *iov,
                         int count, void *myframe)
{
        int                     ret     = 0;
        gf1_cli_gsync_set_rsp   rsp     = {0, };
        dict_t                  *dict   = NULL;
        char                    *gsync_status = NULL;
        char                    *master = NULL;
        char                    *slave  = NULL;

        if (req->rpc_status == -1) {
                ret = -1;
                goto out;
        }

        ret = gf_xdr_to_cli_gsync_set_rsp (*iov, &rsp);
        if (ret < 0) {
                gf_log ("", GF_LOG_ERROR,
                        "Unable to get response structure");
                goto out;
        }

        dict = dict_new ();

        if (!dict) {
                ret = -1;
                goto out;
        }

        ret = dict_unserialize (rsp.dict.dict_val, rsp.dict.dict_len, &dict);

        if (ret)
                goto out;

        if (rsp.op_ret) {
                cli_out ("%s", rsp.op_errstr ? rsp.op_errstr :
                         GEOREP" command unsuccessful");
                ret = rsp.op_ret;
                goto out;
        }

        switch (rsp.type) {
                case GF_GSYNC_OPTION_TYPE_START:
                case GF_GSYNC_OPTION_TYPE_STOP:
                        if (dict_get_str (dict, "master", &master) != 0)
                                master = "???";
                        if (dict_get_str (dict, "slave", &slave) != 0)
                                slave = "???";

                        cli_out ("%s " GEOREP " session between %s & %s"
                                 " has been successful",
                                 rsp.type == GF_GSYNC_OPTION_TYPE_START ?
                                  "Starting" : "Stopping",
                                 master, slave);
                break;

                case GF_GSYNC_OPTION_TYPE_CONFIG:
                        ret = gf_cli3_1_gsync_config_command (dict);
                break;

                case GF_GSYNC_OPTION_TYPE_STATUS:
                        ret = gf_cli3_1_gsync_out_status (dict);
                        goto out;
                default:
                        cli_out (GEOREP" command executed successfully");
        }

out:

        cli_cmd_broadcast_response (ret);

        return ret;
}

int32_t
gf_cli3_1_gsync_set (call_frame_t *frame, xlator_t *this,
                     void *data)
{
        int                      ret    = 0;
        dict_t                  *dict   = NULL;
        gf1_cli_gsync_set_req    req;

        if (!frame || !this || !data) {
                ret = -1;
                goto out;
        }

        dict = data;

        ret = dict_allocate_and_serialize (dict,
                                           &req.dict.dict_val,
                                           (size_t *) &req.dict.dict_len);
        if (ret < 0) {
                gf_log (this->name, GF_LOG_ERROR,
                        "failed to serialize the data");

                goto out;
        }

        ret = cli_cmd_submit (&req, frame, cli_rpc_prog,
                              GLUSTER_CLI_GSYNC_SET, NULL,
                              gf_xdr_from_cli_gsync_set_req,
                              this, gf_cli3_1_gsync_set_cbk);

out:
        return ret;
}

void*
cli_profile_info_elem (void *a, int index)
{
        return ((cli_profile_info_t *)a) + index;
}

int
cli_profile_info_percentage_cmp (void *a, void *b)
{
        cli_profile_info_t *ia = NULL;
        cli_profile_info_t *ib = NULL;
        int                ret = 0;

        ia = a;
        ib = b;
        if (ia->percentage_avg_latency < ib->percentage_avg_latency)
                ret = -1;
        else if (ia->percentage_avg_latency > ib->percentage_avg_latency)
                ret = 1;
        else
                ret = 0;
        return ret;
}

void
cli_profile_info_swap (void *a, void *b)
{
        cli_profile_info_t *ia = NULL;
        cli_profile_info_t *ib = NULL;
        cli_profile_info_t tmp = {0};

        ia = a;
        ib = b;
        tmp = *ia;
        *ia = *ib;
        *ib = tmp;
}

void
cmd_profile_volume_brick_out (dict_t *dict, int count, int interval)
{
        char                    key[256] = {0};
        int                     i = 0;
        uint64_t                sec = 0;
        uint64_t                r_count = 0;
        uint64_t                w_count = 0;
        char                    *brick = NULL;
        uint64_t                rb_counts[32] = {0};
        uint64_t                wb_counts[32] = {0};
        cli_profile_info_t      profile_info[GF_FOP_MAXVALUE] = {{0}};
        char                    output[128] = {0};
        int                     per_line = 0;
        char                    read_blocks[128] = {0};
        char                    write_blocks[128] = {0};
        int                     index = 0;
        int                     is_header_printed = 0;
        int                     ret = 0;
        double                  total_percentage_latency = 0;

        memset (key, 0, sizeof (key));
        snprintf (key, sizeof (key), "%d-brick", count);
        ret = dict_get_str (dict, key, &brick);
        for (i = 0; i < 32; i++) {
                memset (key, 0, sizeof (key));
                snprintf (key, sizeof (key), "%d-%d-read-%d", count,
                          interval, (1 << i));
                ret = dict_get_uint64 (dict, key, &rb_counts[i]);
        }

        for (i = 0; i < 32; i++) {
                memset (key, 0, sizeof (key));
                snprintf (key, sizeof (key), "%d-%d-write-%d", count, interval,
                          (1<<i));
                ret = dict_get_uint64 (dict, key, &wb_counts[i]);
        }

        for (i = 0; i < GF_FOP_MAXVALUE; i++) {
                memset (key, 0, sizeof (key));
                snprintf (key, sizeof (key), "%d-%d-%d-hits", count,
                          interval, i);
                ret = dict_get_uint64 (dict, key, &profile_info[i].fop_hits);

                memset (key, 0, sizeof (key));
                snprintf (key, sizeof (key), "%d-%d-%d-avglatency", count,
                          interval, i);
                ret = dict_get_double (dict, key, &profile_info[i].avg_latency);

                memset (key, 0, sizeof (key));
                snprintf (key, sizeof (key), "%d-%d-%d-minlatency", count,
                          interval, i);
                ret = dict_get_double (dict, key, &profile_info[i].min_latency);

                memset (key, 0, sizeof (key));
                snprintf (key, sizeof (key), "%d-%d-%d-maxlatency", count,
                          interval, i);
                ret = dict_get_double (dict, key, &profile_info[i].max_latency);
                profile_info[i].fop_name = gf_fop_list[i];

                total_percentage_latency +=
                       (profile_info[i].fop_hits * profile_info[i].avg_latency);
        }
        if (total_percentage_latency) {
                for (i = 0; i < GF_FOP_MAXVALUE; i++) {
                        profile_info[i].percentage_avg_latency = 100 * (
                     (profile_info[i].avg_latency* profile_info[i].fop_hits) /
                                total_percentage_latency);
                }
                gf_array_insertionsort (profile_info, 1, GF_FOP_MAXVALUE - 1,
                                    sizeof (cli_profile_info_t),
                                    cli_profile_info_percentage_cmp);
        }
        memset (key, 0, sizeof (key));
        snprintf (key, sizeof (key), "%d-%d-duration", count, interval);
        ret = dict_get_uint64 (dict, key, &sec);

        memset (key, 0, sizeof (key));
        snprintf (key, sizeof (key), "%d-%d-total-read", count, interval);
        ret = dict_get_uint64 (dict, key, &r_count);

        memset (key, 0, sizeof (key));
        snprintf (key, sizeof (key), "%d-%d-total-write", count, interval);
        ret = dict_get_uint64 (dict, key, &w_count);

        if (ret == 0) {
                cli_out ("Brick: %s", brick);
        }

        if (interval == -1)
                cli_out ("Cumulative Stats:");
        else
                cli_out ("Interval %d Stats:", interval);
        snprintf (output, sizeof (output), "%12s", "Block Size: ");
        snprintf (read_blocks, sizeof (read_blocks), "%12s", "Read: ");
        snprintf (write_blocks, sizeof (write_blocks), "%12s", "Write: ");
        index = 12;
        for (i = 0; i < 32; i++) {
                if ((rb_counts[i] == 0) && (wb_counts[i] == 0))
                        continue;
                per_line++;
                snprintf (output+index, sizeof (output)-index, "%19db+ ", (1<<i));
                if (rb_counts[i]) {
                        snprintf (read_blocks+index, sizeof (read_blocks)-index,
                                  "%21"PRId64" ", rb_counts[i]);
                } else {
                        snprintf (read_blocks+index, sizeof (read_blocks)-index,
                                  "%21s ", "0");
                }
                if (wb_counts[i]) {
                        snprintf (write_blocks+index, sizeof (write_blocks)-index,
                                  "%21"PRId64" ", wb_counts[i]);
                } else {
                        snprintf (write_blocks+index, sizeof (write_blocks)-index,
                                  "%21s ", "0");
                }
                index += 22;
                if (per_line == 3) {
                        cli_out (output);
                        cli_out (read_blocks);
                        cli_out (write_blocks);
                        cli_out ("");
                        per_line = 0;
                        memset (output, 0, sizeof (output));
                        memset (read_blocks, 0, sizeof (read_blocks));
                        memset (write_blocks, 0, sizeof (write_blocks));
                        snprintf (output, sizeof (output), "%12s", "Block Size: ");
                        snprintf (read_blocks, sizeof (read_blocks), "%12s",
                                  "Read: ");
                        snprintf (write_blocks, sizeof (write_blocks), "%12s",
                                  "Write: ");
                        index = 12;
                }
        }

        if (per_line != 0) {
                cli_out (output);
                cli_out (read_blocks);
                cli_out (write_blocks);
        }
        for (i = 0; i < GF_FOP_MAXVALUE; i++) {
                if (profile_info[i].fop_hits == 0)
                        continue;
                if (is_header_printed == 0) {
                        cli_out ("%11s %11s %11s %11s %20s %10s", "%-latency", "Avg-latency", "Min-Latency", "Max-Latency", "calls", "Fop");
                        cli_out ("%11s %11s %11s %11s %20s %10s", "---------", "-----------", "-----------", "-----------", "-----", "----");
                        is_header_printed = 1;
                }
                if (profile_info[i].fop_hits) {
                        cli_out ("%11.2lf %11.2lf %11.2lf %11.2lf %20"PRId64" %10s",
                                 profile_info[i].percentage_avg_latency,
                                 profile_info[i].avg_latency,
                                 profile_info[i].min_latency,
                                 profile_info[i].max_latency,
                                 profile_info[i].fop_hits,
                                 profile_info[i].fop_name);
                }
        }
        cli_out ("");
        cli_out ("%12s : %"PRId64, "Duration", sec);
        cli_out ("%12s : %"PRId64, "BytesRead", r_count);
        cli_out ("%12s : %"PRId64, "BytesWritten", w_count);
        cli_out ("");
}

int32_t
gf_cli3_1_profile_volume_cbk (struct rpc_req *req, struct iovec *iov,
                              int count, void *myframe)
{
        gf1_cli_stats_volume_rsp          rsp   = {0,};
        int                               ret   = -1;
        dict_t                            *dict = NULL;
	gf1_cli_stats_op                  op = GF_CLI_STATS_NONE;
        char                              key[256] = {0};
        int                               interval = 0;
        int                               i = 1;
        int32_t                           brick_count = 0;
        char                              *volname = NULL;
        if (-1 == req->rpc_status) {
                goto out;
        }

        gf_log ("cli", GF_LOG_DEBUG, "Received resp to profile");
        ret = gf_xdr_to_cli_stats_volume_rsp (*iov, &rsp);
        if (ret < 0) {
                gf_log ("", GF_LOG_ERROR, "error");
                goto out;
        }

        dict = dict_new ();

        if (!dict) {
                ret = -1;
                goto out;
        }

        ret = dict_unserialize (rsp.stats_info.stats_info_val,
                                rsp.stats_info.stats_info_len,
                                &dict);

        if (ret) {
                gf_log ("", GF_LOG_ERROR,
                                "Unable to allocate memory");
                goto out;
        } else {
                dict->extra_stdfree = rsp.stats_info.stats_info_val;
        }

        ret = dict_get_str (dict, "volname", &volname);
        if (ret)
                goto out;

        ret = dict_get_int32 (dict, "op", (int32_t*)&op);
        if (ret)
                goto out;

        if (rsp.op_ret && strcmp (rsp.op_errstr, "")) {
                cli_out (rsp.op_errstr);
        } else {
                switch (op) {
                case GF_CLI_STATS_START:
                        cli_out ("Starting volume profile on %s has been %s ",
                                 volname,
                                 (rsp.op_ret) ? "unsuccessful": "successful");
                        break;
                case GF_CLI_STATS_STOP:
                        cli_out ("Stopping volume profile on %s has been %s ",
                                 volname,
                                 (rsp.op_ret) ? "unsuccessful": "successful");
                        break;
                case GF_CLI_STATS_INFO:
                        break;
                default:
                        cli_out ("volume profile on %s has been %s ",
                                 volname,
                                 (rsp.op_ret) ? "unsuccessful": "successful");
                        break;
                }
        }

        if (rsp.op_ret) {
                ret = rsp.op_ret;
                goto out;
        }

        if (op != GF_CLI_STATS_INFO) {
                ret = 0;
                goto out;
        }

        ret = dict_get_int32 (dict, "count", &brick_count);
        if (ret)
                goto out;
        while (i <= brick_count) {
                snprintf (key, sizeof (key), "%d-cumulative", i);
                ret = dict_get_int32 (dict, key, &interval);
                if (ret == 0) {
                        cmd_profile_volume_brick_out (dict, i, interval);
                }
                snprintf (key, sizeof (key), "%d-interval", i);
                ret = dict_get_int32 (dict, key, &interval);
                if (ret == 0) {
                        cmd_profile_volume_brick_out (dict, i, interval);
                }
                i++;
        }
        ret = rsp.op_ret;

out:
        if (dict)
                dict_unref (dict);
        if (rsp.op_errstr)
                free (rsp.op_errstr);
        cli_cmd_broadcast_response (ret);
        return ret;
}

int32_t
gf_cli3_1_profile_volume (call_frame_t *frame, xlator_t *this, void *data)
{
        int                        ret   = -1;
        gf1_cli_stats_volume_req   req   = {0,};
        dict_t			   *dict = NULL;

        GF_ASSERT (frame);
        GF_ASSERT (this);
        GF_ASSERT (data);

        if (!frame || !this || !data)
                goto out;
        dict = data;
        ret = dict_get_str (dict, "volname", &req.volname);
        if (ret)
                goto out;

        ret = dict_get_int32 (dict, "op", (int32_t*)&req.op);
        if (ret)
                goto out;

        ret = cli_cmd_submit (&req, frame, cli_rpc_prog,
                              GLUSTER_CLI_PROFILE_VOLUME, NULL,
                              gf_xdr_from_cli_stats_volume_req,
                              this, gf_cli3_1_profile_volume_cbk);

out:
        gf_log ("cli", GF_LOG_DEBUG, "Returning %d", ret);
        return ret;
}

int32_t
gf_cli3_1_top_volume_cbk (struct rpc_req *req, struct iovec *iov,
                              int count, void *myframe)
{
        gf1_cli_stats_volume_rsp        rsp   = {0,};
        int                               ret   = -1;
        dict_t                            *dict = NULL;
        gf1_cli_stats_op                op = GF_CLI_STATS_NONE;
        char                              key[256] = {0};
        int                               i = 0;
        int32_t                           brick_count = 0;
        char                              brick[1024];
        int32_t                           members = 0;
        char                              *filename;
        char                              *bricks;
        uint64_t                           value = 0;
        int32_t                           j = 0;
        gf1_cli_top_op                    top_op = GF_CLI_TOP_NONE;
        uint64_t                          nr_open = 0;
        uint64_t                          max_nr_open = 0;
        double                            throughput = 0;
        double                            time = 0;
        long int                          time_sec = 0;
        long int                           time_usec = 0;
        struct tm                         *tm = NULL;
        char                              timestr[256] = {0, };

        if (-1 == req->rpc_status) {
                goto out;
        }

        gf_log ("cli", GF_LOG_DEBUG, "Received resp to top");
        ret = gf_xdr_to_cli_stats_volume_rsp (*iov, &rsp);
        if (ret < 0) {
                gf_log ("", GF_LOG_ERROR, "Unable to decode response");
                goto out;
        }

        if (rsp.op_ret && strcmp (rsp.op_errstr, "")) {
                cli_out (rsp.op_errstr);
        } else {
                cli_out ("volume top %s ",
                         (rsp.op_ret) ? "unsuccessful": "successful");
        }

        if (rsp.op_ret) {
                ret = rsp.op_ret;
                goto out;
        }

        dict = dict_new ();

        if (!dict) {
                ret = -1;
                goto out;
        }

        ret = dict_unserialize (rsp.stats_info.stats_info_val,
                                rsp.stats_info.stats_info_len,
                                &dict);

        if (ret) {
                gf_log ("", GF_LOG_ERROR,
                                "Unable to allocate memory");
                goto out;
        }

        ret = dict_get_int32 (dict, "op", (int32_t*)&op);

        if (op != GF_CLI_STATS_TOP) {
                ret = 0;
                goto out;
        }
        ret = dict_get_int32 (dict, "count", &brick_count);
        if (ret)
                goto out;
        snprintf (key, sizeof (key), "%d-top-op", 1);
        ret = dict_get_int32 (dict, key, (int32_t*)&top_op);
        if (ret)
                goto out;
        while (i <= brick_count) {
                i++;
                snprintf (brick, sizeof (brick), "%d-brick", i);
                ret = dict_get_str (dict, brick, &bricks);
                if (ret)
                        goto out;
                cli_out ("Brick: %s", bricks);
                snprintf(key, sizeof (key), "%d-members", i);
                ret = dict_get_int32 (dict, key, &members);

                switch (top_op) {
                case GF_CLI_TOP_OPEN:
                        snprintf (key, sizeof (key), "%d-current-open", i);
                        ret = dict_get_uint64 (dict, key, &nr_open);
                        if (ret)
                                break;
                        snprintf (key, sizeof (key), "%d-max-open", i);
                        ret = dict_get_uint64 (dict, key, &max_nr_open);
                        if (ret)
                                goto out;
                        cli_out ("Current open fds: %"PRIu64", Max open"
                                " fds: %"PRIu64, nr_open, max_nr_open);
                case GF_CLI_TOP_READ:
                case GF_CLI_TOP_WRITE:
                case GF_CLI_TOP_OPENDIR:
                case GF_CLI_TOP_READDIR:
                        if (!members) {
                                cli_out ("No entries in list");
                                continue;
                        }
                        cli_out ("Count\t\tfilename\n=======================");
                        break;
                case GF_CLI_TOP_READ_PERF:
                case GF_CLI_TOP_WRITE_PERF:
                        snprintf (key, sizeof (key), "%d-throughput", i);
                        ret = dict_get_double (dict, key, &throughput);
                        if (!ret) {
                                snprintf (key, sizeof (key), "%d-time", i);
                                ret = dict_get_double (dict, key, &time);
                        }
                        if (!ret)
                                cli_out ("Throughput %.2f MBps time %.4f secs", throughput,
                                          time / 1e6);

                        if (!members) {
                                cli_out ("No entries in list");
                                continue;
                        }
                        cli_out ("MBps\t\tfilename\t\t time\n========================");
                        break;
                default:
                        goto out;
                }

                for (j = 1; j <= members; j++) {
                        snprintf (key, sizeof (key), "%d-filename-%d", i, j);
                        ret = dict_get_str (dict, key, &filename);
                        if (ret)
                                break;
                        snprintf (key, sizeof (key), "%d-value-%d", i, j);
                        ret = dict_get_uint64 (dict, key, &value);
                        if (ret)
                                goto out;
                        if ( top_op == GF_CLI_TOP_READ_PERF ||
                                top_op == GF_CLI_TOP_WRITE_PERF) {
                                snprintf (key, sizeof (key), "%d-time-sec-%d", i, j);
                                ret = dict_get_int32 (dict, key, (int32_t *)&time_sec);
                                if (ret)
                                        goto out;
                                snprintf (key, sizeof (key), "%d-time-usec-%d", i, j);
                                ret = dict_get_int32 (dict, key, (int32_t *)&time_usec);
                                if (ret)
                                        goto out;
                                tm    = localtime (&time_sec);
                                if (!tm)
                                        goto out;
                                strftime (timestr, 256, "%Y-%m-%d %H:%M:%S", tm);
                                snprintf (timestr + strlen (timestr), 256 - strlen (timestr),
                                  ".%"GF_PRI_SUSECONDS, time_usec);

                                cli_out ("%"PRIu64"\t\t%s\t\t%s", value, filename, timestr);
                        } else {
                                cli_out ("%"PRIu64"\t\t%s", value, filename);
                        }
                }
        }
        ret = rsp.op_ret;

out:
        cli_cmd_broadcast_response (ret);

        if (dict)
                dict_unref (dict);

        if (rsp.stats_info.stats_info_val)
                free (rsp.stats_info.stats_info_val);
        return ret;
}

int32_t
gf_cli3_1_top_volume (call_frame_t *frame, xlator_t *this, void *data)
{
        int                        ret   = -1;
        gf1_cli_stats_volume_req   req   = {0,};
        dict_t                     *dict = NULL;

        GF_ASSERT (frame);
        GF_ASSERT (this);
        GF_ASSERT (data);

        if (!frame || !this || !data)
                goto out;
        dict = data;
        ret = dict_get_str (dict, "volname", &req.volname);
        if (ret)
                goto out;

        ret = dict_get_int32 (dict, "op", (int32_t*)&req.op);
        if (ret)
                goto out;

        ret = dict_allocate_and_serialize (dict,
                                           &req.dict_req.dict_req_val,
                                           (size_t *)&req.dict_req.dict_req_len);

        ret = cli_cmd_submit (&req, frame, cli_rpc_prog,
                              GLUSTER_CLI_PROFILE_VOLUME, NULL,
                              gf_xdr_from_cli_stats_volume_req,
                              this, gf_cli3_1_top_volume_cbk);

out:
        gf_log ("cli", GF_LOG_DEBUG, "Returning %d", ret);
        return ret;
}


int
gf_cli3_1_getwd_cbk (struct rpc_req *req, struct iovec *iov,
                       int count, void *myframe)
{
        gf1_cli_getwd_rsp rsp   = {0,};
        int               ret   = 0;

        if (-1 == req->rpc_status) {
                goto out;
        }

        ret = gf_xdr_to_cli_getwd_rsp (*iov, &rsp);
        if (ret < 0 || rsp.op_ret == -1) {
                gf_log ("", GF_LOG_ERROR, "error");
                goto out;
        }

        gf_log ("cli", GF_LOG_INFO, "Received resp to getwd");

        cli_out (rsp.wd);

        ret = 0;

out:
        cli_cmd_broadcast_response (ret);
        return ret;
}

int32_t
gf_cli3_1_getwd (call_frame_t *frame, xlator_t *this, void *data)
{
        int                      ret = -1;
        gf1_cli_getwd_req        req = {0,};

        GF_ASSERT (frame);
        GF_ASSERT (this);

        if (!frame || !this)
                goto out;

        ret = cli_cmd_submit (&req, frame, cli_rpc_prog,
                              GLUSTER_CLI_GETWD, NULL,
                              gf_xdr_from_cli_getwd_req,
                              this, gf_cli3_1_getwd_cbk);

out:
        gf_log ("cli", GF_LOG_DEBUG, "Returning %d", ret);

        return ret;
}


struct rpc_clnt_procedure gluster_cli_actors[GLUSTER_CLI_MAXVALUE] = {
        [GLUSTER_CLI_NULL]             = {"NULL", NULL },
        [GLUSTER_CLI_PROBE]            = {"PROBE_QUERY", gf_cli3_1_probe},
        [GLUSTER_CLI_DEPROBE]          = {"DEPROBE_QUERY", gf_cli3_1_deprobe},
        [GLUSTER_CLI_LIST_FRIENDS]     = {"LIST_FRIENDS", gf_cli3_1_list_friends},
        [GLUSTER_CLI_CREATE_VOLUME]    = {"CREATE_VOLUME", gf_cli3_1_create_volume},
        [GLUSTER_CLI_DELETE_VOLUME]    = {"DELETE_VOLUME", gf_cli3_1_delete_volume},
        [GLUSTER_CLI_START_VOLUME]     = {"START_VOLUME", gf_cli3_1_start_volume},
        [GLUSTER_CLI_STOP_VOLUME]      = {"STOP_VOLUME", gf_cli3_1_stop_volume},
        [GLUSTER_CLI_RENAME_VOLUME]    = {"RENAME_VOLUME", gf_cli3_1_rename_volume},
        [GLUSTER_CLI_DEFRAG_VOLUME]    = {"DEFRAG_VOLUME", gf_cli3_1_defrag_volume},
        [GLUSTER_CLI_GET_VOLUME]       = {"GET_VOLUME", gf_cli3_1_get_volume},
        [GLUSTER_CLI_GET_NEXT_VOLUME]  = {"GET_NEXT_VOLUME", gf_cli3_1_get_next_volume},
        [GLUSTER_CLI_SET_VOLUME]       = {"SET_VOLUME", gf_cli3_1_set_volume},
        [GLUSTER_CLI_ADD_BRICK]        = {"ADD_BRICK", gf_cli3_1_add_brick},
        [GLUSTER_CLI_REMOVE_BRICK]     = {"REMOVE_BRICK", gf_cli3_1_remove_brick},
        [GLUSTER_CLI_REPLACE_BRICK]    = {"REPLACE_BRICK", gf_cli3_1_replace_brick},
        [GLUSTER_CLI_LOG_FILENAME]     = {"LOG FILENAME", gf_cli3_1_log_filename},
        [GLUSTER_CLI_LOG_LOCATE]       = {"LOG LOCATE", gf_cli3_1_log_locate},
        [GLUSTER_CLI_LOG_ROTATE]       = {"LOG ROTATE", gf_cli3_1_log_rotate},
        [GLUSTER_CLI_GETSPEC]          = {"GETSPEC", gf_cli3_1_getspec},
        [GLUSTER_CLI_PMAP_PORTBYBRICK] = {"PMAP PORTBYBRICK", gf_cli3_1_pmap_b2p},
        [GLUSTER_CLI_SYNC_VOLUME]      = {"SYNC_VOLUME", gf_cli3_1_sync_volume},
        [GLUSTER_CLI_RESET_VOLUME]     = {"RESET_VOLUME", gf_cli3_1_reset_volume},
        [GLUSTER_CLI_FSM_LOG]          = {"FSM_LOG", gf_cli3_1_fsm_log},
        [GLUSTER_CLI_GSYNC_SET]        = {"GSYNC_SET", gf_cli3_1_gsync_set},
        [GLUSTER_CLI_PROFILE_VOLUME]   = {"PROFILE_VOLUME", gf_cli3_1_profile_volume},
        [GLUSTER_CLI_QUOTA]            = {"QUOTA", gf_cli3_1_quota},
        [GLUSTER_CLI_TOP_VOLUME]       = {"TOP_VOLUME", gf_cli3_1_top_volume},
        [GLUSTER_CLI_GETWD]            = {"GETWD", gf_cli3_1_getwd}
};

struct rpc_clnt_program cli_prog = {
        .progname  = "Gluster CLI",
        .prognum   = GLUSTER_CLI_PROGRAM,
        .progver   = GLUSTER_CLI_VERSION,
        .numproc   = GLUSTER_CLI_PROCCNT,
        .proctable = gluster_cli_actors,
};
