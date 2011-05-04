#include <signal.h>

#include "glusterd-geo-replication.h"
#include "uuid.h"

#include "fnmatch.h"
#include "xlator.h"
#include "protocol-common.h"
#include "glusterd.h"
#include "call-stub.h"
#include "defaults.h"
#include "list.h"
#include "dict.h"
#include "compat.h"
#include "compat-errno.h"
#include "statedump.h"
#include "glusterd-sm.h"
#include "glusterd-op-sm.h"
#include "glusterd-utils.h"
#include "glusterd-store.h"
#include "glusterd-volgen.h"
#include "glusterd-geo-replication.h"
#include "syscall.h"
#include "cli1.h"
#include "common-utils.h"


static char *gsync_reserved_opts[] = {
        "gluster-command",
        "pid-file",
        "state-file",
        "session-owner",
        NULL
};

static int
glusterd_get_canon_url (char *cann, char *name, gf_boolean_t cann_esc)
{
        char                cmd[PATH_MAX] = {0, };
        glusterd_conf_t    *priv  = NULL;

        GF_ASSERT (THIS);
        GF_ASSERT (THIS->private);

        priv = THIS->private;

        snprintf (cmd, PATH_MAX, GSYNCD_PREFIX"/gsyncd --canonicalize-%surl %s",
                  cann_esc? "escape-": "",name);

        return glusterd_query_extutil (cann, cmd);
}

static int
glusterd_gsync_get_param_file (char *prmfile, const char *param, char *master,
                               char *slave, char *gl_workdir)
{
        char                cmd[PATH_MAX] = {0, };
        glusterd_conf_t    *priv  = NULL;

        GF_ASSERT (THIS);
        GF_ASSERT (THIS->private);

        priv = THIS->private;

        snprintf (cmd, PATH_MAX,
                  GSYNCD_PREFIX"/gsyncd -c %s/"GSYNC_CONF" :%s %s --config-get "
                  "%s-file", gl_workdir, master, slave, param);

        return glusterd_query_extutil (prmfile, cmd);
}

static int
gsyncd_getpidfile (char *master, char *slave, char *pidfile)
{
        int                ret             = -1;
        glusterd_conf_t    *priv  = NULL;

        GF_ASSERT (THIS);
        GF_ASSERT (THIS->private);

        priv = THIS->private;

        GF_VALIDATE_OR_GOTO ("gsync", master, out);
        GF_VALIDATE_OR_GOTO ("gsync", slave, out);

        ret = glusterd_gsync_get_param_file (pidfile, "pid", master,
                                              slave, priv->workdir);
        if (ret == -1) {
                ret = -2;
                gf_log ("", GF_LOG_WARNING, "failed to create the pidfile "
                        "string");
                goto out;
        }

        ret = open (pidfile, O_RDWR);

 out:
        return ret;
}

static int
gsync_status_byfd (int fd)
{
        GF_ASSERT (fd >= -1);

        if (lockf (fd, F_TEST, 0) == -1 &&
            (errno == EAGAIN || errno == EACCES))
                /* gsyncd keeps the pidfile locked */
                return 0;

        return -1;
}

/* status: return 0 when gsync is running
 * return -1 when not running
 */
static int
gsync_status (char *master, char *slave, int *status)
{
        char pidfile[PATH_MAX] = {0,};
        int  fd                = -1;

        fd = gsyncd_getpidfile (master, slave, pidfile);
        if (fd == -2)
                return -1;

        *status = gsync_status_byfd (fd);

        close (fd);
        return 0;
}


static int32_t
glusterd_gsync_volinfo_dict_set (glusterd_volinfo_t *volinfo,
                                 char *key, char *value)
{
        int32_t  ret            = -1;
        char    *gsync_status   = NULL;

        gsync_status = gf_strdup (value);
        if (!gsync_status) {
                gf_log ("", GF_LOG_ERROR, "Unable to allocate memory");
                goto out;
        }

        ret = dict_set_dynstr (volinfo->dict, key, gsync_status);
        if (ret) {
                gf_log ("", GF_LOG_ERROR, "Unable to set dict");
                goto out;
        }

        ret = 0;
out:
        return 0;
}

static int
gsync_verify_config_options (dict_t *dict, char **op_errstr)
{
        char    cmd[PATH_MAX] = {0,};
        char  **resopt    = NULL;
        int     i         = 0;
        char   *subop     = NULL;
        char   *slave     = NULL;
        char   *op_name   = NULL;
        char   *op_value  = NULL;
        gf_boolean_t banned = _gf_true;

        if (dict_get_str (dict, "subop", &subop) != 0) {
                gf_log ("", GF_LOG_WARNING, "missing subop");
                *op_errstr = gf_strdup ("Invalid config request");
                return -1;
        }

        if (dict_get_str (dict, "slave", &slave) != 0) {
                gf_log ("", GF_LOG_WARNING, GEOREP" CONFIG: no slave given");
                *op_errstr = gf_strdup ("Slave required");
                return -1;
        }

        if (strcmp (subop, "get-all") == 0)
                return 0;

        if (dict_get_str (dict, "op_name", &op_name) != 0) {
                gf_log ("", GF_LOG_WARNING, "option name missing");
                *op_errstr = gf_strdup ("Option name missing");
                return -1;
        }

        snprintf (cmd, PATH_MAX, GSYNCD_PREFIX"/gsyncd --config-check %s",
                  op_name);
        if (system (cmd)) {
                gf_log ("", GF_LOG_WARNING, "Invalid option %s", op_name);
                *op_errstr = gf_strdup ("Invalid option");

                return -1;
        }

        if (strcmp (subop, "get") == 0)
                return 0;

        if (strcmp (subop, "set") != 0 && strcmp (subop, "del") != 0) {
                gf_log ("", GF_LOG_WARNING, "unknown subop %s", subop);
                *op_errstr = gf_strdup ("Invalid config request");
                return -1;
        }

        if (strcmp (subop, "set") == 0 &&
            dict_get_str (dict, "op_value", &op_value) != 0) {
                gf_log ("", GF_LOG_WARNING, "missing value for set");
                *op_errstr = gf_strdup ("missing value");
        }

        /* match option name against reserved options, modulo -/_
         * difference
         */
        for (resopt = gsync_reserved_opts; *resopt; resopt++) {
                banned = _gf_true;
                for (i = 0; (*resopt)[i] && op_name[i]; i++) {
                        if ((*resopt)[i] == op_name[i] ||
                            ((*resopt)[i] == '-' && op_name[i] == '_'))
                                continue;
                        banned = _gf_false;
                }
                if (banned) {
                        gf_log ("", GF_LOG_WARNING, "Reserved option %s",
                                op_name);
                        *op_errstr = gf_strdup ("Reserved option");

                        return -1;
                        break;
                }
        }

        return 0;
}

/* The return   status indicates success (ret_status = 0) if the host uuid
 *  matches,    status indicates failure (ret_status = -1) if the host uuid
 *  mismatches, status indicates not found if the slave is not found to be
 *  spawned for the given master */
static void
_compare_host_uuid (dict_t *this, char *key, data_t *value, void *data)
{
        glusterd_gsync_slaves_t     *status = NULL;
        char                        *slave = NULL;
        int                          uuid_len = 0;

        status = (glusterd_gsync_slaves_t *)data;

        if ((status->ret_status == -1) || (status->ret_status == 0))
                return;
        slave = strchr(value->data, ':');
        if (slave)
                slave ++;

        uuid_len = (slave - value->data - 1);

        if (strncmp (slave, status->slave, PATH_MAX) == 0) {
                if (strncmp (value->data, status->host_uuid, uuid_len) == 0) {
                        status->ret_status = 0;
                } else {
                        status->ret_status = -1;
                        strncpy (status->rmt_hostname, value->data, uuid_len);
                        status->rmt_hostname[uuid_len] = '\0';
                }
        }

}

static void
_get_max_gsync_slave_num (dict_t *this, char *key, data_t *value, void *data)
{
        int                          tmp_slvnum = 0;
        glusterd_gsync_slaves_t     *status = NULL;

        status = (glusterd_gsync_slaves_t *)data;

        sscanf (key, "slave%d", &tmp_slvnum);
        if (tmp_slvnum > status->ret_status)
                status->ret_status = tmp_slvnum;
}

static void
_remove_gsync_slave (dict_t *this, char *key, data_t *value, void *data)
{
        glusterd_gsync_slaves_t     *status = NULL;
        char                        *slave = NULL;


        status = (glusterd_gsync_slaves_t *)data;

        slave = strchr(value->data, ':');
        if (slave)
                slave ++;

        if (strncmp (slave, status->slave, PATH_MAX) == 0)
                dict_del (this, key);

}

static int
glusterd_remove_slave_in_info (glusterd_volinfo_t *volinfo, char *slave,
                               char *host_uuid, char **op_errstr)
{
        int                         ret = 0;
        glusterd_gsync_slaves_t     status = {0, };
        char                        cann_slave[PATH_MAX] = {0,  };

        GF_ASSERT (volinfo);
        GF_ASSERT (slave);
        GF_ASSERT (host_uuid);

        ret = glusterd_get_canon_url (cann_slave, slave, _gf_false);
        if (ret)
                goto out;

        status.slave = cann_slave;
        status.host_uuid = host_uuid;
        status.ret_status = 1;

        dict_foreach (volinfo->gsync_slaves, _remove_gsync_slave, &status);

        ret = glusterd_store_volinfo (volinfo,
                                      GLUSTERD_VOLINFO_VER_AC_INCREMENT);
        if (ret) {
                 *op_errstr = gf_strdup ("Failed to store the Volume"
                                         "information");
                goto out;
        }
 out:
        gf_log ("", GF_LOG_DEBUG, "returning %d", ret);
        return ret;

}

static int
glusterd_gsync_get_uuid (char *slave, glusterd_volinfo_t *vol,
                         uuid_t uuid)
{

        int                         ret = 0;
        glusterd_gsync_slaves_t     status = {0, };
        char                        cann_slave[PATH_MAX] = {0,  };
        char                        host_uuid_str[64] = {0};
        xlator_t                    *this = NULL;
        glusterd_conf_t             *priv = NULL;


        this = THIS;
        GF_ASSERT (this);
        priv = this->private;
        GF_ASSERT (priv);
        GF_ASSERT (vol);
        GF_ASSERT (slave);

        uuid_utoa_r (priv->uuid, host_uuid_str);
        ret = glusterd_get_canon_url (cann_slave, slave, _gf_false);
        if (ret)
                goto out;

        status.slave = cann_slave;
        status.host_uuid = host_uuid_str;
        status.ret_status = 1;
        dict_foreach (vol->gsync_slaves, _compare_host_uuid, &status);
        if (status.ret_status == 0) {
                uuid_copy (uuid, priv->uuid);
        } else if (status.ret_status == -1) {
                uuid_parse (status.rmt_hostname, uuid);
        } else {
                ret = -1;
                goto out;
        }
        ret = 0;
 out:
        gf_log ("", GF_LOG_DEBUG, "Returning %d", ret);
        return ret;

}

static int
glusterd_check_gsync_running_local (char *master, char *slave,
                                    gf_boolean_t *is_run)
{
        int                 ret    = -1;
        int                 ret_status = 0;

        GF_ASSERT (master);
        GF_ASSERT (slave);
        GF_ASSERT (is_run);

        *is_run = _gf_false;
        ret = gsync_status (master, slave, &ret_status);
        if (ret == 0 && ret_status == 0) {
                *is_run = _gf_true;
        } else if (ret == -1) {
                gf_log ("", GF_LOG_WARNING, GEOREP" validation "
                        " failed");
                goto out;
        }
        ret = 0;
 out:
        gf_log ("", GF_LOG_DEBUG, "Returning %d", ret);
        return ret;

}

static int32_t
glusterd_marker_create_volfile (glusterd_volinfo_t *volinfo)
{
        int32_t          ret     = 0;

        ret = glusterd_create_volfiles_and_notify_services (volinfo);
        if (ret) {
                gf_log ("", GF_LOG_ERROR, "Unable to create volfile"
                        " for setting of marker while '"GEOREP" start'");
                ret = -1;
                goto out;
        }

        ret = glusterd_store_volinfo (volinfo,
                                      GLUSTERD_VOLINFO_VER_AC_INCREMENT);
        if (ret)
                goto out;

        if (GLUSTERD_STATUS_STARTED == volinfo->status)
                ret = glusterd_check_generate_start_nfs ();
        ret = 0;
out:
        return ret;
}


static int
glusterd_set_marker_gsync (glusterd_volinfo_t *volinfo)
{
        int                      ret     = -1;
        int                      marker_set = _gf_false;
        char                    *gsync_status = NULL;
        glusterd_conf_t         *priv = NULL;

        GF_ASSERT (THIS);
        GF_ASSERT (THIS->private);

        priv = THIS->private;

        marker_set = glusterd_volinfo_get_boolean (volinfo, VKEY_MARKER_XTIME);
        if (marker_set == -1) {
                gf_log ("", GF_LOG_ERROR, "failed to get the marker status");
                ret = -1;
                goto out;
        }

        if (marker_set == _gf_false) {
                gsync_status = gf_strdup ("on");
                if (gsync_status == NULL) {
                        ret = -1;
                        goto out;
                }

                ret = glusterd_gsync_volinfo_dict_set (volinfo,
                                                       VKEY_MARKER_XTIME,
                                                       gsync_status);
                if (ret < 0)
                        goto out;

                ret = glusterd_marker_create_volfile (volinfo);
                if (ret) {
                        gf_log ("", GF_LOG_ERROR, "Setting dict failed");
                        goto out;
                }
        }
        ret = 0;

out:
        gf_log ("", GF_LOG_DEBUG, "Returning %d", ret);
        return ret;
}


static int
glusterd_store_slave_in_info (glusterd_volinfo_t *volinfo, char *slave,
                              char *host_uuid, char **op_errstr)
{
        int                         ret = 0;
        glusterd_gsync_slaves_t     status = {0, };
        char                        cann_slave[PATH_MAX] = {0,  };
        char                       *value = NULL;
        char                        key[512] = {0, };

        GF_ASSERT (volinfo);
        GF_ASSERT (slave);
        GF_ASSERT (host_uuid);

        ret = glusterd_get_canon_url (cann_slave, slave, _gf_false);
        if (ret)
                goto out;

        status.slave = cann_slave;
        status.host_uuid = host_uuid;
        status.ret_status = 1;
        dict_foreach (volinfo->gsync_slaves, _compare_host_uuid, &status);

        if (status.ret_status == -1) {
                gf_log ("", GF_LOG_ERROR, GEOREP" has already been invoked for "
                                          "the %s (master) and %s (slave)"
                                          "from a different machine",
                                           volinfo->volname, slave);
                 *op_errstr = gf_strdup (GEOREP" already running in an an"
                                        "orhter machine");
                ret = -1;
                goto out;
        }

        memset (&status, 0, sizeof (status));

        dict_foreach (volinfo->gsync_slaves, _get_max_gsync_slave_num, &status);

        gf_asprintf (&value,  "%s:%s", host_uuid, cann_slave);
        snprintf (key, 512, "slave%d", status.ret_status +1);
        ret = dict_set_dynstr (volinfo->gsync_slaves, key, value);

        if (ret)
                goto out;
        ret = glusterd_store_volinfo (volinfo,
                                      GLUSTERD_VOLINFO_VER_AC_INCREMENT);
        if (ret) {
                 *op_errstr = gf_strdup ("Failed to store the Volume"
                                         "information");
                goto out;
        }
        ret = 0;
 out:
        return ret;
}


static int
glusterd_op_verify_gsync_start_options (glusterd_volinfo_t *volinfo,
                                        char *slave, char **op_errstr)
{
        int                     ret = -1;
        gf_boolean_t            is_running = _gf_false;
        char                    msg[2048] = {0};
        uuid_t                  uuid = {0};
        glusterd_conf_t         *priv = NULL;
        xlator_t                *this = NULL;

        this = THIS;

        GF_ASSERT (volinfo);
        GF_ASSERT (slave);
        GF_ASSERT (op_errstr);
        GF_ASSERT (this && this->private);

        priv  = this->private;

        if (GLUSTERD_STATUS_STARTED != volinfo->status) {
                snprintf (msg, sizeof (msg), "Volume %s needs to be started "
                          "before "GEOREP" start", volinfo->volname);
                goto out;
        }

        ret = glusterd_gsync_get_uuid (slave, volinfo, uuid);
        if ((ret == 0) && (uuid_compare (priv->uuid, uuid) == 0)) {
                ret = glusterd_check_gsync_running_local (volinfo->volname,
                                                          slave, &is_running);
                if (ret) {
                        snprintf (msg, sizeof (msg), GEOREP" start option "
                                  "validation failed ");
                        goto out;
                }
                if (_gf_true == is_running) {
                        snprintf (msg, sizeof (msg), GEOREP " session between"
                                  " %s & %s already started", volinfo->volname,
                                  slave);
                        ret = -1;
                        goto out;
                }
        }
        ret = 0;
out:
        if (ret && (msg[0] != '\0')) {
                *op_errstr = gf_strdup (msg);
        }
        gf_log ("", GF_LOG_DEBUG, "Returning %d", ret);
        return ret;
}

int
glusterd_check_gsync_running (glusterd_volinfo_t *volinfo, gf_boolean_t *flag)
{

        GF_ASSERT (volinfo);
        GF_ASSERT (flag);

        if (volinfo->gsync_slaves->count)
                *flag = _gf_true;
        else
                *flag = _gf_false;

        return 0;
}

static int
glusterd_op_verify_gsync_running (glusterd_volinfo_t *volinfo,
                                  char *slave, char **op_errstr)
{
        int                     ret = -1;
        char                    msg[2048] = {0};
        uuid_t                  uuid = {0};
        glusterd_conf_t         *priv = NULL;

        GF_ASSERT (THIS && THIS->private);
        GF_ASSERT (volinfo);
        GF_ASSERT (slave);
        GF_ASSERT (op_errstr);

        priv = THIS->private;

        if (GLUSTERD_STATUS_STARTED != volinfo->status) {
                snprintf (msg, sizeof (msg), "Volume %s needs to be started "
                          "before "GEOREP" start", volinfo->volname);

                goto out;
        }
        ret = glusterd_gsync_get_uuid (slave, volinfo, uuid);
        if (ret == -1) {
                snprintf (msg, sizeof (msg), GEOREP" session between %s & %s"
                          " not active", volinfo->volname, slave);
                goto out;
        }

        ret = 0;
out:
        if (ret && (msg[0] != '\0')) {
                *op_errstr = gf_strdup (msg);
        }
        gf_log ("", GF_LOG_DEBUG, "Returning %d", ret);
        return ret;
}

static int
glusterd_verify_gsync_status_opts (dict_t *dict, char **op_errstr)
{
        char               *slave  = NULL;
        char               *volname = NULL;
        char               errmsg[PATH_MAX] = {0, };
        gf_boolean_t       exists = _gf_false;
        glusterd_volinfo_t *volinfo = NULL;
        int                ret = 0;

        ret = dict_get_str (dict, "master", &volname);
        if (ret < 0) {
                ret = 0;
                goto out;
        }

        exists = glusterd_check_volume_exists (volname);
        ret = glusterd_volinfo_find (volname, &volinfo);
        if ((ret) || (!exists)) {
                gf_log ("", GF_LOG_WARNING, "volume name does not exist");
                snprintf (errmsg, sizeof(errmsg), "Volume name %s does not"
                          " exist", volname);
                *op_errstr = gf_strdup (errmsg);
                ret = -1;
                goto out;
        }

        ret = dict_get_str (dict, "slave", &slave);
        if (ret < 0) {
                ret = 0;
                goto out;
        }

 out:
        gf_log ("", GF_LOG_DEBUG, "Returning %d", ret);
        return ret;

}


static int
glusterd_op_gsync_args_get (dict_t *dict, char **op_errstr,
                            char **master, char **slave)
{

        int             ret = -1;
        GF_ASSERT (dict);
        GF_ASSERT (op_errstr);
        GF_ASSERT (master);
        GF_ASSERT (slave);

        ret = dict_get_str (dict, "master", master);
        if (ret < 0) {
                gf_log ("", GF_LOG_WARNING, "master not found");
                *op_errstr = gf_strdup ("master not found");
                goto out;
        }

        ret = dict_get_str (dict, "slave", slave);
        if (ret < 0) {
                gf_log ("", GF_LOG_WARNING, "slave not found");
                *op_errstr = gf_strdup ("slave not found");
                goto out;
        }


        ret = 0;
out:
        gf_log ("", GF_LOG_DEBUG, "Returning %d", ret);
        return ret;
}

int
glusterd_op_stage_gsync_set (dict_t *dict, char **op_errstr)
{
        int                     ret     = 0;
        int                     type    = 0;
        char                    *volname = NULL;
        char                    *slave   = NULL;
        gf_boolean_t            exists   = _gf_false;
        glusterd_volinfo_t      *volinfo = NULL;
        char                    errmsg[PATH_MAX] = {0,};


        ret = dict_get_int32 (dict, "type", &type);
        if (ret < 0) {
                gf_log ("", GF_LOG_WARNING, "command type not found");
                *op_errstr = gf_strdup ("command unsuccessful");
                goto out;
        }

        switch (type) {
        case GF_GSYNC_OPTION_TYPE_STATUS:
                ret = glusterd_verify_gsync_status_opts (dict, op_errstr);

                goto out;
        case GF_GSYNC_OPTION_TYPE_CONFIG:
                ret = gsync_verify_config_options (dict, op_errstr);

                goto out;
        }

        ret = glusterd_op_gsync_args_get (dict, op_errstr, &volname, &slave);
        if (ret)
                goto out;

        exists = glusterd_check_volume_exists (volname);
        ret = glusterd_volinfo_find (volname, &volinfo);
        if ((ret) || (!exists)) {
                gf_log ("", GF_LOG_WARNING, "volume name does not exist");
                snprintf (errmsg, sizeof(errmsg), "Volume name %s does not"
                          " exist", volname);
                *op_errstr = gf_strdup (errmsg);
                ret = -1;
                goto out;
        }

        switch (type) {
        case GF_GSYNC_OPTION_TYPE_START:
                ret = glusterd_op_verify_gsync_start_options (volinfo, slave,
                                                              op_errstr);
                break;
        case GF_GSYNC_OPTION_TYPE_STOP:
                ret = glusterd_op_verify_gsync_running (volinfo, slave,
                                                        op_errstr);
                break;
        }

out:
        gf_log ("glusterd", GF_LOG_DEBUG, "Returning %d", ret);
        return ret;
}

static int
glusterd_gsync_read_frm_status (char *path, char *data)
{
        int                 ret = 0;
        FILE               *status_file = NULL;

        GF_ASSERT (path);
        GF_ASSERT (data);
        status_file = fopen (path, "r");
        if (status_file  == NULL) {
                gf_log ("", GF_LOG_WARNING, "Unable to read gsyncd status"
                        " file");
                return -1;
        }
        ret = fread (data, PATH_MAX, 1, status_file);
        if (ret < 0) {
                gf_log ("", GF_LOG_WARNING, "Status file of gsyncd is corrupt");
                return -1;
        }

        data[strlen(data)-1] = '\0';

        return 0;
}


static int
glusterd_read_status_file (char *master, char *slave,
                           dict_t *dict)
{
        glusterd_conf_t  *priv = NULL;
        int              ret = 0;
        char             statusfile[PATH_MAX] = {0, };
        char             buff[PATH_MAX] = {0, };
        char             mst[PATH_MAX] = {0, };
        char             slv[PATH_MAX] = {0, };
        char             sts[PATH_MAX] = {0, };
        int              gsync_count = 0;
        int              status = 0;

        GF_ASSERT (THIS);
        GF_ASSERT (THIS->private);

        priv = THIS->private;
        ret = glusterd_gsync_get_param_file (statusfile, "state", master,
                                             slave, priv->workdir);
        if (ret) {
                gf_log ("", GF_LOG_WARNING, "Unable to get the name of status"
                        "file for %s(master), %s(slave)", master, slave);
                goto out;

        }

        ret = gsync_status (master, slave, &status);
        if (ret == 0 && status == -1) {
                strncpy (buff, "corrupt", sizeof (buff));
                goto done;
        } else if (ret == -1)
                goto out;

        ret = glusterd_gsync_read_frm_status (statusfile, buff);
        if (ret) {
                gf_log ("", GF_LOG_WARNING, "Unable to read the status"
                        "file for %s(master), %s(slave)", master, slave);
                goto out;

        }

 done:
        ret = dict_get_int32 (dict, "gsync-count", &gsync_count);

        if (ret)
                gsync_count = 1;
        else
                gsync_count++;

        snprintf (mst, sizeof (mst), "master%d", gsync_count);
        ret = dict_set_dynstr (dict, mst, gf_strdup (master));
        if (ret)
                goto out;

        snprintf (slv, sizeof (slv), "slave%d", gsync_count);
        ret = dict_set_dynstr (dict, slv, gf_strdup (slave));
        if (ret)
                goto out;

        snprintf (sts, sizeof (slv), "status%d", gsync_count);
        ret = dict_set_dynstr (dict, sts, gf_strdup (buff));
        if (ret)
                goto out;
        ret = dict_set_int32 (dict, "gsync-count", gsync_count);
        if (ret)
                goto out;

        ret = 0;
 out:
        gf_log ("", GF_LOG_DEBUG, "Returning %d ", ret);
        return ret;
}


static int
glusterd_get_gsync_status_mst_slv( glusterd_volinfo_t *volinfo,
                                   char *slave, dict_t *rsp_dict)
{
        uuid_t             uuid = {0, };
        glusterd_conf_t    *priv = NULL;
        int                ret = 0;

        GF_ASSERT (volinfo);
        GF_ASSERT (slave);
        GF_ASSERT (THIS);
        GF_ASSERT (THIS->private);

        priv = THIS->private;

        ret = glusterd_gsync_get_uuid (slave, volinfo, uuid);
        if ((ret == 0) && (uuid_compare (priv->uuid, uuid) != 0))
                goto out;

        if (ret) {
                ret = 0;
                gf_log ("", GF_LOG_INFO, "geo-replication status %s %s :"
                        "session is not active", volinfo->volname, slave);
                goto out;
        }

        ret = glusterd_read_status_file (volinfo->volname, slave, rsp_dict);
 out:
        gf_log ("", GF_LOG_DEBUG, "Returning with %d", ret);
        return ret;
}


static void
_get_status_mst_slv (dict_t *this, char *key, data_t *value, void *data)
{
        glusterd_gsync_status_temp_t  *param = NULL;
        char                          *slave = NULL;
        int                           ret = 0;

        param = (glusterd_gsync_status_temp_t *)data;

        GF_ASSERT (param);
        GF_ASSERT (param->volinfo);

        slave = strchr(value->data, ':');
        if (slave)
                slave ++;
        else
                return;

        ret = glusterd_get_gsync_status_mst_slv(param->volinfo,
                                                slave, param->rsp_dict);

}


static int
glusterd_get_gsync_status_mst (glusterd_volinfo_t *volinfo, dict_t *rsp_dict)
{
        glusterd_gsync_status_temp_t  param = {0, };

        GF_ASSERT (volinfo);

        param.rsp_dict = rsp_dict;
        param.volinfo = volinfo;
        dict_foreach (volinfo->gsync_slaves, _get_status_mst_slv, &param);

        return 0;
}

static int
glusterd_get_gsync_status_all ( dict_t *rsp_dict)
{

        int32_t                 ret = 0;
        glusterd_conf_t         *priv = NULL;
        glusterd_volinfo_t      *volinfo = NULL;

        GF_ASSERT (THIS);
        priv = THIS->private;

        GF_ASSERT (priv);

        list_for_each_entry (volinfo, &priv->volumes, vol_list) {
                ret = glusterd_get_gsync_status_mst (volinfo, rsp_dict);
                if (ret)
                        goto out;
        }

out:
        gf_log ("", GF_LOG_DEBUG, "Returning with %d", ret);
        return ret;

}

static int
glusterd_get_gsync_status (dict_t *dict, char **op_errstr, dict_t *rsp_dict)
{
        char               *slave  = NULL;
        char               *volname = NULL;
        char               errmsg[PATH_MAX] = {0, };
        gf_boolean_t       exists = _gf_false;
        glusterd_volinfo_t *volinfo = NULL;
        int                ret = 0;


        ret = dict_get_str (dict, "master", &volname);
        if (ret < 0){
                ret = glusterd_get_gsync_status_all (rsp_dict);
                goto out;
        }

        exists = glusterd_check_volume_exists (volname);
        ret = glusterd_volinfo_find (volname, &volinfo);
        if ((ret) || (!exists)) {
                gf_log ("", GF_LOG_WARNING, "volume name does not exist");
                snprintf (errmsg, sizeof(errmsg), "Volume name %s does not"
                          " exist", volname);
                *op_errstr = gf_strdup (errmsg);
                ret = -1;
                goto out;
        }


        ret = dict_get_str (dict, "slave", &slave);
        if (ret < 0) {
                ret = glusterd_get_gsync_status_mst (volinfo, rsp_dict);
                goto out;
        }

        ret = glusterd_get_gsync_status_mst_slv (volinfo, slave, rsp_dict);

 out:
        gf_log ("", GF_LOG_DEBUG, "Returning %d", ret);
        return ret;


}

int
glusterd_start_gsync (glusterd_volinfo_t *master_vol, char *slave,
                      char *glusterd_uuid_str, char **op_errstr)
{
        int32_t         ret     = 0;
        int32_t         status  = 0;
        char            buf[PATH_MAX]   = {0,};
        char            uuid_str [64] = {0};
        xlator_t        *this = NULL;
        glusterd_conf_t *priv = NULL;
        int             errcode = 0;

        this = THIS;
        GF_ASSERT (this);
        priv = this->private;
        GF_ASSERT (priv);

        uuid_utoa_r (priv->uuid, uuid_str);
        if (strcmp (uuid_str, glusterd_uuid_str))
                goto out;

        ret = gsync_status (master_vol->volname, slave, &status);
        if (status == 0)
                goto out;

        snprintf (buf, PATH_MAX, "%s/"GEOREP"/%s", priv->workdir,
                  master_vol->volname);
        ret = mkdir_if_missing (buf);
        if (ret) {
                errcode = -1;
                goto out;
        }

        snprintf (buf, PATH_MAX, DEFAULT_LOG_FILE_DIRECTORY"/"GEOREP"/%s",
                  master_vol->volname);
        ret = mkdir_if_missing (buf);
        if (ret) {
                errcode = -1;
                goto out;
        }

        uuid_utoa_r (master_vol->volume_id, uuid_str);
        ret = snprintf (buf, PATH_MAX,
                        GSYNCD_PREFIX"/gsyncd -c %s/"GSYNC_CONF" "
                        ":%s %s --config-set session-owner %s",
                        priv->workdir, master_vol->volname, slave, uuid_str);
        if (ret <= 0 || ret >= PATH_MAX)
                ret = -1;
        if (ret != -1)
                ret = gf_system (buf) ? -1 : 0;
        if (ret == -1) {
                errcode = -1;
                goto out;
        }

        ret = snprintf (buf, PATH_MAX, GSYNCD_PREFIX "/gsyncd --monitor -c "
                        "%s/"GSYNC_CONF" :%s %s", priv->workdir,
                        master_vol->volname, slave);
        if (ret <= 0) {
                ret = -1;
                errcode = -1;
                goto out;
        }

        ret = gf_system (buf);
        if (ret == -1) {
                gf_asprintf (op_errstr, GEOREP" start failed for %s %s",
                             master_vol->volname, slave);
                goto out;
        }

        ret = 0;

out:
        if ((ret != 0) && errcode == -1) {
                if (op_errstr)
                        *op_errstr = gf_strdup ("internal error, cannot start"
                                                "the " GEOREP " session");
        }

        gf_log ("", GF_LOG_DEBUG, "Returning %d", ret);
        return ret;
}


static int
glusterd_stop_gsync (char *master, char *slave, char **msg)
{
        int32_t         ret     = 0;
        int             pfd     = -1;
        pid_t           pid     = 0;
        char            pidfile[PATH_MAX] = {0,};
        char            buf [1024] = {0,};
        int             i       = 0;
        glusterd_conf_t *priv = NULL;

        GF_ASSERT (THIS);
        GF_ASSERT (THIS->private);

        priv = THIS->private;

        pfd = gsyncd_getpidfile (master, slave, pidfile);
        if (pfd == -2) {
                gf_log ("", GF_LOG_ERROR, GEOREP" stop validation "
                        " failed for %s & %s", master, slave);
                if (msg)
                        *msg = "Warning: "GEOREP" session was in corrupt "
                                "state";
                ret = -1;
                goto out;
        }
        if (gsync_status_byfd (pfd) == -1) {
                gf_log ("", GF_LOG_ERROR, "gsyncd b/w %s & %s is not"
                        " running", master, slave);
                if (msg)
                        *msg = "Warning: "GEOREP" session was in corrupt "
                                "state";
                /*monitor gsyncd already dead*/
                ret = -1;
                goto out;
        }

        ret = read (pfd, buf, 1024);
        if (ret > 0) {
                pid = strtol (buf, NULL, 10);
                ret = kill (-pid, SIGTERM);
                if (ret) {
                        gf_log ("", GF_LOG_WARNING,
                                "failed to kill gsyncd");
                        goto out;
                }
                for (i = 0; i < 20; i++) {
                        if (gsync_status_byfd (pfd) == -1) {
                                /* monitor gsyncd is dead but worker may
                                 * still be alive, give some more time
                                 * before SIGKILL (hack)
                                 */
                                usleep (50000);
                                break;
                        }
                        usleep (50000);
                }
                kill (-pid, SIGKILL);
                unlink (pidfile);
        }
        ret = 0;

out:
        close (pfd);
        return ret;
}

static int
glusterd_check_restart_gsync_session (glusterd_volinfo_t *volinfo, char *slave,
                                      dict_t *resp_dict)
{

        int                    ret = 0;
        uuid_t                 uuid = {0, };
        glusterd_conf_t        *priv = NULL;
        char                   *status_msg = NULL;

        GF_ASSERT (volinfo);
        GF_ASSERT (slave);
        GF_ASSERT (THIS);
        GF_ASSERT (THIS->private);

        priv = THIS->private;

        if (glusterd_gsync_get_uuid (slave, volinfo, uuid))
                /* session does not exist, nothing to do */
                goto out;
        if (uuid_compare (priv->uuid, uuid) == 0) {

                ret = glusterd_stop_gsync (volinfo->volname, slave,
                                           &status_msg);
                if (ret) {
                        gf_log ("", GF_LOG_ERROR, GEOREP " session not running,"
                                "for %s & %s", volinfo->volname, slave);
                        ret = dict_set_str (resp_dict, "gsync-status",
                                            status_msg);
                        if (ret) {
                                gf_log ("glusterd", GF_LOG_INFO, "dict_set "
                                        "failed while stopping gsync session");
                                ret = 0;
                        }
                        goto out;

                }

                ret = glusterd_start_gsync (volinfo, slave,
                                            uuid_utoa(priv->uuid), NULL);
                if (ret)
                        goto out;
        }

        ret = 0;

 out:
        gf_log ("", GF_LOG_DEBUG, "Returning %d", ret);
        return ret;
}


static int
glusterd_gsync_configure (glusterd_volinfo_t *volinfo, char *slave,
                          dict_t *dict, dict_t *resp_dict, char **op_errstr)
{
        int32_t         ret     = -1;
        char            *op_name = NULL;
        char            *op_value = NULL;
        char            cmd[1024] = {0,};
        glusterd_conf_t *priv   = NULL;
        char            *subop  = NULL;
        char            *q1 = NULL;
        char            *q2 = NULL;
        char            *cm = NULL;
        char            *master = NULL;

        GF_ASSERT (slave);
        GF_ASSERT (op_errstr);
        GF_ASSERT (dict);
        GF_ASSERT (resp_dict);

        ret = dict_get_str (dict, "subop", &subop);
        if (ret != 0)
                goto out;

        if (strcmp (subop, "get") == 0 || strcmp (subop, "get-all") == 0) {
                /* deferred to cli */
                gf_log ("", GF_LOG_DEBUG, "Returning 0");
                return 0;
        }

        ret = dict_get_str (dict, "op_name", &op_name);
        if (ret != 0)
                goto out;

        if (strcmp (subop, "set") == 0) {
                ret = dict_get_str (dict, "op_value", &op_value);
                if (ret != 0)
                        goto out;
                q1 = " \"";
                q2 = "\"";
        } else {
                q1 = "";
                op_value = "";
                q2 = "";
        }

        if (THIS)
                priv = THIS->private;
        if (priv == NULL) {
                gf_log ("", GF_LOG_ERROR, "priv of glusterd not present");
                *op_errstr = gf_strdup ("glusterd defunct");
                goto out;
        }

        if (volinfo) {
                cm = ":";
                master = volinfo->volname;
        } else {
                cm = "";
                master = "";
        }

        ret = snprintf (cmd, 1024, GSYNCD_PREFIX"/gsyncd -c %s/"GSYNC_CONF
                        " %s%s %s --config-%s %s" "%s%s%s", priv->workdir,
                        cm, master, slave, subop, op_name,
                        q1, op_value, q2);
        ret = system (cmd);
        if (ret) {
                gf_log ("", GF_LOG_WARNING, "gsyncd failed to "
                        "%s %s option for %s %s peers",
                        subop, op_name, master, slave);

                gf_asprintf (op_errstr, GEOREP" config-%s failed for %s %s",
                             subop, master, slave);

                goto out;
        }
        ret = 0;
        gf_asprintf (op_errstr, "config-%s successful", subop);

out:
        if (!ret && volinfo) {
                ret = glusterd_check_restart_gsync_session (volinfo, slave,
                                                            resp_dict);
                if (ret)
                        *op_errstr = gf_strdup ("internal error");
        }

        gf_log ("", GF_LOG_DEBUG, "Returning %d", ret);
        return ret;
}

int
glusterd_op_gsync_set (dict_t *dict, char **op_errstr, dict_t *rsp_dict)
{
        int32_t             ret     = -1;
        int32_t             type    = -1;
        dict_t             *ctx    = NULL;
        dict_t             *resp_dict = NULL;
        char               *host_uuid = NULL;
        char               *slave  = NULL;
        char               *volname = NULL;
        glusterd_volinfo_t *volinfo = NULL;
        glusterd_conf_t    *priv = NULL;
        char               *status_msg = NULL;
        uuid_t              uuid = {0, };

        GF_ASSERT (THIS);
        GF_ASSERT (THIS->private);
        GF_ASSERT (dict);
        GF_ASSERT (op_errstr);

        priv = THIS->private;

        ret = dict_get_int32 (dict, "type", &type);
        if (ret < 0)
                goto out;

        ret = dict_get_str (dict, "host-uuid", &host_uuid);
        if (ret < 0)
                goto out;

        ctx = glusterd_op_get_ctx (GD_OP_GSYNC_SET);
        resp_dict = ctx ? ctx : rsp_dict;
        GF_ASSERT (resp_dict);

        if (type == GF_GSYNC_OPTION_TYPE_STATUS) {
                ret = glusterd_get_gsync_status (dict, op_errstr, resp_dict);
                goto out;
        }

        ret = dict_get_str (dict, "slave", &slave);
        if (ret < 0)
                goto out;

        if (dict_get_str (dict, "master", &volname) == 0) {
                ret = glusterd_volinfo_find (volname, &volinfo);
                if (ret) {
                        gf_log ("", GF_LOG_WARNING, "Volinfo for %s (master)"
                                " not found", volname);
                        goto out;
                }
        }

        if (type == GF_GSYNC_OPTION_TYPE_CONFIG) {
                ret = glusterd_gsync_configure (volinfo, slave, dict, resp_dict,
                                                op_errstr);
                goto out;
        }

        if (!volinfo) {
                ret = -1;
                goto out;
        }

        if (type == GF_GSYNC_OPTION_TYPE_START) {

                ret = glusterd_set_marker_gsync (volinfo);
                if (ret != 0) {
                        gf_log ("", GF_LOG_WARNING, "marker start failed");
                        *op_errstr = gf_strdup ("failed to initialize "
                                                "indexing");
                        ret = -1;
                        goto out;
                }
                ret = glusterd_store_slave_in_info(volinfo, slave,
                                                   host_uuid, op_errstr);
                if (ret)
                        goto out;

                ret = glusterd_start_gsync (volinfo, slave, host_uuid,
                                            op_errstr);
        }

        if (type == GF_GSYNC_OPTION_TYPE_STOP) {

                ret = glusterd_gsync_get_uuid (slave, volinfo, uuid);
                if (ret) {
                        gf_log ("", GF_LOG_WARNING, GEOREP" is not set up for"
                                "%s(master) and %s(slave)", volname, slave);
                        *op_errstr = strdup (GEOREP" is not set up");
                        goto out;
                }

                ret = glusterd_remove_slave_in_info(volinfo, slave,
                                                    host_uuid, op_errstr);
                if (ret)
                        goto out;

                if (uuid_compare (priv->uuid, uuid) != 0) {
                        goto out;
                }

                ret = glusterd_stop_gsync (volname, slave, &status_msg);
                if (ret) {
                        gf_log ("glusterd", GF_LOG_ERROR, "Monitor thread of "
                                GEOREP" session between %s & %s already dead",
                                volinfo->volname, slave);
                        ret = dict_set_str (resp_dict, "gsync-status",
                                            status_msg);
                        if (ret)
                                gf_log ("glusterd", GF_LOG_INFO, "dict_set "
                                        "failed while stopping gsync session");
                }
        }

out:
        gf_log ("", GF_LOG_DEBUG,"Returning %d", ret);
        return ret;
}
