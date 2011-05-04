/*
  Copyright (c) 2010 Gluster, Inc. <http://www.gluster.com>
  This file is part of GlusterFS.

  GlusterFS is GF_FREE software; you can redistribute it and/or modify
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

#include <fnmatch.h>

#include "xlator.h"
#include "glusterd.h"
#include "defaults.h"
#include "logging.h"
#include "dict.h"
#include "graph-utils.h"
#include "trie.h"
#include "glusterd-mem-types.h"
#include "cli1.h"
#include "glusterd-volgen.h"
#include "glusterd-op-sm.h"
#include "glusterd-geo-replication.h"


/* dispatch table for VOLUME SET
 * -----------------------------
 *
 * Format of entries:
 *
 * First field is the <key>, for the purpose of looking it up
 * in volume dictionary. Each <key> is of the format "<domain>.<specifier>".
 *
 * Second field is <voltype>.
 *
 * Third field is <option>, if its unset, it's assumed to be
 * the same as <specifier>.
 *
 * Fourth field is <value>. In this context they are used to specify
 * a default. That is, even the volume dict doesn't have a value,
 * we procced as if the default value were set for it.
 *
 * There are two type of entries: basic and special.
 *
 * - Basic entries are the ones where the <option> does _not_ start with
 *   the bang! character ('!').
 *
 *   In their case, <option> is understood as an option for an xlator of
 *   type <voltype>. Their effect is to copy over the volinfo->dict[<key>]
 *   value to all graph nodes of type <voltype> (if such a value is set).
 *
 *   You are free to add entries of this type, they will become functional
 *   just by being present in the table.
 *
 * - Special entries where the <option> starts with the bang!.
 *
 *   They are not applied to all graphs during generation, and you cannot
 *   extend them in a trivial way which could be just picked up. Better
 *   not touch them unless you know what you do.
 *
 * "NODOC" entries are not part of the public interface and are subject
 * to change at any time.
 *
 * Another kind of grouping for options, according to visibility:
 *
 * - Exported: one which is used in the code. These are characterized by
 *   being used a macro as <key> (of the format VKEY_..., defined in
 *   glusterd-volgen.h
 *
 * - Non-exported: the rest; these have string literal <keys>.
 *
 * Adhering to this policy, option name changes shall be one-liners.
 *
 */
typedef enum  { DOC, NO_DOC, GLOBAL_DOC, GLOBAL_NO_DOC } option_type_t;


struct volopt_map_entry {
        char *key;
        char *voltype;
        char *option;
        char *value;
        option_type_t type;
        uint32_t flags;
};

static struct volopt_map_entry glusterd_volopt_map[] = {

        {"cluster.lookup-unhashed",              "cluster/distribute", NULL, NULL, NO_DOC, 0    },
        {"cluster.min-free-disk",                "cluster/distribute", NULL, NULL, NO_DOC, 0    },

        {"cluster.entry-change-log",             "cluster/replicate",  NULL, NULL, NO_DOC, 0     },
        {"cluster.read-subvolume",               "cluster/replicate",  NULL, NULL, NO_DOC, 0    },
        {"cluster.background-self-heal-count",   "cluster/replicate",  NULL, NULL, NO_DOC, 0    },
        {"cluster.metadata-self-heal",           "cluster/replicate",  NULL, NULL, NO_DOC, 0     },
        {"cluster.data-self-heal",               "cluster/replicate",  NULL, NULL, NO_DOC, 0     },
        {"cluster.entry-self-heal",              "cluster/replicate",  NULL, NULL, NO_DOC, 0     },
        {"cluster.strict-readdir",               "cluster/replicate",  NULL, NULL, NO_DOC, 0     },
        {"cluster.self-heal-window-size",        "cluster/replicate",         "data-self-heal-window-size", NULL, DOC, 0},
        {"cluster.data-change-log",              "cluster/replicate",  NULL, NULL, NO_DOC, 0     },
        {"cluster.metadata-change-log",          "cluster/replicate",  NULL, NULL, NO_DOC, 0     },
        {"cluster.data-self-heal-algorithm",     "cluster/replicate",         "data-self-heal-algorithm", NULL,DOC, 0},

        {"cluster.stripe-block-size",            "cluster/stripe",            "block-size", NULL, DOC, 0},

        {VKEY_DIAG_LAT_MEASUREMENT,              "debug/io-stats",     "latency-measurement", "off", NO_DOC, 0      },
        {"diagnostics.dump-fd-stats",            "debug/io-stats",     NULL, NULL, NO_DOC, 0     },
        {VKEY_DIAG_CNT_FOP_HITS,                 "debug/io-stats",     "count-fop-hits", "off", NO_DOC, 0     },
        {"diagnostics.brick-log-level",          "debug/io-stats",            "!log-level", NULL, DOC, 0},
        {"diagnostics.client-log-level",         "debug/io-stats",            "!log-level", NULL, DOC, 0},

        {"performance.cache-max-file-size",      "performance/io-cache",      "max-file-size", NULL, DOC, 0},
        {"performance.cache-min-file-size",      "performance/io-cache",      "min-file-size", NULL, DOC, 0},
        {"performance.cache-refresh-timeout",    "performance/io-cache",      "cache-timeout", NULL, DOC, 0},
        {"performance.cache-priority",           "performance/io-cache",      "priority", NULL, DOC, 0},
        {"performance.cache-size",               "performance/io-cache",   NULL, NULL, NO_DOC, 0 },
        {"performance.cache-size",               "performance/quick-read", NULL, NULL, NO_DOC, 0 },
        {"performance.flush-behind",             "performance/write-behind",      "flush-behind", NULL, DOC, 0},

        {"performance.io-thread-count",          "performance/io-threads",    "thread-count", DOC, 0},

        {"performance.disk-usage-limit",         "performance/quota",   NULL, NULL, NO_DOC, 0    },
        {"performance.min-free-disk-limit",      "performance/quota",   NULL, NULL, NO_DOC, 0    },

        {"performance.write-behind-window-size", "performance/write-behind",  "cache-size", NULL, DOC},

        {"network.frame-timeout",                "protocol/client",    NULL, NULL, NO_DOC, 0     },
        {"network.ping-timeout",                 "protocol/client",    NULL, NULL, NO_DOC, 0     },
        {"network.inode-lru-limit",              "protocol/server",    NULL, NULL, NO_DOC, 0     },

        {"auth.allow",                           "protocol/server",           "!server-auth", "*", DOC, 0},
        {"auth.reject",                          "protocol/server",           "!server-auth", NULL, DOC, 0},

        {"transport.keepalive",                   "protocol/server",           "transport.socket.keepalive", NULL, NO_DOC, 0},
        {"server.allow-insecure",                 "protocol/server",          "rpc-auth-allow-insecure", NULL, NO_DOC, 0},

        {"performance.write-behind",             "performance/write-behind",  "!perf", "on", NO_DOC, 0},
        {"performance.read-ahead",               "performance/read-ahead",    "!perf", "on", NO_DOC, 0},
        {"performance.io-cache",                 "performance/io-cache",      "!perf", "on", NO_DOC, 0},
        {"performance.quick-read",               "performance/quick-read",    "!perf", "on", NO_DOC, 0},
        {VKEY_PERF_STAT_PREFETCH,                "performance/stat-prefetch", "!perf", "on", NO_DOC, 0},

        {VKEY_MARKER_XTIME,                      "features/marker",           "xtime", "off", NO_DOC, OPT_FLAG_FORCE},
        {VKEY_MARKER_XTIME,                      "features/marker",           "!xtime", "off", NO_DOC, OPT_FLAG_FORCE},

        {"nfs.enable-ino32",                     "nfs/server",                "nfs.enable-ino32", NULL, GLOBAL_DOC, 0},
        {"nfs.mem-factor",                       "nfs/server",                "nfs.mem-factor", NULL, GLOBAL_DOC, 0},
        {"nfs.export-dirs",                      "nfs/server",                "nfs3.export-dirs", NULL, GLOBAL_DOC, 0},
        {"nfs.export-volumes",                   "nfs/server",                "nfs3.export-volumes", NULL, GLOBAL_DOC, 0},
        {"nfs.addr-namelookup",                  "nfs/server",                "rpc-auth.addr.namelookup", NULL, GLOBAL_DOC, 0},
        {"nfs.dynamic-volumes",                  "nfs/server",                "nfs.dynamic-volumes", NULL, GLOBAL_DOC, 0},
        {"nfs.register-with-portmap",            "nfs/server",                "rpc.register-with-portmap", NULL, GLOBAL_DOC, 0},
        {"nfs.port",                             "nfs/server",                "nfs.port", NULL, GLOBAL_DOC, 0},

        {"nfs.rpc-auth-unix",                    "nfs/server",                "!nfs.rpc-auth-auth-unix", NULL, DOC, 0},
        {"nfs.rpc-auth-null",                    "nfs/server",                "!nfs.rpc-auth-auth-null", NULL, DOC, 0},
        {"nfs.rpc-auth-allow",                   "nfs/server",                "!nfs.rpc-auth.addr.allow", NULL, DOC, 0},
        {"nfs.rpc-auth-reject",                  "nfs/server",                "!nfs.rpc-auth.addr.reject", NULL, DOC, 0},
        {"nfs.ports-insecure",                   "nfs/server",                "!nfs.auth.ports.insecure", NULL, DOC, 0},

        {"nfs.trusted-sync",                     "nfs/server",                "!nfs-trusted-sync", NULL, DOC, 0},
        {"nfs.trusted-write",                    "nfs/server",                "!nfs-trusted-write", NULL, DOC, 0},
        {"nfs.volume-access",                    "nfs/server",                "!nfs-volume-access", NULL, DOC, 0},
        {"nfs.export-dir",                       "nfs/server",                "!nfs-export-dir", NULL, DOC, 0},
        {"nfs.disable",                          "nfs/server",                "!nfs-disable", NULL, DOC, 0},

        {VKEY_FEATURES_QUOTA,                    "features/marker",           "quota", "off", NO_DOC, OPT_FLAG_FORCE},
        {VKEY_FEATURES_LIMIT_USAGE,              "features/quota",            "limit-set", NULL, NO_DOC, 0},
        {"features.quota-timeout",               "features/quota",            "timeout", "0", NO_DOC, 0},
        {NULL,                                                                }
};



/*********************************************
 *
 * xlator generation / graph manipulation API
 *
 *********************************************/


struct volgen_graph {
        char **errstr;
        glusterfs_graph_t graph;
};
typedef struct volgen_graph volgen_graph_t;

static void
set_graph_errstr (volgen_graph_t *graph, const char *str)
{
        if (!graph->errstr)
                return;

        *graph->errstr = gf_strdup (str);
}

static xlator_t *
xlator_instantiate_va (const char *type, const char *format, va_list arg)
{
        xlator_t *xl = NULL;
        char *volname = NULL;
        int ret = 0;

        ret = gf_vasprintf (&volname, format, arg);
        if (ret < 0) {
                volname = NULL;

                goto error;
        }

        xl = GF_CALLOC (1, sizeof (*xl), gf_common_mt_xlator_t);
        if (!xl)
                goto error;
        ret = xlator_set_type_virtual (xl, type);
        if (ret)
                goto error;
        xl->options = get_new_dict();
        if (!xl->options)
                goto error;
        xl->name = volname;
        INIT_LIST_HEAD (&xl->volume_options);

        return xl;

 error:
        gf_log ("", GF_LOG_ERROR, "creating xlator of type %s failed",
                type);
        if (volname)
                GF_FREE (volname);
        if (xl)
                xlator_destroy (xl);

        return NULL;
}

#ifdef __not_used_as_of_now_
static xlator_t *
xlator_instantiate (const char *type, const char *format, ...)
{
        va_list arg;
        xlator_t *xl;

        va_start (arg, format);
        xl = xlator_instantiate_va (type, format, arg);
        va_end (arg);

        return xl;
}
#endif

static int
volgen_xlator_link (xlator_t *pxl, xlator_t *cxl)
{
        int ret = 0;

        ret = glusterfs_xlator_link (pxl, cxl);
        if (ret == -1) {
                gf_log ("", GF_LOG_ERROR,
                        "Out of memory, cannot link xlators %s <- %s",
                        pxl->name, cxl->name);
        }

        return ret;
}

static int
volgen_graph_link (volgen_graph_t *graph, xlator_t *xl)
{
        int ret = 0;

        /* no need to care about graph->top here */
        if (graph->graph.first)
                ret = volgen_xlator_link (xl, graph->graph.first);
        if (ret == -1) {
                gf_log ("", GF_LOG_ERROR, "failed to add graph entry %s",
                        xl->name);

                return -1;
        }

        return 0;
}

static xlator_t *
volgen_graph_add_as (volgen_graph_t *graph, const char *type,
                     const char *format, ...)
{
        va_list arg;
        xlator_t *xl = NULL;

        va_start (arg, format);
        xl = xlator_instantiate_va (type, format, arg);
        va_end (arg);

        if (!xl)
                return NULL;

        if (volgen_graph_link (graph, xl)) {
                xlator_destroy (xl);

                return NULL;
        } else
                glusterfs_graph_set_first (&graph->graph, xl);

        return xl;
}

static xlator_t *
volgen_graph_add_nolink (volgen_graph_t *graph, const char *type,
                         const char *format, ...)
{
        va_list arg;
        xlator_t *xl = NULL;

        va_start (arg, format);
        xl = xlator_instantiate_va (type, format, arg);
        va_end (arg);

        if (!xl)
                return NULL;

        glusterfs_graph_set_first (&graph->graph, xl);

        return xl;
}

static xlator_t *
volgen_graph_add (volgen_graph_t *graph, char *type, char *volname)
{
        char *shorttype = NULL;

        shorttype = strrchr (type, '/');
        GF_ASSERT (shorttype);
        shorttype++;
        GF_ASSERT (*shorttype);

        return volgen_graph_add_as (graph, type, "%s-%s", volname, shorttype);
}

/* XXX Seems there is no such generic routine?
 * Maybe should put to xlator.c ??
 */
static int
xlator_set_option (xlator_t *xl, char *key, char *value)
{
        char *dval     = NULL;

        dval = gf_strdup (value);
        if (!dval) {
                gf_log ("", GF_LOG_ERROR,
                        "failed to set xlator opt: %s[%s] = %s",
                        xl->name, key, value);

                return -1;
        }

        return dict_set_dynstr (xl->options, key, dval);
}

static inline xlator_t *
first_of (volgen_graph_t *graph)
{
        return (xlator_t *)graph->graph.first;
}




/**************************
 *
 * Trie glue
 *
 *************************/


static int
volopt_selector (int lvl, char **patt, void *param,
                     int (*optcbk)(char *word, void *param))
{
        struct volopt_map_entry *vme = NULL;
        char *w = NULL;
        int i = 0;
        int len = 0;
        int ret = 0;
        char *dot = NULL;

        for (vme = glusterd_volopt_map; vme->key; vme++) {
                w = vme->key;

                for (i = 0; i < lvl; i++) {
                        if (patt[i]) {
                                w = strtail (w, patt[i]);
                                GF_ASSERT (!w || *w);
                                if (!w || *w != '.')
                                        goto next;
                        } else {
                                w = strchr (w, '.');
                                GF_ASSERT (w);
                        }
                        w++;
                }

                dot = strchr (w, '.');
                if (dot) {
                        len = dot - w;
                        w = gf_strdup (w);
                        if (!w)
                                return -1;
                        w[len] = '\0';
                }
                ret = optcbk (w, param);
                if (dot)
                        GF_FREE (w);
                if (ret)
                        return -1;
 next:
                continue;
        }

        return 0;
}

static int
volopt_trie_cbk (char *word, void *param)
{
        return trie_add ((trie_t *)param, word);
}

static int
process_nodevec (struct trienodevec *nodevec, char **hint)
{
        int ret = 0;
        char *hint1 = NULL;
        char *hint2 = NULL;
        char *hintinfx = "";
        trienode_t **nodes = nodevec->nodes;

        if (!nodes[0]) {
                *hint = NULL;
                return 0;
        }

#if 0
        /* Limit as in git */
        if (trienode_get_dist (nodes[0]) >= 6) {
                *hint = NULL;
                return 0;
        }
#endif

        if (trienode_get_word (nodes[0], &hint1))
                return -1;

        if (nodevec->cnt < 2 || !nodes[1]) {
                *hint = hint1;
                return 0;
        }

        if (trienode_get_word (nodes[1], &hint2))
                return -1;

        if (*hint)
                hintinfx = *hint;
        ret = gf_asprintf (hint, "%s or %s%s", hint1, hintinfx, hint2);
        if (ret > 0)
                ret = 0;
        return ret;
}

static int
volopt_trie_section (int lvl, char **patt, char *word, char **hint, int hints)
{
        trienode_t *nodes[] = { NULL, NULL };
        struct trienodevec nodevec = { nodes, 2};
        trie_t *trie = NULL;
        int ret = 0;

        trie = trie_new ();
        if (!trie)
                return -1;

        if (volopt_selector (lvl, patt, trie, &volopt_trie_cbk)) {
                trie_destroy (trie);

                return -1;
        }

        GF_ASSERT (hints <= 2);
        nodevec.cnt = hints;
        ret = trie_measure_vec (trie, word, &nodevec);
        if (ret || !nodevec.nodes[0])
                trie_destroy (trie);

        ret = process_nodevec (&nodevec, hint);
        trie_destroy (trie);

        return ret;
}

static int
volopt_trie (char *key, char **hint)
{
        char *patt[] = { NULL };
        char *fullhint = NULL;
        char *dot = NULL;
        char *dom = NULL;
        int   len = 0;
        int   ret = 0;

        *hint = NULL;

        dot = strchr (key, '.');
        if (!dot)
                return volopt_trie_section (1, patt, key, hint, 2);

        len = dot - key;
        dom = gf_strdup (key);
        if (!dom)
                return -1;
        dom[len] = '\0';

        ret = volopt_trie_section (0, NULL, dom, patt, 1);
        GF_FREE (dom);
        if (ret) {
                patt[0] = NULL;
                goto out;
        }
        if (!patt[0])
                goto out;

        *hint = "...";
        ret = volopt_trie_section (1, patt, dot + 1, hint, 2);
        if (ret)
                goto out;
        if (*hint) {
                ret = gf_asprintf (&fullhint, "%s.%s", patt[0], *hint);
                GF_FREE (*hint);
                if (ret >= 0) {
                        ret = 0;
                        *hint = fullhint;
                }
        }

 out:
        if (patt[0])
                GF_FREE (patt[0]);
        if (ret)
                *hint = NULL;

        return ret;
}




/**************************
 *
 * Volume generation engine
 *
 **************************/


typedef int (*volgen_opthandler_t) (volgen_graph_t *graph,
                                    struct volopt_map_entry *vme,
                                    void *param);

struct opthandler_data {
        volgen_graph_t *graph;
        volgen_opthandler_t handler;
        struct volopt_map_entry *vme;
        gf_boolean_t found;
        gf_boolean_t data_t_fake;
        int rv;
        char *volname;
        void *param;
};

#define pattern_match_options 0


static void
process_option (dict_t *dict, char *key, data_t *value, void *param)
{
        struct opthandler_data *odt = param;
        struct volopt_map_entry vme = {0,};

        if (odt->rv)
                return;
#if pattern_match_options
        if (fnmatch (odt->vme->key, key, 0) != 0)
                return;
#endif
        odt->found = _gf_true;

        vme.key = key;
        vme.voltype = odt->vme->voltype;
        vme.option = odt->vme->option;
        if (!vme.option) {
                vme.option = strrchr (key, '.');
                if (vme.option)
                        vme.option++;
                else
                        vme.option = key;
        }
        if (odt->data_t_fake)
                vme.value = (char *)value;
        else
                vme.value = value->data;

        odt->rv = odt->handler (odt->graph, &vme, odt->param);
}

static int
volgen_graph_set_options_generic (volgen_graph_t *graph, dict_t *dict,
                                  void *param, volgen_opthandler_t handler)
{
        struct volopt_map_entry *vme = NULL;
        struct opthandler_data odt = {0,};
        data_t *data = NULL;

        odt.graph = graph;
        odt.handler = handler;
        odt.param = param;
        (void)data;

        for (vme = glusterd_volopt_map; vme->key; vme++) {
                odt.vme = vme;
                odt.found = _gf_false;
                odt.data_t_fake = _gf_false;

#if pattern_match_options
                dict_foreach (dict, process_option, &odt);
#else
                data = dict_get (dict, vme->key);

                if (data)
                        process_option (dict, vme->key, data, &odt);
#endif
                if (odt.rv)
                        return odt.rv;

                if (odt.found)
                        continue;

                /* check for default value */

                if (vme->value) {
                        /* stupid hack to be able to reuse dict iterator
                         * in this context
                         */
                        odt.data_t_fake = _gf_true;
                        process_option (NULL, vme->key, (data_t *)vme->value,
                                        &odt);
                        if (odt.rv)
                                return odt.rv;
                }
        }

        return 0;
}

static int
basic_option_handler (volgen_graph_t *graph, struct volopt_map_entry *vme,
                      void *param)
{
        xlator_t *trav;
        int ret = 0;

        if (vme->option[0] == '!')
                return 0;

        for (trav = first_of (graph); trav; trav = trav->next) {
                if (strcmp (trav->type, vme->voltype) != 0)
                        continue;

                ret = xlator_set_option (trav, vme->option, vme->value);
                if (ret)
                        return -1;
        }

        return 0;
}

static int
volgen_graph_set_options (volgen_graph_t *graph, dict_t *dict)
{
        return volgen_graph_set_options_generic (graph, dict, NULL,
                                                 &basic_option_handler);
}

static int
optget_option_handler (volgen_graph_t *graph, struct volopt_map_entry *vme,
                       void *param)
{
        struct volopt_map_entry *vme2 = param;

        if (strcmp (vme->key, vme2->key) == 0)
                vme2->value = vme->value;

        return 0;
}

/* This getter considers defaults also. */
static int
volgen_dict_get (dict_t *dict, char *key, char **value)
{
        struct volopt_map_entry vme = {0,};
        int ret = 0;

        vme.key = key;

        ret = volgen_graph_set_options_generic (NULL, dict, &vme,
                                                &optget_option_handler);
        if (ret) {
                gf_log ("", GF_LOG_ERROR, "Out of memory");

                return -1;
        }

        *value = vme.value;

        return 0;
}

static int
option_complete (char *key, char **completion)
{
        struct volopt_map_entry *vme = NULL;

        *completion = NULL;
        for (vme = glusterd_volopt_map; vme->key; vme++) {
                if (strcmp (strchr (vme->key, '.') + 1, key) != 0)
                        continue;

                if (*completion && strcmp (*completion, vme->key) != 0) {
                        /* cancel on non-unique match */
                        *completion = NULL;

                        return 0;
                } else
                        *completion = vme->key;
        }

        if (*completion) {
                /* For sake of unified API we want
                 * have the completion to be a to-be-freed
                 * string.
                 */
                *completion = gf_strdup (*completion);
                return -!*completion;
        }

        return 0;
}

int
glusterd_volinfo_get (glusterd_volinfo_t *volinfo, char *key, char **value)
{
        return volgen_dict_get (volinfo->dict, key, value);
}

int
glusterd_volinfo_get_boolean (glusterd_volinfo_t *volinfo, char *key)
{
        char *val = NULL;
        gf_boolean_t  boo = _gf_false;
        int ret = 0;

        ret = glusterd_volinfo_get (volinfo, key, &val);
        if (ret)
                return -1;

        if (val)
                ret = gf_string2boolean (val, &boo);
        if (ret) {
                gf_log ("", GF_LOG_ERROR, "value for %s option is not valid", key);

                return -1;
        }

        return boo;
}

gf_boolean_t
glusterd_check_voloption_flags (char *key, int32_t flags)
{
        char *completion = NULL;
        struct volopt_map_entry *vmep = NULL;
        int   ret = 0;

        if (!strchr (key, '.')) {
                ret = option_complete (key, &completion);
                if (ret) {
                        gf_log ("", GF_LOG_ERROR, "Out of memory");
                        return _gf_false;
                }

                if (!completion) {
                        gf_log ("", GF_LOG_ERROR, "option %s does not exist",
                                        key);
                        return _gf_false;
                }
        }

        for (vmep = glusterd_volopt_map; vmep->key; vmep++) {
                if (strcmp (vmep->key, key) == 0) {
                        if (vmep->flags & flags)
                                return _gf_true;
                        else
                                return _gf_false;
                }
        }

        return _gf_false;
}

gf_boolean_t
glusterd_check_globaloption (char *key)
{
        char *completion = NULL;
        struct volopt_map_entry *vmep = NULL;
        int   ret = 0;

        if (!strchr (key, '.')) {
                ret = option_complete (key, &completion);
                if (ret) {
                        gf_log ("", GF_LOG_ERROR, "Out of memory");
                        return _gf_false;
                }

                if (!completion) {
                        gf_log ("", GF_LOG_ERROR, "option %s does not exist",
                                        key);
                        return _gf_false;
                }
        }

        for (vmep = glusterd_volopt_map; vmep->key; vmep++) {
                if (strcmp (vmep->key, key) == 0) {
                        if ((vmep->type == GLOBAL_DOC) ||
                            (vmep->type == GLOBAL_NO_DOC))
                                return _gf_true;
                        else
                                return _gf_false;
                }
        }

        return _gf_false;
}

gf_boolean_t
glusterd_check_localoption (char *key)
{
        char *completion = NULL;
        struct volopt_map_entry *vmep = NULL;
        int   ret = 0;

        if (!strchr (key, '.')) {
                ret = option_complete (key, &completion);
                if (ret) {
                        gf_log ("", GF_LOG_ERROR, "Out of memory");
                        return _gf_false;
                }

                if (!completion) {
                        gf_log ("", GF_LOG_ERROR, "option %s does not exist",
                                        key);
                        return _gf_false;
               }
        }

        for (vmep = glusterd_volopt_map; vmep->key; vmep++) {
                if (strcmp (vmep->key, key) == 0) {
                        if ((vmep->type == DOC) ||
                            (vmep->type == NO_DOC))
                                return _gf_true;
                        else
                                return _gf_false;
                }
        }

        return _gf_false;
}

int
glusterd_check_voloption (char *key)
{
        char *completion = NULL;
        struct volopt_map_entry *vmep = NULL;
        int ret = 0;

        if (!strchr (key, '.')) {
                ret = option_complete (key, &completion);
                if (ret) {
                        gf_log ("", GF_LOG_ERROR, "Out of memory");
                        return _gf_false;
                }

                if (!completion) {
                        gf_log ("", GF_LOG_ERROR, "option %s does not exist",
                                        key);
                        return _gf_false;
                }
        }

        for (vmep = glusterd_volopt_map; vmep->key; vmep++) {
                if (strcmp (vmep->key, key) == 0) {
                        if ((vmep->type == DOC) ||
                            (vmep->type == DOC))
                                return _gf_true;
                        else
                                return _gf_false;
                }
        }

        return _gf_false;

}

int
glusterd_check_option_exists (char *key, char **completion)
{
        dict_t *dict = NULL;
        struct volopt_map_entry vme = {0,};
        struct volopt_map_entry *vmep = NULL;
        int ret = 0;

        (void)vme;
        (void)vmep;
        (void)dict;

        if (!strchr (key, '.')) {
                if (completion) {
                        ret = option_complete (key, completion);
                        if (ret) {
                                gf_log ("", GF_LOG_ERROR, "Out of memory");
                                return -1;
                        }

                        ret = !!*completion;
                        if (ret)
                                return ret;
                        else
                                goto trie;
                } else
                        return 0;
        }

#if !pattern_match_options
        for (vmep = glusterd_volopt_map; vmep->key; vmep++) {
                if (strcmp (vmep->key, key) == 0) {
                        ret = 1;
                        break;
                }
        }
#else
        vme.key = key;

        /* We are getting a bit anal here to avoid typing
         * fnmatch one more time. Orthogonality foremost!
         * The internal logic of looking up in the volopt_map table
         * should be coded exactly once.
         *
         * [[Ha-ha-ha, so now if I ever change the internals then I'll
         * have to update the fnmatch in this comment also :P ]]
         */
        dict = get_new_dict ();
        if (!dict || dict_set_str (dict, key, "")) {
                gf_log ("", GF_LOG_ERROR, "Out of memory");

                return -1;
        }

        ret = volgen_graph_set_options_generic (NULL, dict, &vme,
                                                &optget_option_handler);
        dict_destroy (dict);
        if (ret) {
                gf_log ("", GF_LOG_ERROR, "Out of memory");

                return -1;
        }

        ret = !!vme.value;
#endif

        if (ret || !completion)
                return ret;

 trie:
        ret = volopt_trie (key, completion);
        if (ret) {
                gf_log ("", GF_LOG_ERROR,
                        "Some error occured during keyword hinting");
        }

        return ret;
}

static int
volgen_graph_merge_sub (volgen_graph_t *dgraph, volgen_graph_t *sgraph)
{
        xlator_t *trav = NULL;

        GF_ASSERT (dgraph->graph.first);

        if (volgen_xlator_link (first_of (dgraph), first_of (sgraph)) == -1)
                return -1;

        for (trav = first_of (dgraph); trav->next; trav = trav->next);

        trav->next = first_of (sgraph);
        trav->next->prev = trav;
        dgraph->graph.xl_count += sgraph->graph.xl_count;

        return 0;
}

static int
volgen_write_volfile (volgen_graph_t *graph, char *filename)
{
        char *ftmp = NULL;
        FILE *f = NULL;

        if (gf_asprintf (&ftmp, "%s.tmp", filename) == -1) {
                ftmp = NULL;

                goto error;
        }

        f = fopen (ftmp, "w");
        if (!f)
                goto error;

        if (glusterfs_graph_print_file (f, &graph->graph) == -1)
                goto error;

        if (fclose (f) == -1)
                goto error;
        f = NULL;

        if (rename (ftmp, filename) == -1)
                goto error;

        GF_FREE (ftmp);

        return 0;

 error:

        if (ftmp)
                GF_FREE (ftmp);
        if (f)
                fclose (f);

        gf_log ("", GF_LOG_ERROR, "failed to create volfile %s", filename);

        return -1;
}

static void
volgen_graph_free (volgen_graph_t *graph)
{
        xlator_t *trav = NULL;
        xlator_t *trav_old = NULL;

        for (trav = first_of (graph) ;; trav = trav->next) {
                if (trav_old)
                        xlator_destroy (trav_old);

                trav_old = trav;

                if (!trav)
                        break;
        }
}

static int
build_graph_generic (volgen_graph_t *graph, glusterd_volinfo_t *volinfo,
                     dict_t *mod_dict, void *param,
                     int (*builder) (volgen_graph_t *graph,
                                     glusterd_volinfo_t *volinfo,
                                     dict_t *set_dict, void *param))
{
        dict_t *set_dict = NULL;
        int ret = 0;

        if (mod_dict) {
                set_dict = dict_copy (volinfo->dict, NULL);
                if (!set_dict)
                        return -1;
                dict_copy (mod_dict, set_dict);
                /* XXX dict_copy swallows errors */
        } else
                set_dict = volinfo->dict;

        ret = builder (graph, volinfo, set_dict, param);
        if (!ret)
                ret = volgen_graph_set_options (graph, set_dict);

        if (mod_dict)
                dict_destroy (set_dict);

        return ret;
}

static void
get_vol_transport_type (glusterd_volinfo_t *volinfo, char *tt)
{
        switch (volinfo->transport_type) {
        case GF_TRANSPORT_RDMA:
                strcpy (tt, "rdma");
                break;
        case GF_TRANSPORT_TCP:
                strcpy (tt, "tcp");
                break;
        case GF_TRANSPORT_BOTH_TCP_RDMA:
                strcpy (tt, "tcp,rdma");
                break;
        }
}

static int
server_auth_option_handler (volgen_graph_t *graph,
                            struct volopt_map_entry *vme, void *param)
{
        xlator_t *xl = NULL;
        xlator_list_t *trav = NULL;
        char *aa = NULL;
        int   ret   = 0;
        char *key = NULL;

        if (strcmp (vme->option, "!server-auth") != 0)
                return 0;

        xl = first_of (graph);

        /* from 'auth.allow' -> 'allow', and 'auth.reject' -> 'reject' */
        key = strchr (vme->key, '.') + 1;

        for (trav = xl->children; trav; trav = trav->next) {
                ret = gf_asprintf (&aa, "auth.addr.%s.%s", trav->xlator->name,
                                   key);
                if (ret != -1) {
                        ret = xlator_set_option (xl, aa, vme->value);
                        GF_FREE (aa);
                }
                if (ret)
                        return -1;
        }

        return 0;
}

static int
loglevel_option_handler (volgen_graph_t *graph,
                         struct volopt_map_entry *vme, void *param)
{
        char *role = param;
        struct volopt_map_entry vme2 = {0,};

        if (strcmp (vme->option, "!log-level") != 0 ||
            !strstr (vme->key, role))
                return 0;

        memcpy (&vme2, vme, sizeof (vme2));
        vme2.option = "log-level";

        return basic_option_handler (graph, &vme2, NULL);
}

static int
server_check_marker_off (volgen_graph_t *graph, struct volopt_map_entry *vme,
                         glusterd_volinfo_t *volinfo)
{
        gf_boolean_t           bool = _gf_false;
        int                    ret = 0;

        GF_ASSERT (volinfo);
        GF_ASSERT (vme);

        if (strcmp (vme->option, "!xtime") != 0)
                return 0;

        ret = gf_string2boolean (vme->value, &bool);
        if (ret || bool)
                goto out;

        ret = glusterd_volinfo_get_boolean (volinfo, VKEY_MARKER_XTIME);
        if (ret < 0) {
                gf_log ("", GF_LOG_WARNING, "failed to get the marker status");
                ret = -1;
                goto out;
        }

        if (ret) {
                bool = _gf_false;
                ret = glusterd_check_gsync_running (volinfo, &bool);

                if (bool) {
                        gf_log ("", GF_LOG_WARNING, GEOREP" sessions active"
                                "for the volume %s, cannot disable marker "
                                ,volinfo->volname);
                        set_graph_errstr (graph,
                                          VKEY_MARKER_XTIME" cannot be disabled "
                                          "while "GEOREP" sessions exist");
                        ret = -1;
                        goto out;
                }

                if (ret) {
                        gf_log ("", GF_LOG_WARNING, "Unable to get the status"
                                 " of active gsync session");
                        goto out;
                }
        }

        ret = 0;
 out:
        gf_log ("", GF_LOG_DEBUG, "Returning %d", ret);
        return ret;

}

static int
server_spec_option_handler (volgen_graph_t *graph,
                            struct volopt_map_entry *vme, void *param)
{
        int                     ret = 0;
        glusterd_volinfo_t      *volinfo = NULL;

        volinfo = param;

        ret = server_auth_option_handler (graph, vme, NULL);
        if (!ret)
                ret = server_check_marker_off (graph, vme, volinfo);

        if (!ret)
                ret = loglevel_option_handler (graph, vme, "brick");

        return ret;
}

static void get_vol_tstamp_file (char *filename, glusterd_volinfo_t *volinfo);

static int
server_graph_builder (volgen_graph_t *graph, glusterd_volinfo_t *volinfo,
                      dict_t *set_dict, void *param)
{
        char     *volname = NULL;
        char     *path = NULL;
        int       pump = 0;
        xlator_t *xl = NULL;
        xlator_t *txl = NULL;
        xlator_t *rbxl = NULL;
        char      transt[16] = {0,};
        char      volume_id[64] = {0,};
        char      tstamp_file[PATH_MAX] = {0,};
        int       ret = 0;

        path = param;
        volname = volinfo->volname;
        get_vol_transport_type (volinfo, transt);

        xl = volgen_graph_add (graph, "storage/posix", volname);
        if (!xl)
                return -1;

        ret = xlator_set_option (xl, "directory", path);
        if (ret)
                return -1;

        xl = volgen_graph_add (graph, "features/access-control", volname);
        if (!xl)
                return -1;

        xl = volgen_graph_add (graph, "features/locks", volname);
        if (!xl)
                return -1;

        xl = volgen_graph_add (graph, "performance/io-threads", volname);
        if (!xl)
                return -1;

        ret = dict_get_int32 (volinfo->dict, "enable-pump", &pump);
        if (ret == -ENOENT)
                ret = pump = 0;
        if (ret)
                return -1;
        if (pump) {
                txl = first_of (graph);

                rbxl = volgen_graph_add_nolink (graph, "protocol/client",
                                                "%s-replace-brick", volname);
                if (!rbxl)
                        return -1;
                ret = xlator_set_option (rbxl, "transport-type", transt);
                if (ret)
                        return -1;
                xl = volgen_graph_add_nolink (graph, "cluster/pump", "%s-pump",
                                              volname);
                if (!xl)
                        return -1;
                ret = volgen_xlator_link (xl, txl);
                if (ret)
                        return -1;
                ret = volgen_xlator_link (xl, rbxl);
                if (ret)
                        return -1;
        }

        xl = volgen_graph_add (graph, "features/marker", volname);
        if (!xl)
                return -1;
        uuid_unparse (volinfo->volume_id, volume_id);
        ret = xlator_set_option (xl, "volume-uuid", volume_id);
        if (ret)
                return -1;
        get_vol_tstamp_file (tstamp_file, volinfo);
        ret = xlator_set_option (xl, "timestamp-file", tstamp_file);
        if (ret)
                return -1;

        xl = volgen_graph_add_as (graph, "debug/io-stats", path);
        if (!xl)
                return -1;

        xl = volgen_graph_add (graph, "protocol/server", volname);
        if (!xl)
                return -1;
        ret = xlator_set_option (xl, "transport-type", transt);
        if (ret)
                return -1;

        ret = volgen_graph_set_options_generic (graph, set_dict, volinfo,
                                                &server_spec_option_handler);

        return ret;
}


/* builds a graph for server role , with option overrides in mod_dict */
static int
build_server_graph (volgen_graph_t *graph, glusterd_volinfo_t *volinfo,
                    dict_t *mod_dict, char *path)
{
        return build_graph_generic (graph, volinfo, mod_dict, path,
                                    &server_graph_builder);
}

static int
perfxl_option_handler (volgen_graph_t *graph, struct volopt_map_entry *vme,
                       void *param)
{
        char *volname = NULL;
        gf_boolean_t enabled = _gf_false;

        volname = param;

        if (strcmp (vme->option, "!perf") != 0)
                return 0;

        if (gf_string2boolean (vme->value, &enabled) == -1)
                return -1;
        if (!enabled)
                return 0;

        if (volgen_graph_add (graph, vme->voltype, volname))
                return 0;
        else
                return -1;
}

static int
client_graph_builder (volgen_graph_t *graph, glusterd_volinfo_t *volinfo,
                      dict_t *set_dict, void *param)
{
        int                      dist_count         = 0;
        char                     transt[16]         = {0,};
        char                    *tt                 = NULL;
        char                    *volname            = NULL;
        dict_t                  *dict               = NULL;
        glusterd_brickinfo_t    *brick = NULL;
        char                    *replicate_args[]   = {"cluster/replicate",
                                                       "%s-replicate-%d"};
        char                    *stripe_args[]      = {"cluster/stripe",
                                                       "%s-stripe-%d"};
        char                   **cluster_args       = NULL;
        int                      i                  = 0;
        int                      j                  = 0;
        int                      ret                = 0;
        xlator_t                *xl                 = NULL;
        xlator_t                *txl                = NULL;
        xlator_t                *trav               = NULL;

        volname = volinfo->volname;
        dict    = volinfo->dict;
        GF_ASSERT (dict);

        if (volinfo->brick_count == 0) {
                gf_log ("", GF_LOG_ERROR,
                        "volume inconsistency: brick count is 0");

                return -1;
        }
        if (volinfo->sub_count && volinfo->sub_count < volinfo->brick_count &&
            volinfo->brick_count % volinfo->sub_count != 0) {
                gf_log ("", GF_LOG_ERROR,
                        "volume inconsistency: "
                        "total number of bricks (%d) is not divisible with "
                        "number of bricks per cluster (%d) in a multi-cluster "
                        "setup",
                        volinfo->brick_count, volinfo->sub_count);
                return -1;
        }

        ret = dict_get_str (set_dict, "client-transport-type", &tt);
        if (ret)
                get_vol_transport_type (volinfo, transt);
        if (!ret)
                strcpy (transt, tt);

        i = 0;
        list_for_each_entry (brick, &volinfo->bricks, brick_list) {
                xl = volgen_graph_add_nolink (graph, "protocol/client",
                                              "%s-client-%d", volname, i);
                if (!xl)
                        return -1;
                ret = xlator_set_option (xl, "remote-host", brick->hostname);
                if (ret)
                        return -1;
                ret = xlator_set_option (xl, "remote-subvolume", brick->path);
                if (ret)
                        return -1;
                ret = xlator_set_option (xl, "transport-type", transt);
                if (ret)
                        return -1;

                i++;
        }
        if (i != volinfo->brick_count) {
                gf_log ("", GF_LOG_ERROR,
                        "volume inconsistency: actual number of bricks (%d) "
                        "differs from brick count (%d)", i,
                        volinfo->brick_count);

                return -1;
        }

        if (volinfo->sub_count > 1) {
                switch (volinfo->type) {
                case GF_CLUSTER_TYPE_REPLICATE:
                        cluster_args = replicate_args;
                        break;
                case GF_CLUSTER_TYPE_STRIPE:
                        cluster_args = stripe_args;
                        break;
                default:
                        gf_log ("", GF_LOG_ERROR, "volume inconsistency: "
                                "unrecognized clustering type");
                        return -1;
                }

                i = 0;
                j = 0;
                txl = first_of (graph);
                for (trav = txl; trav->next; trav = trav->next);
                for (;; trav = trav->prev) {
                        if (i % volinfo->sub_count == 0) {
                                xl = volgen_graph_add_nolink (graph,
                                                              cluster_args[0],
                                                              cluster_args[1],
                                                              volname, j);
                                if (!xl)
                                        return -1;
                                j++;
                        }

                        ret = volgen_xlator_link (xl, trav);
                        if (ret)
                                return -1;

                        if (trav == txl)
                                break;
                        i++;
                }
        }

        if (volinfo->sub_count)
                dist_count = volinfo->brick_count / volinfo->sub_count;
        else
                dist_count = volinfo->brick_count;
        if (dist_count > 1) {
                xl = volgen_graph_add_nolink (graph, "cluster/distribute",
                                              "%s-dht", volname);
                if (!xl)
                        return -1;

                trav = xl;
                for (i = 0; i < dist_count; i++)
                        trav = trav->next;
                for (; trav != xl; trav = trav->prev) {
                        ret = volgen_xlator_link (xl, trav);
                        if (ret)
                                return -1;
                }
        }

        ret = glusterd_volinfo_get_boolean (volinfo, VKEY_FEATURES_QUOTA);
        if (ret == -1)
                return -1;
        if (ret) {
                xl = volgen_graph_add (graph, "features/quota", volname);

                if (!xl)
                        return -1;
        }

        ret = volgen_graph_set_options_generic (graph, set_dict, volname,
                                                &perfxl_option_handler);
        if (ret)
                return -1;

        xl = volgen_graph_add_as (graph, "debug/io-stats", volname);
        if (!xl)
                return -1;

        ret = volgen_graph_set_options_generic (graph, set_dict, "client",
                                                &loglevel_option_handler);

        return ret;
}


/* builds a graph for client role , with option overrides in mod_dict */
static int
build_client_graph (volgen_graph_t *graph, glusterd_volinfo_t *volinfo,
                    dict_t *mod_dict)
{
        return build_graph_generic (graph, volinfo, mod_dict, NULL,
                                    &client_graph_builder);
}

static int
nfs_option_handler (volgen_graph_t *graph,
                            struct volopt_map_entry *vme, void *param)
{
        xlator_t *xl = NULL;
        char *aa = NULL;
        int   ret   = 0;
        glusterd_volinfo_t *volinfo = NULL;

        volinfo = param;

        xl = first_of (graph);

/*        if (vme->type  == GLOBAL_DOC || vme->type == GLOBAL_NO_DOC) {

                ret = xlator_set_option (xl, vme->key, vme->value);
        }*/
        if ( !volinfo || !volinfo->volname)
                return 0;

        if (! strcmp (vme->option, "!nfs.rpc-auth-addr-allow")) {
                ret = gf_asprintf (&aa, "rpc-auth.addr.%s.allow",
                                        volinfo->volname);

                if (ret != -1) {
                        ret = xlator_set_option (xl, aa, vme->value);
                        GF_FREE (aa);
                }

                if (ret)
                        return -1;
        }

        if (! strcmp (vme->option, "!nfs.rpc-auth-addr-reject")) {
                ret = gf_asprintf (&aa, "rpc-auth.addr.%s.reject",
                                        volinfo->volname);

                if (ret != -1) {
                        ret = xlator_set_option (xl, aa, vme->value);
                        GF_FREE (aa);
                }

                if (ret)
                        return -1;
        }

        if (! strcmp (vme->option, "!nfs.rpc-auth-auth-unix")) {
                ret = gf_asprintf (&aa, "rpc-auth.auth.unix.%s",
                                        volinfo->volname);

                if (ret != -1) {
                        ret = xlator_set_option (xl, aa, vme->value);
                        GF_FREE (aa);
                }

                if (ret)
                        return -1;
        }
        if (! strcmp (vme->option, "!nfs.rpc-auth-auth-null")) {
                ret = gf_asprintf (&aa, "rpc-auth.auth.null.%s",
                                        volinfo->volname);

                if (ret != -1) {
                        ret = xlator_set_option (xl, aa, vme->value);
                        GF_FREE (aa);
                }

                if (ret)
                        return -1;
        }

        if (! strcmp (vme->option, "!nfs-trusted-sync")) {
                ret = gf_asprintf (&aa, "nfs3.%s.trusted-sync",
                                        volinfo->volname);

                if (ret != -1) {
                        ret = xlator_set_option (xl, aa, vme->value);
                        GF_FREE (aa);
                }

                if (ret)
                        return -1;
        }

        if (! strcmp (vme->option, "!nfs-trusted-write")) {
                ret = gf_asprintf (&aa, "nfs3.%s.trusted-write",
                                        volinfo->volname);

                if (ret != -1) {
                        ret = xlator_set_option (xl, aa, vme->value);
                        GF_FREE (aa);
                }

                if (ret)
                        return -1;
        }

        if (! strcmp (vme->option, "!nfs-volume-access")) {
                ret = gf_asprintf (&aa, "nfs3.%s.volume-access",
                                        volinfo->volname);

                if (ret != -1) {
                        ret = xlator_set_option (xl, aa, vme->value);
                        GF_FREE (aa);
                }

                if (ret)
                        return -1;
        }

        if (! strcmp (vme->option, "!nfs-export-dir")) {
                ret = gf_asprintf (&aa, "nfs3.%s.export-dir",
                                        volinfo->volname);

                if (ret != -1) {
                        ret = xlator_set_option (xl, aa, vme->value);
                        GF_FREE (aa);
                }

                if (ret)
                        return -1;
        }



        if (! strcmp (vme->option, "!nfs.ports-insecure")) {
                ret = gf_asprintf (&aa, "rpc-auth.ports.%s.insecure",
                                        volinfo->volname);

                if (ret != -1) {
                        ret = xlator_set_option (xl, aa, vme->value);
                        GF_FREE (aa);
                }

                if (ret)
                        return -1;
        }


        if (! strcmp (vme->option, "!nfs-disable")) {
                ret = gf_asprintf (&aa, "nfs.%s.disable",
                                        volinfo->volname);

                if (ret != -1) {
                        ret = xlator_set_option (xl, aa, vme->value);
                        GF_FREE (aa);
                }

                if (ret)
                        return -1;
        }


        /*key = strchr (vme->key, '.') + 1;

        for (trav = xl->children; trav; trav = trav->next) {
                ret = gf_asprintf (&aa, "auth.addr.%s.%s", trav->xlator->name,
                                   key);
                if (ret != -1) {
                        ret = xlator_set_option (xl, aa, vme->value);
                        GF_FREE (aa);
                }
                if (ret)
                        return -1;
        }*/

        return 0;
}

static int
nfs_spec_option_handler (volgen_graph_t *graph,
                            struct volopt_map_entry *vme, void *param)
{
        int ret = 0;

        ret = nfs_option_handler (graph, vme, param);
        if (!ret)
                return basic_option_handler (graph, vme, NULL);
        return ret;
}

/* builds a graph for nfs server role, with option overrides in mod_dict */
static int
build_nfs_graph (volgen_graph_t *graph, dict_t *mod_dict)
{
        volgen_graph_t      cgraph        = {0,};
        glusterd_volinfo_t *voliter       = NULL;
        xlator_t           *this          = NULL;
        glusterd_conf_t    *priv          = NULL;
        dict_t             *set_dict      = NULL;
        xlator_t           *nfsxl         = NULL;
        char               *skey          = NULL;
        int                 ret           = 0;

        this = THIS;
        GF_ASSERT (this);
        priv = this->private;
        GF_ASSERT (priv);

        set_dict = dict_new ();
        if (!set_dict) {
                gf_log ("", GF_LOG_ERROR, "Out of memory");
                return -1;
        }

        ret = dict_set_str (set_dict, VKEY_PERF_STAT_PREFETCH, "off");
        if (ret)
                goto out;

        nfsxl = volgen_graph_add_as (graph, "nfs/server", "nfs-server");
        if (!nfsxl) {
                ret = -1;
                goto out;
        }
        ret = xlator_set_option (nfsxl, "nfs.dynamic-volumes", "on");
        if (ret)
                goto out;;

        list_for_each_entry (voliter, &priv->volumes, vol_list) {
                if (voliter->status != GLUSTERD_STATUS_STARTED)
                        continue;

                if (dict_get_str_boolean (voliter->dict, "nfs.disable", 0))
                        continue;

                ret = gf_asprintf (&skey, "rpc-auth.addr.%s.allow",
                                   voliter->volname);
                if (ret == -1) {
                        gf_log ("", GF_LOG_ERROR, "Out of memory");
                        goto out;
                }
                ret = xlator_set_option (nfsxl, skey, "*");
                GF_FREE (skey);
                if (ret)
                        goto out;

                ret = gf_asprintf (&skey, "nfs3.%s.volume-id",
                                   voliter->volname);
                if (ret == -1) {
                        gf_log ("", GF_LOG_ERROR, "Out of memory");
                        goto out;
                }
                ret = xlator_set_option (nfsxl, skey, uuid_utoa (voliter->volume_id));
                GF_FREE (skey);
                if (ret)
                        goto out;

                /* If both RDMA and TCP are the transport_type, use RDMA
                   for NFS client protocols */
                if (voliter->transport_type == GF_TRANSPORT_BOTH_TCP_RDMA) {
                        ret = dict_set_str (set_dict, "client-transport-type",
                                            "rdma");
                        if (ret)
                                goto out;
                }

                memset (&cgraph, 0, sizeof (cgraph));
                ret = build_client_graph (&cgraph, voliter, mod_dict);
                if (ret)
                        goto out;;
                ret = volgen_graph_merge_sub (graph, &cgraph);
                if (ret)
                        goto out;

                if (mod_dict) {
                        dict_copy (mod_dict, set_dict);
                        ret = volgen_graph_set_options_generic (graph, set_dict, voliter,
                                                        nfs_spec_option_handler);
                }
                else
                        ret = volgen_graph_set_options_generic (graph, voliter->dict, voliter,
                                                        nfs_spec_option_handler);

        }



 out:
        dict_destroy (set_dict);

        return ret;
}




/****************************
 *
 * Volume generation interface
 *
 ****************************/


static void
get_brick_filepath (char *filename, glusterd_volinfo_t *volinfo,
                    glusterd_brickinfo_t *brickinfo)
{
        char  path[PATH_MAX]   = {0,};
        char  brick[PATH_MAX]  = {0,};
        glusterd_conf_t *priv  = NULL;

        priv = THIS->private;

        GLUSTERD_REMOVE_SLASH_FROM_PATH (brickinfo->path, brick);
        GLUSTERD_GET_VOLUME_DIR (path, volinfo, priv);

        snprintf (filename, PATH_MAX, "%s/%s.%s.%s.vol",
                  path, volinfo->volname,
                  brickinfo->hostname,
                  brick);
}

static int
glusterd_generate_brick_volfile (glusterd_volinfo_t *volinfo,
                                 glusterd_brickinfo_t *brickinfo)
{
        volgen_graph_t graph = {0,};
        char    filename[PATH_MAX] = {0,};
        int     ret = -1;

        GF_ASSERT (volinfo);
        GF_ASSERT (brickinfo);

        get_brick_filepath (filename, volinfo, brickinfo);

        ret = build_server_graph (&graph, volinfo, NULL, brickinfo->path);
        if (!ret)
                ret = volgen_write_volfile (&graph, filename);

        volgen_graph_free (&graph);

        return ret;
}

static void
get_vol_tstamp_file (char *filename, glusterd_volinfo_t *volinfo)
{
        glusterd_conf_t *priv  = NULL;

        priv = THIS->private;

        GLUSTERD_GET_VOLUME_DIR (filename, volinfo, priv);
        strncat (filename, "/marker.tstamp",
                 PATH_MAX - strlen(filename) - 1);
}

static int
generate_brick_volfiles (glusterd_volinfo_t *volinfo)
{
        glusterd_brickinfo_t    *brickinfo = NULL;
        char                     tstamp_file[PATH_MAX] = {0,};
        int                      ret = -1;

        ret = glusterd_volinfo_get_boolean (volinfo, VKEY_MARKER_XTIME);
        if (ret == -1)
                return -1;

        get_vol_tstamp_file (tstamp_file, volinfo);

        if (ret) {
                ret = open (tstamp_file, O_WRONLY|O_CREAT|O_EXCL, 0644);
                if (ret == -1 && errno == EEXIST) {
                        gf_log ("", GF_LOG_DEBUG, "timestamp file exist");
                        ret = -2;
                }
                if (ret == -1) {
                        gf_log ("", GF_LOG_ERROR, "failed to create %s (%s)",
                                tstamp_file, strerror (errno));
                        return -1;
                }
                if (ret >= 0)
                        close (ret);
        } else {
                ret = unlink (tstamp_file);
                if (ret == -1 && errno == ENOENT)
                        ret = 0;
                if (ret == -1) {
                        gf_log ("", GF_LOG_ERROR, "failed to unlink %s (%s)",
                                tstamp_file, strerror (errno));
                        return -1;
                }
        }

        list_for_each_entry (brickinfo, &volinfo->bricks, brick_list) {
                gf_log ("", GF_LOG_DEBUG,
                        "Found a brick - %s:%s", brickinfo->hostname,
                        brickinfo->path);

                ret = glusterd_generate_brick_volfile (volinfo, brickinfo);
                if (ret)
                        goto out;

        }

        ret = 0;

out:
        gf_log ("", GF_LOG_DEBUG, "Returning %d", ret);
        return ret;
}

static void
get_client_filepath (char *filename, glusterd_volinfo_t *volinfo)
{
        char  path[PATH_MAX] = {0,};
        glusterd_conf_t *priv = NULL;

        priv = THIS->private;

        GLUSTERD_GET_VOLUME_DIR (path, volinfo, priv);

        snprintf (filename, PATH_MAX, "%s/%s-fuse.vol",
                  path, volinfo->volname);
}

static void
get_rdma_client_filepath (char *filename, glusterd_volinfo_t *volinfo)
{
        char  path[PATH_MAX] = {0,};
        glusterd_conf_t *priv = NULL;

        priv = THIS->private;

        GLUSTERD_GET_VOLUME_DIR (path, volinfo, priv);

        snprintf (filename, PATH_MAX, "%s/%s-rdma-fuse.vol",
                  path, volinfo->volname);
}

static int
generate_client_volfile (glusterd_volinfo_t *volinfo)
{
        volgen_graph_t graph = {0,};
        char    filename[PATH_MAX] = {0,};
        int     ret = -1;
        dict_t *dict = NULL;

        get_client_filepath (filename, volinfo);

        if (volinfo->transport_type == GF_TRANSPORT_BOTH_TCP_RDMA) {
                dict = dict_new ();
                if (!dict)
                        goto out;
                ret = dict_set_str (dict, "client-transport-type", "tcp");
                if (ret)
                        goto out;
        }

        ret = build_client_graph (&graph, volinfo, dict);
        if (!ret)
                ret = volgen_write_volfile (&graph, filename);

        volgen_graph_free (&graph);

        if (dict) {
                /* This means, transport type is both RDMA and TCP */

                memset (&graph, 0, sizeof (graph));
                get_rdma_client_filepath (filename, volinfo);

                ret = dict_set_str (dict, "client-transport-type", "rdma");
                if (ret)
                        goto out;

                ret = build_client_graph (&graph, volinfo, dict);
                if (!ret)
                        ret = volgen_write_volfile (&graph, filename);

                volgen_graph_free (&graph);

                dict_unref (dict);
        }

out:
        return ret;
}

int
glusterd_create_rb_volfiles (glusterd_volinfo_t *volinfo,
                             glusterd_brickinfo_t *brickinfo)
{
        int ret = -1;

        ret = glusterd_generate_brick_volfile (volinfo, brickinfo);
        if (!ret)
                ret = generate_client_volfile (volinfo);
        if (!ret)
                ret = glusterd_fetchspec_notify (THIS);

        return ret;
}

int
glusterd_create_volfiles_and_notify_services (glusterd_volinfo_t *volinfo)
{
        int ret = -1;

        ret = generate_brick_volfiles (volinfo);
        if (ret) {
                gf_log ("", GF_LOG_ERROR,
                        "Could not generate volfiles for bricks");
                goto out;
        }

        ret = generate_client_volfile (volinfo);
        if (ret) {
                gf_log ("", GF_LOG_ERROR,
                        "Could not generate volfile for client");
                goto out;
        }

        ret = glusterd_fetchspec_notify (THIS);

out:
        return ret;
}

void
glusterd_get_nfs_filepath (char *filename)
{
        char  path[PATH_MAX] = {0,};
        glusterd_conf_t *priv  = NULL;

        priv = THIS->private;

        GLUSTERD_GET_NFS_DIR (path, priv);

        snprintf (filename, PATH_MAX, "%s/nfs-server.vol", path);
}

int
glusterd_create_nfs_volfile ()
{
        volgen_graph_t graph = {0,};
        char    filename[PATH_MAX] = {0,};
        int     ret = -1;

        glusterd_get_nfs_filepath (filename);

        ret = build_nfs_graph (&graph, NULL);
        if (!ret)
                ret = volgen_write_volfile (&graph, filename);

        volgen_graph_free (&graph);

        return ret;
}

int
glusterd_delete_volfile (glusterd_volinfo_t *volinfo,
                         glusterd_brickinfo_t *brickinfo)
{
        int  ret = 0;
        char filename[PATH_MAX] = {0,};

        GF_ASSERT (volinfo);
        GF_ASSERT (brickinfo);

        get_brick_filepath (filename, volinfo, brickinfo);
        ret = unlink (filename);
        if (ret)
                gf_log ("glusterd", GF_LOG_ERROR, "failed to delete file: %s, "
                        "reason: %s", filename, strerror (errno));
        return ret;
}

int
validate_nfsopts (glusterd_volinfo_t *volinfo,
                    dict_t *val_dict,
                    char **op_errstr)
{
        volgen_graph_t graph = {0,};
        int     ret = -1;

        graph.errstr = op_errstr;

        ret = build_nfs_graph (&graph, val_dict);
        if (!ret)
                ret = graph_reconf_validateopt (&graph.graph, op_errstr);

        volgen_graph_free (&graph);

        gf_log ("glusterd", GF_LOG_DEBUG, "Returning %d", ret);
        return ret;
}

int
validate_clientopts (glusterd_volinfo_t *volinfo,
                    dict_t *val_dict,
                    char **op_errstr)
{
        volgen_graph_t graph = {0,};
        int     ret = -1;

        GF_ASSERT (volinfo);

        graph.errstr = op_errstr;

        ret = build_client_graph (&graph, volinfo, val_dict);
        if (!ret)
                ret = graph_reconf_validateopt (&graph.graph, op_errstr);

        volgen_graph_free (&graph);

        gf_log ("", GF_LOG_DEBUG, "Returning %d", ret);
        return ret;
}

int
validate_brickopts (glusterd_volinfo_t *volinfo,
                    char *brickinfo_path,
                    dict_t *val_dict,
                    char **op_errstr)
{
        volgen_graph_t graph = {0,};
        int     ret = -1;

        GF_ASSERT (volinfo);

        graph.errstr = op_errstr;

        ret = build_server_graph (&graph, volinfo, val_dict, brickinfo_path);
        if (!ret)
                ret = graph_reconf_validateopt (&graph.graph, op_errstr);

        volgen_graph_free (&graph);

        gf_log ("", GF_LOG_DEBUG, "Returning %d", ret);
        return ret;
}

int
glusterd_validate_brickreconf (glusterd_volinfo_t *volinfo,
                               dict_t *val_dict,
                               char **op_errstr)
{
        glusterd_brickinfo_t *brickinfo = NULL;
        int                   ret = -1;

        list_for_each_entry (brickinfo, &volinfo->bricks, brick_list) {
                gf_log ("", GF_LOG_DEBUG,
                        "Validating %s", brickinfo->hostname);

                ret = validate_brickopts (volinfo, brickinfo->path, val_dict,
                                          op_errstr);
                if (ret)
                        goto out;
        }

        ret = 0;
out:

        return ret;
}

static void
_check_globalopt (dict_t *this, char *key, data_t *value, void *ret_val)
{
        int *ret = NULL;

        ret = ret_val;
        if (*ret)
                return;
        if (!glusterd_check_globaloption (key))
                *ret = 1;
}

int
glusterd_validate_globalopts (glusterd_volinfo_t *volinfo,
                              dict_t *val_dict, char **op_errstr)
{
        int ret = 0;

        dict_foreach (val_dict, _check_globalopt, &ret);
        if (ret) {
                *op_errstr = gf_strdup ( "option specified is not a global option");
                return -1;
        }
        ret = glusterd_validate_brickreconf (volinfo, val_dict, op_errstr);

        if (ret) {
                gf_log ("", GF_LOG_DEBUG,
                        "Could not Validate  bricks");
                goto out;
        }

        ret = validate_clientopts (volinfo, val_dict, op_errstr);
        if (ret) {
                gf_log ("", GF_LOG_DEBUG,
                        "Could not Validate client");
                goto out;
        }

        ret = validate_nfsopts (volinfo, val_dict, op_errstr);


out:
                gf_log ("", GF_LOG_DEBUG, "Returning %d", ret);
                return ret;
}

static void
_check_localopt (dict_t *this, char *key, data_t *value, void *ret_val)
{
        int *ret = NULL;

        ret = ret_val;
        if (*ret)
                return;
        if (!glusterd_check_localoption (key))
                *ret = 1;
}

int
glusterd_validate_reconfopts (glusterd_volinfo_t *volinfo, dict_t *val_dict,
                              char **op_errstr)
{
        int ret = 0;

        dict_foreach (val_dict, _check_localopt, &ret);
        if (ret) {
                *op_errstr = gf_strdup ( "option specified is not a local option");
                return -1;
        }
        ret = glusterd_validate_brickreconf (volinfo, val_dict, op_errstr);

        if (ret) {
                gf_log ("", GF_LOG_DEBUG,
                        "Could not Validate  bricks");
                goto out;
        }

        ret = validate_clientopts (volinfo, val_dict, op_errstr);
        if (ret) {
                gf_log ("", GF_LOG_DEBUG,
                        "Could not Validate client");
                goto out;
        }

        ret = validate_nfsopts (volinfo, val_dict, op_errstr);


out:
                gf_log ("", GF_LOG_DEBUG, "Returning %d", ret);
                return ret;
}
