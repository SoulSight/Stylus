using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Trinity;

using Stylus.DataModel;
using Stylus.Storage;
using Stylus.Util;

namespace Stylus.Query
{
    public abstract partial class BaseQueryWorker
    {
        public List<long> GetPids(long eid) 
        {
            ushort tid = TidUtil.GetTid(eid);
            if (tid != StylusConfig.GenericTid)
            {
                return StylusSchema.Tid2Pids[tid];
            }
            else
            {
                using (var cell = Global.LocalStorage.UseGenericPropEntity(eid))
                {
                    return ((List<Property>)cell.Props).Select(prop => prop.Name).ToList();
                }
            }
        }

        public List<string> GetPreds(long eid)
        {
            return GetPids(eid).Select(pid => this.pid2pred[pid]).ToList();
        }

        public List<string> GetPreds(string entity) 
        {
            long eid = LiteralToId[entity];
            return GetPreds(eid);
        }

        public List<long> GetOids(long eid, long pid) 
        {
            return Storage.SelectObjectToList(eid, pid);
        }

        public List<string> GetObjs(long eid, long pid) 
        {
            return GetOids(eid, pid).Select(oid => GetLiteral(oid)).ToList();
        }

        public List<string> GetObjs(string subj, string pred) 
        {
            long eid = LiteralToId[subj];
            long pid = StylusSchema.Pred2Pid[pred];
            return GetObjs(eid, pid);
        }

        public List<long> GetSids(long pid, long oid) 
        {
            long inv_pid = StylusSchema.InvPreds[pid];
            return GetOids(oid, inv_pid);
        }

        public List<string> GetSubjs(string pred, string obj) 
        {
            long oid = LiteralToId[obj];
            long pid = StylusSchema.Pred2Pid[pred];
            return GetSids(pid, oid).Select(sid => GetLiteral(sid)).ToList();
        }

        public List<long> GetAllOids(long eid) 
        {
            var pids = this.GetPids(eid);
            return Storage.SelectObjectsToList(eid, pids).SelectMany(l => l).ToList();
        }

        public IEnumerable<long> GetSids(IEnumerable<long> pids) 
        {
            var tids = Storage.GetUDTs(pids);
            foreach (var tid in tids)
            {
                var list = Storage.TidInstances[tid];
                foreach (var item in list)
                {
                    yield return item;
                }
            }
        }

        public IEnumerable<string> GetSubjs(IEnumerable<long> pids)
        {
            var sids = GetSids(pids);
            foreach (var sid in sids)
            {
                yield return GetLiteral(sid);
            }
        }

        public IEnumerable<long> GetSids(IEnumerable<string> preds)
        {
            var pids = preds.Select(pred => StylusSchema.Pred2Pid[pred]);
            return GetSids(pids);
        }

        public IEnumerable<string> GetSubjs(IEnumerable<string> preds) 
        {
            var sids = GetSids(preds);
            foreach (var sid in sids)
            {
                yield return GetLiteral(sid);
            }
        }
    }
}
