using Stylus.DataModel;
using Stylus.Storage;
using Stylus.Util;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Trinity;

namespace Stylus.Query
{
    public abstract class BaseQueryServer : IQueryServer
    {
        public IStorage Storage
        {
            set;
            get;
        }

        public Statistics CardStatistics { set; get; }

        public XDictionary<string, long> LiteralToId
        {
            set;
            get;
        }

        protected Dictionary<uint, string[]> IdToLiteral;

        protected Dictionary<long, string> pid2pred = new Dictionary<long, string>();

        public BaseQueryServer()
        {
            if (TrinityConfig.CurrentRunningMode == RunningMode.Embedded)
            {
                // Initialize: LiteralToEid & Statistics
                LoadLiteralMapping();
            }

            this.Storage = RAMStorage.Singleton;
            CardStatistics = RAMStorage.CardStatistics;
            InitPid2Pred();
        }

        protected void AddLiteralMapEntry(string literal, long eid)
        {
            this.LiteralToId.Add(literal, eid);
            ushort tid = TidUtil.GetTid(eid);
            int index = (int)TidUtil.CloneMaskTid(eid) - 1;
            this.IdToLiteral[tid][index] = literal;
        }

        protected void InitPid2Pred()
        {
            foreach (var item in StylusSchema.Pred2Pid)
            {
                this.pid2pred.Add(item.Value, item.Key);
            }
        }

        protected string GetLiteral(long eid)
        {
            ushort tid = TidUtil.GetTid(eid);
            int index = (int)TidUtil.CloneMaskTid(eid) - 1;
            return this.IdToLiteral[tid][index];
        }

        protected void LoadLiteralMapping()
        {
            // LoadLiteralToEid
            this.LiteralToId = new XDictionary<string, long>(7);
            this.IdToLiteral = new Dictionary<uint, string[]>();
            foreach (var tid2count in StylusSchema.Tid2Count)
            {
                IdToLiteral.Add(tid2count.Key, new string[(int)tid2count.Value]);
            }

            // IOUtil.LoadEidMapFile((literal, eid) => this.LiteralToId.Add(literal, eid));
            IOUtil.LoadEidMapFile((literal, eid) => AddLiteralMapEntry(literal, eid));
        }

        #region Triple Server
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
            var forward_pids = pids.Where(pid => StylusSchema.ForwardPids.Contains(pid)).ToList();
            return Storage.SelectObjectsToList(eid, forward_pids).SelectMany(l => l).ToList();
        }

        public List<long> GetAllRevOids(long eid)
        {
            var pids = this.GetPids(eid);
            var forward_pids = pids.Where(pid => !StylusSchema.ForwardPids.Contains(pid)).ToList();
            return Storage.SelectObjectsToList(eid, forward_pids).SelectMany(l => l).ToList();
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
        #endregion

        #region Binding Operations
        public abstract bool ContainsBinding(string variable);

        public abstract IEnumerable<long> EnumerateBinding(string variable);

        public abstract Binding GetBinding(string variable);

        public abstract void SetBinding(string variable, Binding binding);

        public abstract void ReplaceBinding(string variable, IEnumerable<long> bindings);

        public abstract void ReplaceBinding(string variable, Binding bindings);

        public abstract void AppendBinding(string variable, IEnumerable<long> bindings);
        #endregion

        public IEnumerable<List<string>> ResolveQuerySolutions(QuerySolutions querySolutions)
        {
            foreach (var record in querySolutions.Records)
            {
                yield return record.Select(eid => this.GetLiteral(eid)).ToList();
            }
        }
    }
}
