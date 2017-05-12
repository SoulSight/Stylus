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
    public class GraphApiWorker
    {
        public IStorage Storage
        {
            set;
            get;
        }

        public Statistics CardStatistics { set; get; }

        public Dictionary<string, long> LiteralToId
        {
            set;
            get;
        }

        private Dictionary<uint, string[]> IdToLiteral;

        private Dictionary<long, string> pid2pred = new Dictionary<long, string>();

        public GraphApiWorker() 
        {
            if (TrinityConfig.CurrentRunningMode == RunningMode.Embedded)
            {
                // Initialize: LiteralToEid & Statistics
                LoadLiteralMapping();
            }

            Storage = RAMStorage.Singleton;
            CardStatistics = RAMStorage.CardStatistics;

            InitPid2Pred();
        }

        private void AddLiteralMapEntry(string literal, long eid)
        {
            this.LiteralToId.Add(literal, eid);
            ushort tid = TidUtil.GetTid(eid);
            int index = (int)TidUtil.CloneMaskTid(eid) - 1;
            this.IdToLiteral[tid][index] = literal;
        }

        private void InitPid2Pred()
        {
            foreach (var item in StylusSchema.Pred2Pid)
            {
                this.pid2pred.Add(item.Value, item.Key);
            }
        }

        private string GetLiteral(long eid)
        {
            ushort tid = TidUtil.GetTid(eid);
            int index = (int)TidUtil.CloneMaskTid(eid) - 1;
            return this.IdToLiteral[tid][index];
        }

        private void LoadLiteralMapping() 
        {
            // LoadLiteralToEid
            this.LiteralToId = new Dictionary<string, long>();
            this.IdToLiteral = new Dictionary<uint, string[]>();
            foreach (var tid2count in StylusSchema.Tid2Count)
            {
                IdToLiteral.Add(tid2count.Key, new string[(int)tid2count.Value]);
            }

            // IOUtil.LoadEidMapFile((literal, eid) => this.LiteralToId.Add(literal, eid));
            IOUtil.LoadEidMapFile((literal, eid) => AddLiteralMapEntry(literal, eid));
        }

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
    }
}
