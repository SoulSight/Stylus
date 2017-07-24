using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Trinity;
using Trinity.Diagnostics;
using Trinity.TSL.Lib;

using Stylus.DataModel;
using Stylus.Util;
using Stylus.Query;
using System.IO;

namespace Stylus.Storage
{
    public class RAMStorage : IStorage
    {
        Dictionary<ushort, List<long>> tid_instances = new Dictionary<ushort, List<long>>();

        // For Synthetic Predicates: t=[..., p_o1, p_o2, ..., p_on, ...] => [p] => [o1, o2, ..., on]
        Dictionary<ushort, Dictionary<long, List<long>>> synpred_tid2pid2oids = new Dictionary<ushort, Dictionary<long, List<long>>>();

        private RAMStorage() 
        {
            Log.WriteLine(LogLevel.Info, "Initializing Schema...");
            /// Initialize the Ps & xUDTs
            InitializeSchema();

            Log.WriteLine(LogLevel.Info, "Initializing Indices...");
            /// Initialize the xUDT indices
            InitializeIndices();

            Log.WriteLine(LogLevel.Info, "Initialize SyntheticPredIndex...");
            InitializeSyntheticPredIndex();

            Log.WriteLine(LogLevel.Info, "SingleThreadServer Ready.");
        }

        private static RAMStorage storage = new RAMStorage();
        private static Statistics cardStatistics = null;

        public static RAMStorage Singleton
        {
            get 
            {
                return storage;
            }
        }

        public static Statistics CardStatistics
        {
            set 
            {
                cardStatistics = value;
            }
            get 
            {
                if (cardStatistics == null)
                {
                    if (TrinityConfig.CurrentRunningMode == RunningMode.Embedded
                        && File.Exists(Statistics.GetPersistFilename()))
                    {
                        cardStatistics = new Statistics(Statistics.GetPersistFilename());
                    }
                    else if (TrinityConfig.CurrentRunningMode == RunningMode.Server
                        && File.Exists(Statistics.GetClusterPersistFilename()))
                    {
                        cardStatistics = new Statistics(Statistics.GetClusterPersistFilename());
                    }
                    else
                    {
                        cardStatistics = new Statistics(RAMStorage.Singleton);
                    }

                    if (TrinityConfig.CurrentRunningMode == RunningMode.Embedded)
                    {
                        cardStatistics.SaveToFile();
                    }
                }
                return cardStatistics;
            }
        }

        public void ReloadIndices() 
        {
            Log.WriteLine(LogLevel.Info, "Reloading...");
            /// Initialize the xUDT indices
            InitializeIndices();
            Log.WriteLine(LogLevel.Info, "Loaded.");
        }

        private void InitializeIndices() 
        {
            tid_instances.Clear();

            foreach (var accessor in Global.LocalStorage.xEntity_Accessor_Selector())
            {
                long id = accessor.CellID.Value;
                ushort tid = accessor.Tid;
                if (!tid_instances.ContainsKey(tid))
                {
                    tid_instances.Add(tid, new List<long>());
                }
                tid_instances[tid].Add(id);
            }

            /// Initialize the generic indices
            foreach (var accessor in Global.LocalStorage.GenericPOEntity_Accessor_Selector())
            {
                long id = accessor.CellID.Value;
                ushort tid = accessor.Tid;
                if (!tid_instances.ContainsKey(tid))
                {
                    tid_instances.Add(tid, new List<long>());
                }
                tid_instances[tid].Add(id);
            }

            /// Initialize the generic indices
            foreach (var accessor in Global.LocalStorage.GenericPropEntity_Accessor_Selector())
            {
                long id = accessor.CellID.Value;
                ushort tid = accessor.Tid;
                if (!tid_instances.ContainsKey(tid))
                {
                    tid_instances.Add(tid, new List<long>());
                }
                tid_instances[tid].Add(id);
            }

            /// Update Schema Info
            foreach (var kvp in tid_instances)
            {
                StylusSchema.Tid2Count[kvp.Key] = (double)kvp.Value.Count;
            }
        }

        private void InitializeSchema() 
        {
            if (Global.LocalStorage.Contains(StylusConfig.SchemaCell))
            {
                StylusSchema.LoadFromStorage();
            }
            else
            {
                StylusSchema.LoadFromFile();
            }
        }

        private void InitializeSyntheticPredIndex() 
        {
            foreach (var kvp in StylusSchema.Synpid2PidOid)
            {
                long synpid = kvp.Key;
                long pid = kvp.Value.Item1;
                long oid = kvp.Value.Item2;

                var tids = StylusSchema.GetUDTsForPid(synpid);

                foreach (var tid in tids)
                {
                    if (!synpred_tid2pid2oids.ContainsKey(tid))
                    {
                        synpred_tid2pid2oids.Add(tid, new Dictionary<long,List<long>>());
                    }
                    if (!synpred_tid2pid2oids[tid].ContainsKey(pid))
                    {
                        synpred_tid2pid2oids[tid].Add(pid, new List<long>());
                    }
                    synpred_tid2pid2oids[tid][pid].Add(oid);
                }
            }
        }

        public Dictionary<ushort, List<long>> TidInstances
        {
            get
            {
                return tid_instances;
            }
            set
            {
                this.tid_instances = value;
            }
        }

        public HashSet<ushort> GetUDTs(IEnumerable<long> pids)
        {
            return StylusSchema.GetUDTs(pids);
        }

        public List<long> LoadEids(ushort tid)
        {
            List<long> instances = new List<long>();
            tid_instances.TryGetValue(tid, out instances);
            return instances;
        }

        #region Select objects
        public long[] SelectOffset(long eid, int offsetIndex) 
        {
            using (var cell = Global.LocalStorage.UsexEntity(eid, CellAccessOptions.ReturnNullOnCellNotFound))
            {
                if (cell == null)
                {
                    return new long[0];
                }
                int start = offsetIndex == 0 ? 0 : cell.Offsets[offsetIndex - 1];
                int end = cell.Offsets[offsetIndex];
                int count = end - start;
                long[] results = new long[count];
                //cell.ObjVals.CopyTo(start, results, 0, count);
                for (int i = 0; i < count; i++)
                {
                    results[i] = cell.ObjVals[i + start];
                }
                return results;
            }
        }

        public List<long> SelectOffsetToList(long eid, int offsetIndex)
        {
            using (var cell = Global.LocalStorage.UsexEntity(eid, CellAccessOptions.ReturnNullOnCellNotFound))
            {
                if (cell == null)
                {
                    return new List<long>();
                }
                int start = offsetIndex == 0 ? 0 : cell.Offsets[offsetIndex - 1];
                int end = cell.Offsets[offsetIndex];
                int count = end - start;
                var results = new List<long>(count);
                for (int i = 0; i < count; i++)
                {
                    results.Add(cell.ObjVals[i + start]);
                }
                return results;
            }
        }

        public List<long[]> SelectOffsets(long eid, List<int> offsetIndexes)
        {
            var leaves = new List<long[]>();
            using (var cell = Global.LocalStorage.UsexEntity(eid, CellAccessOptions.ReturnNullOnCellNotFound))
            {
                if (cell == null)
                {
                    return null;
                }
                foreach (var offsetIndex in offsetIndexes)
                {
                    int start = offsetIndex == 0 ? 0 : cell.Offsets[offsetIndex - 1];
                    int end = cell.Offsets[offsetIndex];
                    int count = end - start;
                    long[] results = new long[count];
                    //cell.ObjVals.CopyTo(start, results, 0, count);
                    for (int i = 0; i < count; i++)
                    {
                        results[i] = cell.ObjVals[i + start];
                    }
                    leaves.Add(results);
                }
            }
            return leaves;
        }

        public List<List<long>> SelectOffsetsToList(long eid, List<int> offsetIndexes)
        {
            var leaves = new List<List<long>>();
            using (var cell = Global.LocalStorage.UsexEntity(eid, CellAccessOptions.ReturnNullOnCellNotFound))
            {
                if (cell == null)
                {
                    return null;
                }
                foreach (var offsetIndex in offsetIndexes)
                {
                    int start = offsetIndex == 0 ? 0 : cell.Offsets[offsetIndex - 1];
                    int end = cell.Offsets[offsetIndex];
                    int count = end - start;
                    var results = new List<long>(count);
                    //cell.ObjVals.CopyTo(start, results, 0, count);
                    for (int i = 0; i < count; i++)
                    {
                        results.Add(cell.ObjVals[i + start]);
                    }
                    leaves.Add(results);
                }
            }
            return leaves;
        }

        public long[] SelectObjectFromSynpred(long eid, long pid)
        {
            ushort tid = TidUtil.GetTid(eid);
            return this.synpred_tid2pid2oids[tid][pid].ToArray();
        }

        public List<long> SelectObjectFromSynpredToList(long eid, long pid)
        {
            ushort tid = TidUtil.GetTid(eid);
            return this.synpred_tid2pid2oids[tid][pid];
        }

        private long[] SelectGenericObject(long eid, long pid, Binding binding = null) 
        {
            using (var cell = Global.LocalStorage.UseGenericPropEntity(eid, CellAccessOptions.ReturnNullOnCellNotFound))
            {
                if (cell == null)
                {
                    return new long[0];
                }
                foreach (var prop in cell.Props)
                {
                    if (prop.Name != pid)
                    {
                        continue;
                    }

                    if (binding == null)
                    {
                        return prop.Values.ToArray();
                    }

                    List<long> vals = new List<long>();
                    for (int i = 0; i < prop.Values.Count; i++)
                    {
                        if (binding.ContainEid(prop.Values[i]))
                        {
                            vals.Add(prop.Values[i]);
                        }
                    }
                    return vals.ToArray();
                }
                return new long[0];
            }
        }
        
        private List<long> SelectGenericObjectToList(long eid, long pid, Binding binding = null)
        {
            using (var cell = Global.LocalStorage.UseGenericPropEntity(eid, CellAccessOptions.ReturnNullOnCellNotFound))
            {
                if (cell == null)
                {
                    return new List<long>();
                }
                foreach (var prop in cell.Props)
                {
                    if (prop.Name != pid)
                    {
                        continue;
                    }

                    if (binding == null)
                    {
                        return (List<long>)prop.Values;
                    }

                    List<long> vals = new List<long>();
                    for (int i = 0; i < prop.Values.Count; i++)
                    {
                        if (binding.ContainEid(prop.Values[i]))
                        {
                            vals.Add(prop.Values[i]);
                        }
                    }
                    return vals;
                }
                return new List<long>();
            }
        }

        private List<long[]> SelectGenericObjects(long eid, List<long> pids, List<Binding> bindings = null)
        {
            Dictionary<long, int> pid2indexes = new Dictionary<long, int>();
            List<long[]> results = new List<long[]>();
            List<long[]> null_results = new List<long[]>();
            for (int i = 0; i < pids.Count; i++)
            {
                pid2indexes.Add(pids[i], i);
                results.Add(new long[0]);
                null_results.Add(new long[0]);
            }

            using (var cell = Global.LocalStorage.UseGenericPropEntity(eid, CellAccessOptions.ReturnNullOnCellNotFound))
            {
                if (cell == null)
                {
                    return results;
                }
                foreach (var prop in cell.Props)
                {
                    if (pid2indexes.ContainsKey(prop.Name))
                    {
                        int index = pid2indexes[prop.Name];
                        if (bindings == null || bindings[index] == null)
                        {
                            results[index] = prop.Values.ToArray();
                        }
                        else
                        {
                            var vals = new List<long>();
                            for (int i = 0; i < prop.Values.Count; i++)
                            {
                                if (bindings[index].ContainEid(prop.Values[i]))
                                {
                                    vals.Add(prop.Values[i]);
                                }
                            }
                            if (vals.Count == 0)
                            {
                                return null_results;
                            }
                            results[index] = vals.ToArray();
                        }
                    }
                }
                return results;
            }
        }

        private List<List<long>> SelectGenericObjectsToList(long eid, List<long> pids, List<Binding> bindings = null)
        {
            Dictionary<long, int> pid2indexes = new Dictionary<long, int>();
            List<List<long>> results = new List<List<long>>();
            List<List<long>> null_results = new List<List<long>>();
            for (int i = 0; i < pids.Count; i++)
            {
                pid2indexes.Add(pids[i], i);
                results.Add(new List<long>());
                null_results.Add(new List<long>());
            }

            using (var cell = Global.LocalStorage.UseGenericPropEntity(eid, CellAccessOptions.ReturnNullOnCellNotFound))
            {
                if (cell == null)
                {
                    return results;
                }
                foreach (var prop in cell.Props)
                {
                    if (pid2indexes.ContainsKey(prop.Name))
                    {
                        int index = pid2indexes[prop.Name];
                        if (bindings == null || bindings[index] == null)
                        {
                            results[index] = (List<long>)prop.Values;
                        }
                        else
                        {
                            var vals = new List<long>();
                            for (int i = 0; i < prop.Values.Count; i++)
                            {
                                if (bindings[index].ContainEid(prop.Values[i]))
                                {
                                    vals.Add(prop.Values[i]);
                                }
                            }
                            if (vals.Count == 0)
                            {
                                return null_results;
                            }
                            results[index] = vals;
                        }
                    }
                }
                return results;
            }
        }

        public long[] SelectObject(long eid, long pid)
        {
            ushort tid = TidUtil.GetTid(eid);
            if (tid == StylusConfig.GenericTid)
            {
                return SelectGenericObject(eid, pid);
            }
            else
            {
                int offset = StylusSchema.TidPid2Index[tid][pid];
                return SelectOffset(eid, offset);
            }
        }

        public List<long> SelectObjectToList(long eid, long pid)
        {
            ushort tid = TidUtil.GetTid(eid);
            if (tid == StylusConfig.GenericTid)
            {
                return SelectGenericObjectToList(eid, pid);
            }
            else
            {
                int offset = StylusSchema.TidPid2Index[tid][pid];
                return SelectOffsetToList(eid, offset);
            }
        }

        public List<long[]> SelectObjects(long eid, List<long> pids)
        {
            ushort tid = TidUtil.GetTid(eid);
            if (tid == StylusConfig.GenericTid)
            {
                return SelectGenericObjects(eid, pids);
            }
            else
            {
                List<int> offsets = pids.Select(P => StylusSchema.TidPid2Index[tid][P]).ToList();
                return SelectOffsets(eid, offsets);
            }
        }

        public List<List<long>> SelectObjectsToList(long eid, List<long> pids)
        {
            ushort tid = TidUtil.GetTid(eid);
            if (tid == StylusConfig.GenericTid)
            {
                return SelectGenericObjectsToList(eid, pids);
            }
            else
            {
                List<int> offsets = pids.Select(P => StylusSchema.TidPid2Index[tid][P]).ToList();
                return SelectOffsetsToList(eid, offsets);
            }
        }
        #endregion

        #region Select objects with bindings
        public long[] SelectOffset(long eid, int offsetIndex, Binding binding)
        {
            List<long> results = new List<long>();
            using (var cell = Global.LocalStorage.UsexEntity(eid, CellAccessOptions.ReturnNullOnCellNotFound))
            {
                if (cell == null)
                {
                    return new long[0];
                }
                int start = offsetIndex == 0 ? 0 : cell.Offsets[offsetIndex - 1];
                int end = cell.Offsets[offsetIndex];
                int count = end - start;
                for (int i = start; i < end; i++)
                {
                    long c_eid = cell.ObjVals[i];
                    if (binding.ContainEid(c_eid))
                    {
                        results.Add(c_eid);
                    }
                }
            }
            return results.ToArray();
        }

        public List<long> SelectOffsetToList(long eid, int offsetIndex, Binding binding)
        {
            List<long> results = new List<long>();
            using (var cell = Global.LocalStorage.UsexEntity(eid, CellAccessOptions.ReturnNullOnCellNotFound))
            {
                if (cell == null)
                {
                    return results;
                }
                int start = offsetIndex == 0 ? 0 : cell.Offsets[offsetIndex - 1];
                int end = cell.Offsets[offsetIndex];
                int count = end - start;
                for (int i = start; i < end; i++)
                {
                    long c_eid = cell.ObjVals[i];
                    if (binding.ContainEid(c_eid))
                    {
                        results.Add(c_eid);
                    }
                }
            }
            return results;
        }

        public List<long[]> SelectOffsets(long eid, List<int> offsetIndexes, List<Binding> bindings)
        {
            var leaves = new List<long[]>();
            using (var cell = Global.LocalStorage.UsexEntity(eid, CellAccessOptions.ReturnNullOnCellNotFound))
            {
                if (cell == null)
                {
                    return null;
                }
                for (int pos = 0; pos < offsetIndexes.Count; pos++)
                {
                    var offsetIndex = offsetIndexes[pos];
                    int start = offsetIndex == 0 ? 0 : cell.Offsets[offsetIndex - 1];
                    int end = cell.Offsets[offsetIndex];
                    int count = end - start;
                    List<long> results = new List<long>();
                    for (int i = start; i < end; i++)
                    {
                        long c_eid = cell.ObjVals[i];
                        if (bindings[pos].ContainEid(c_eid))
                        {
                            results.Add(c_eid);
                        }
                    }
                    leaves.Add(results.ToArray());
                }
            }
            return leaves;
        }

        public List<List<long>> SelectOffsetsToList(long eid, List<int> offsetIndexes, List<Binding> bindings)
        {
            var leaves = new List<List<long>>();
            using (var cell = Global.LocalStorage.UsexEntity(eid, CellAccessOptions.ReturnNullOnCellNotFound))
            {
                if (cell == null)
                {
                    return null;
                }
                for (int pos = 0; pos < offsetIndexes.Count; pos++)
                {
                    var offsetIndex = offsetIndexes[pos];
                    int start = offsetIndex == 0 ? 0 : cell.Offsets[offsetIndex - 1];
                    int end = cell.Offsets[offsetIndex];
                    int count = end - start;
                    List<long> results = new List<long>();
                    for (int i = start; i < end; i++)
                    {
                        long c_eid = cell.ObjVals[i];
                        if (bindings[pos].ContainEid(c_eid))
                        {
                            results.Add(c_eid);
                        }
                    }
                    leaves.Add(results);
                }
            }
            return leaves;
        }

        public long[] SelectObjectFromSynpred(long eid, long pid, Binding binding)
        {
            ushort tid = TidUtil.GetTid(eid);
            List<long> results = new List<long>();
            foreach (var c_eid in this.synpred_tid2pid2oids[tid][pid])
            {
                if (binding.ContainEid(c_eid))
                {
                    results.Add(c_eid);
                }
            }
            return results.ToArray();
        }

        public List<long> SelectObjectFromSynpredToList(long eid, long pid, Binding binding)
        {
            ushort tid = TidUtil.GetTid(eid);
            List<long> results = new List<long>();
            foreach (var c_eid in this.synpred_tid2pid2oids[tid][pid])
            {
                if (binding.ContainEid(c_eid))
                {
                    results.Add(c_eid);
                }
            }
            return results;
        }

        public long[] SelectObject(long eid, long pid, Binding binding)
        {
            ushort tid = TidUtil.GetTid(eid);
            if (tid == StylusConfig.GenericTid)
            {
                return SelectGenericObject(eid, pid, binding);
            }
            else
            {
                int offset = StylusSchema.TidPid2Index[tid][pid];
                return SelectOffset(eid, offset, binding);
            }
        }

        public List<long> SelectObjectToList(long eid, long pid, Binding binding)
        {
            ushort tid = TidUtil.GetTid(eid);
            if (tid == StylusConfig.GenericTid)
            {
                return SelectGenericObjectToList(eid, pid, binding);
            }
            else
            {
                int offset = StylusSchema.TidPid2Index[tid][pid];
                return SelectOffsetToList(eid, offset, binding);
            }
        }

        public List<long[]> SelectObjects(long eid, List<long> pids, List<Binding> bindings)
        {
            ushort tid = TidUtil.GetTid(eid);
            if (tid == StylusConfig.GenericTid)
            {
                return SelectGenericObjects(eid, pids, bindings);
            }
            else
            {
                List<int> offsets = pids.Select(P => StylusSchema.TidPid2Index[tid][P]).ToList();
                return SelectOffsets(eid, offsets, bindings);
            }
        }

        public List<List<long>> SelectObjectsToList(long eid, List<long> pids, List<Binding> bindings)
        {
            ushort tid = TidUtil.GetTid(eid);
            if (tid == StylusConfig.GenericTid)
            {
                return SelectGenericObjectsToList(eid, pids, bindings);
            }
            else
            {
                List<int> offsets = pids.Select(P => StylusSchema.TidPid2Index[tid][P]).ToList();
                return SelectOffsetsToList(eid, offsets, bindings);
            }
        }
        #endregion
    }
}
