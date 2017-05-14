using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

using Trinity;
using Trinity.TSL.Lib;
using Trinity.Diagnostics;

using Stylus.DataModel;
using Stylus.Util;

namespace Stylus
{
    public class StylusSchema
    {
        public static Dictionary<ushort, List<long>> Tid2Pids = new Dictionary<ushort, List<long>>();
        public static Dictionary<string, long> Pred2Pid = new Dictionary<string, long>();
        public static Dictionary<ushort, double> Tid2Count = new Dictionary<ushort, double>();

        public static Dictionary<ushort, Dictionary<long, int>> TidPid2Index = new Dictionary<ushort, Dictionary<long, int>>();
        public static Dictionary<long, Dictionary<ushort, int>> PidTid2Index = new Dictionary<long, Dictionary<ushort, int>>();
        public static HashSet<long> GenericInclusivePids = new HashSet<long>();

        public static Dictionary<long, long> InvPreds = new Dictionary<long, long>();

        public static List<Tuple<long, long, long>> SynpidPidOids = new List<Tuple<long, long, long>>();

        public static Dictionary<long, Tuple<long, long>> Synpid2PidOid = new Dictionary<long, Tuple<long, long>>();
        public static Dictionary<Tuple<long, long>, long> PidOid2Synpid = new Dictionary<Tuple<long, long>, long>();
        //[rdf:type] => [rdf:type Person], [rdf:type Film]
        public static Dictionary<long, HashSet<long>> Pid2Synpids = new Dictionary<long, HashSet<long>>();

        public static HashSet<string> PredCandidatesForSynPred = new HashSet<string>() { Vocab.RdfType };
        //public static HashSet<string> PredCandidatesForSynPred = new HashSet<string>(); // no synthetic preds

        #region Load Schema
        private static void RefreshTPIndex() 
        {
            TidPid2Index.Clear();
            PidTid2Index.Clear();

            foreach (var kvp in Tid2Pids)
            {
                var tid = kvp.Key;
                var Ps = kvp.Value;

                if (tid == StylusConfig.GenericTid)
                {
                    GenericInclusivePids.AddAll(Ps);
                }
                else
                {
                    if (!TidPid2Index.ContainsKey(tid))
                    {
                        TidPid2Index.Add(tid, new Dictionary<long, int>());
                    }

                    for (int i = 0; i < Ps.Count; i++)
                    {
                        var p = Ps[i];
                        var index = i;
                        TidPid2Index[tid].Add(p, index);

                        if (!PidTid2Index.ContainsKey(p))
                        {
                            PidTid2Index.Add(p, new Dictionary<ushort, int>());
                        }
                        PidTid2Index[p].Add(tid, index);
                    }
                }
            }
        }

        private static void RefreshSynPIndex() 
        {
            Synpid2PidOid.Clear();
            PidOid2Synpid.Clear();
            Pid2Synpids.Clear();

            SynpidPidOids = SynpidPidOids.Distinct().ToList();
            foreach (var tuple in SynpidPidOids)
            {
                long synpid = tuple.Item1;
                var pid_oid = new Tuple<long, long>(tuple.Item2, tuple.Item3);
                Synpid2PidOid.Add(synpid, pid_oid);
                PidOid2Synpid.Add(pid_oid, synpid);

                if (!Pid2Synpids.ContainsKey(tuple.Item2))
                {
                    Pid2Synpids.Add(tuple.Item2, new HashSet<long>());
                }
                Pid2Synpids[tuple.Item2].Add(synpid);
            }
        }

        private static void Clear() 
        {
            Tid2Pids.Clear();
            Pred2Pid.Clear();
            Tid2Count.Clear();

            TidPid2Index.Clear();
            PidTid2Index.Clear();

            InvPreds.Clear();

            SynpidPidOids.Clear();
            Synpid2PidOid.Clear();
            PidOid2Synpid.Clear();

            Pid2Synpids.Clear();
        }

        public static void LoadFromFile() 
        {
            Clear();

            string filename = StylusConfig.GetStoreMetaRootDir() + StylusConfig.xUDTFilename;
            ushort tid_cur = StylusConfig.TidStart;
            int udt_cnt = 0;
            //Tid2Pids.Add(StoreConfig.GenericTid, new List<long>());
            HashSet<long> generic_tid_pids = new HashSet<long>();
            double generic_tid_count = 0.0;
            foreach (var line in File.ReadLines(filename))
            {
                string[] splits = line.Split('\t');
                if (splits.Length < 2)
                {
                    continue;
                }
                List<long> pids = splits[0].Split(' ').Select(str => long.Parse(str)).ToList();
                pids.Sort();
                if (udt_cnt++ <= StylusConfig.MaxXudt)
                {
                    Tid2Pids.Add(tid_cur, pids);
                    Tid2Count.Add(tid_cur, double.Parse(splits[1]));
                    tid_cur++;
                }
                else 
                {
                    //Tid2Pids[StoreConfig.GenericTid].AddRange(Ps);
                    foreach (var pid in pids)
                    {
                        generic_tid_pids.Add(pid);
                    }
                    generic_tid_count += double.Parse(splits[1]);
                }
            }
            //Tid2Pids[StoreConfig.GenericTid] = Tid2Pids[StoreConfig.GenericTid].Distinct().ToList();
            Tid2Pids.Add(StylusConfig.GenericTid, generic_tid_pids.ToList());
            Tid2Count.Add(StylusConfig.GenericTid, generic_tid_count);
            RefreshTPIndex();

            LoadPredInfo();
        }

        public static void LoadFromStorage()
        {
            Clear();

            foreach (var accessor in Global.LocalStorage.xUDTSchema_Accessor_Selector())
            {
                foreach (var xUDT in (List<xUDT>)accessor.xUDTs)
                {
                    ushort tid = xUDT.Tid;
                    List<long> Ps = (List<long>)xUDT.Ps;
                    Tid2Pids.Add(tid, Ps);
                }
                RefreshTPIndex();

                foreach (var inv_p in (List<InvertPred>)accessor.InvertPreds)
                {
                    InvPreds.Add(inv_p.P, inv_p.P_Inv);
                }

                foreach (var syn_p in (List<SyntheticPred>)accessor.SyntheticPreds)
                {
                    SynpidPidOids.Add(Tuple.Create(syn_p.SynP, syn_p.P, syn_p.O));
                }
                RefreshSynPIndex();
            }

            LoadPredInfo();
        }

        private static void LoadPredInfo()
        {
            // loading pid.map
            string pid_map_file = StylusConfig.GetStoreMetaRootDir() + StylusConfig.PidMapFilename;
            foreach (var line in File.ReadLines(pid_map_file))
            {
                string[] splits = line.Split('\t');
                long pid = long.Parse(splits[1]);
                Pred2Pid.Add(splits[0], pid);
            }

            foreach (var kvp in Pred2Pid)
            {
                string key = kvp.Key;
                string inv_key = key.StartsWith("_") ? key.Substring(1) : "_" + key;
                if (!Pred2Pid.ContainsKey(inv_key))
                {
                    Log.WriteLine(LogLevel.Debug, "inv_key not found: " + inv_key);
                    continue;
                }
                if (InvPreds.ContainsKey(kvp.Value))
                {
                    Log.WriteLine(LogLevel.Debug, "InvPreds.ContainsKey: " + kvp.Value);
                    continue;
                }
                InvPreds.Add(kvp.Value, Pred2Pid[inv_key]);
            }

            // loading syn_pred.map
            string syn_pred_map_file = StylusConfig.GetStoreMetaRootDir() + StylusConfig.SynPredMapFilename;
            foreach (var line in File.ReadLines(syn_pred_map_file))
            {
                string[] splits = line.Split('\t');
                if (splits.Length < 3)
                {
                    continue;
                }
                long[] spid_pid_oid = splits.Select(x => long.Parse(x)).ToArray();
                SynpidPidOids.Add(Tuple.Create(spid_pid_oid[0], spid_pid_oid[1], spid_pid_oid[2]));
            }
            RefreshSynPIndex();
        }

        public static void SaveToStorage() 
        {
            List<xUDT> xudts = new List<xUDT>();
            foreach (var tid2preds in Tid2Pids)
            {
                xUDT xudt = new xUDT(tid2preds.Key, tid2preds.Value);
                xudts.Add(xudt);
            }

            List<InvertPred> inv_preds = new List<InvertPred>();
            foreach (var inv_p in InvPreds)
            {
                InvertPred inv_p_struct = new InvertPred(inv_p.Key, inv_p.Value);
                inv_preds.Add(inv_p_struct);
            }

            List<SyntheticPred> synpreds = new List<SyntheticPred>();
            foreach (var tuple in SynpidPidOids)
            {
                SyntheticPred sp = new SyntheticPred(tuple.Item1, tuple.Item2, tuple.Item3);
                synpreds.Add(sp);
            }

            var schema = new xUDTSchema(StylusConfig.SchemaCell, xudts, inv_preds, synpreds);
            Global.LocalStorage.SavexUDTSchema(schema);
        }
        #endregion

        #region Utils
        public static HashSet<ushort> GetUDTs(IEnumerable<long> pids)
        {
            HashSet<ushort> sup_types = null;
            foreach (var pid in pids)
            {
                var contained_tids = GetUDTsForPid(pid);
                if (sup_types == null)
                {
                    sup_types = new HashSet<ushort>(contained_tids);
                }
                sup_types.RemoveWhere(t => !contained_tids.Contains(t));
            }
            if (pids.All(pid => StylusSchema.GenericInclusivePids.Contains(pid)))
            {
                sup_types.Add(StylusConfig.GenericTid);
            }
            return sup_types;
        }

        // Regarding Synthetic Predicates: [rdf:type] => [rdf:type_Person], [rdf:type_Film] => Tids
        public static HashSet<ushort> GetUDTsForPid(long pid)
        {
            HashSet<ushort> contained_tids = new HashSet<ushort>();
            if (!StylusSchema.Pid2Synpids.ContainsKey(pid)) // no synthetic pred for it
            {
                return new HashSet<ushort>(StylusSchema.PidTid2Index[pid].Keys);
            }
            foreach (var synpid in StylusSchema.Pid2Synpids[pid])
            {
                foreach (var tid in StylusSchema.PidTid2Index[synpid].Keys)
                {
                    contained_tids.Add(tid);
                }
            }
            return contained_tids;
        }

        public static double GetTidInstCount(ushort tid) 
        {
            if (Tid2Count.Count == 0)
            {
                throw new Exception("Tid2Count is Empty, seems not have been initialized yet.");
            }
            double val;
            if (Tid2Count.TryGetValue(tid, out val))
            {
                return val;
            }
            return 0.0;
        }
        #endregion

        #region Generate Schema
        private static long cur_p_encoding = 0;

        public static long GetOrAddPid(string pred)
        {
            long encoding;
            if (Pred2Pid.TryGetValue(pred, out encoding))
            {
                return encoding;
            }
            cur_p_encoding++;
            Pred2Pid.Add(pred, cur_p_encoding);
            return cur_p_encoding;
        }

        private static string ConcatPids(HashSet<string> preds)
        {
            List<long> pids = new List<long>();
            foreach (var pred in preds)
            {
                pids.Add(GetOrAddPid(pred));
            }
            pids.Sort();
            return string.Join(" ", pids);
        }

        private static void IncrFreq(Dictionary<string, int> xUDT_freq, string key)
        {
            if (string.IsNullOrEmpty(key))
            {
                return;
            }

            if (!xUDT_freq.ContainsKey(key))
            {
                xUDT_freq.Add(key, 1);
            }
            else
            {
                xUDT_freq[key] += 1;
            }
        }

        private static string ComposeSynPred(string pred, string obj) 
        {
            string pred_obj = ConcatSynPred(pred, obj);
            long synpid = GetOrAddPid(pred_obj);

            if (!Synpid2PidOid.ContainsKey(synpid))
            {
                long pid = GetOrAddPid(pred);
                long oid = GetOrAddPid(obj);
                var pid_oid = Tuple.Create(pid, oid);

                SynpidPidOids.Add(Tuple.Create(synpid, pid, oid));
                Synpid2PidOid.Add(synpid, pid_oid);
                PidOid2Synpid.Add(pid_oid, synpid);
            }

            return pred_obj;
        }

        public static string ConcatSynPred(string pred, string obj) 
        {
            return pred + " " + obj;
        }

        public static bool IsSynPred(string pred, string obj) 
        {
            return Pred2Pid.ContainsKey(ConcatSynPred(pred, obj));
        }

        public static void ScanFrom(string ntFilename, char fieldSeparator)
        {
            NTripleUtil.FieldSeparator = fieldSeparator;

            //string root_dir = StoreConfig.ComposeStoreMetaRootDir(rootDir);
            string root_dir = StylusConfig.GetStoreMetaRootDir();

            if (!Directory.Exists(root_dir))
            {
                Directory.CreateDirectory(root_dir);
            }

            Dictionary<string, int> xUDT_freq = new Dictionary<string, int>();

            using (StreamReader reader = new StreamReader(ntFilename))
            {
                string line;
                string pre = "";
                long cnt = 0;
                HashSet<string> preds = new HashSet<string>();
                while ((line = reader.ReadLine()) != null)
                {
                    if (++cnt % 1000000 == 0)
                    {
                        Log.WriteLine(LogLevel.Info, "Process " + cnt + " lines");
                    }
                    string[] splits = NTripleUtil.FastSplit(line);
                    if (splits.Length < 3)
                    {
                        Log.WriteLine(LogLevel.Warning, "Skip line: " + line);
                        continue;
                    }
                    if (splits[0] != pre && pre != "")
                    {
                        IncrFreq(xUDT_freq, ConcatPids(preds));
                        preds = new HashSet<string>();
                    }
                    pre = splits[0];
                    //preds.Add(splits[1]);
                    if (!PredCandidatesForSynPred.Contains(splits[1])) // Synthetic predicate to compose
                    {
                        preds.Add(splits[1]);
                    }
                    else
                    {
                        string pred_obj = ComposeSynPred(splits[1], splits[2]);
                        preds.Add(pred_obj);
                    }
                }
                Log.WriteLine(LogLevel.Info, "Process " + cnt + " lines");
                IncrFreq(xUDT_freq, ConcatPids(preds));
            }

            using (StreamWriter writer = new StreamWriter(root_dir + StylusConfig.PidMapFilename))
            {
                foreach (var kvp in Pred2Pid)
                {
                    writer.WriteLine(kvp.Key + "\t" + kvp.Value);
                }
            }

            using (StreamWriter writer = new StreamWriter(root_dir + StylusConfig.xUDTFilename))
            {
                foreach (var kvp in xUDT_freq.OrderBy(xf => -1 * xf.Value))
                {
                    writer.WriteLine(kvp.Key + "\t" + kvp.Value);
                }
            }

            using (StreamWriter writer = new StreamWriter(root_dir + StylusConfig.SynPredMapFilename))
            {
                foreach (var tuple in SynpidPidOids)
                {
                    writer.WriteLine(tuple.Item1 + "\t" + tuple.Item2 + "\t" + tuple.Item3);
                }
            }
        }
        #endregion
    }
}
