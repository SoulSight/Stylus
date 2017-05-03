using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Trinity;
using Trinity.Diagnostics;

using Stylus.Util;
using Stylus.DataModel;

namespace Stylus.Loading
{
    public class DataScanner
    {
        private static Dictionary<string, ushort> plist_tid_dict = new Dictionary<string, ushort>();
        private static XDictionary<string, long> literal_to_eid = new XDictionary<string, long>(17);
        private static Dictionary<ushort, long> tid_cur_pos = new Dictionary<ushort, long>();

        public static void LoadSchemaFromFile()
        {
            StylusSchema.LoadFromFile();

            plist_tid_dict.Clear();
            foreach (var kvp in StylusSchema.Tid2Pids)
            {
                string str = string.Join(" ", kvp.Value);
                plist_tid_dict.Add(str, kvp.Key);
            }
        }

        private static void LoadIdMappingFromFile()
        {
            IOUtil.LoadEidMapFile((literal, eid) => literal_to_eid.Add(literal, eid));
        }

        private static ushort GetTid(List<long> pids)
        {
            pids.Sort();
            string plist_str = string.Join(" ", pids);
            ushort tid = StylusConfig.GenericTid;
            plist_tid_dict.TryGetValue(plist_str, out tid);
            return tid;
        }

        private static IEnumerable<EncodedLoadingEntity> EnumerateEncodedEntity(string filename, char sep) 
        {
            NTripleUtil.FieldSeparator = sep;
            string pre = "";
            Dictionary<long, List<long>> pos = new Dictionary<long, List<long>>();
            long cnt = 0;
            foreach (var line in File.ReadLines(filename))
            {
                if (++cnt % 1000000 == 0)
                {
                    Console.WriteLine("EnumerateEncodedEntity: " + cnt);
                }
                string[] splits = line.Split(sep);
                if (splits[0] != pre && pre != "")
                {
                    yield return new EncodedLoadingEntity() { Literal = long.Parse(pre), POs = pos };
                    pos = new Dictionary<long, List<long>>();
                }
                pre = splits[0];

                long pid = long.Parse(splits[1]);
                long oid = long.Parse(splits[2]);
                if (StylusSchema.Pid2Synpids.ContainsKey(pid)) // Synthetic predicate to compose
                {
                    long synpid = StylusSchema.PidOid2Synpid[Tuple.Create(pid, oid)];
                    pid = synpid;
                }

                if (!pos.ContainsKey(pid))
                {
                    pos.Add(pid, new List<long>());
                }
                pos[pid].Add(long.Parse(splits[2]));
            }
            Console.WriteLine("EnumerateEncodedEntity: " + cnt);
            yield return new EncodedLoadingEntity() { Literal = long.Parse(pre), POs = pos };
        }

        private static IEnumerable<LoadingEntity> EnumerateEntity(string filename, char sep)
        {
            NTripleUtil.FieldSeparator = sep;
            string pre = "";
            Dictionary<long, List<string>> pos = new Dictionary<long, List<string>>();
            long cnt = 0;
            foreach (var line in File.ReadLines(filename))
            {
                if (++cnt % 1000000 == 0)
                {
                    Console.WriteLine("EnumerateEntity: " + cnt);
                }
                string[] splits = NTripleUtil.FastSplit(line); //line.Split(sep);
                if (splits[0] != pre && pre != "")
                {
                    yield return new LoadingEntity() { Literal = pre, POs = pos };
                    pos = new Dictionary<long, List<string>>();
                }
                pre = splits[0];

                long pid;
                if (!StylusSchema.PredCandidatesForSynPred.Contains(splits[1])) // Synthetic predicate to compose
                {
                    pid = StylusSchema.GetOrAddPid(splits[1]);
                }
                else
                {
                    long synpid = StylusSchema.GetOrAddPid(StylusSchema.ConcatSynPred(splits[1], splits[2]));
                    pid = synpid;
                }

                if (!pos.ContainsKey(pid))
                {
                    pos.Add(pid, new List<string>());
                }
                pos[pid].Add(splits[2]);
            }
            Console.WriteLine("EnumerateEntity: " + cnt);
            yield return new LoadingEntity() { Literal = pre, POs = pos };
        }

        public static void AssignEids(string rdfFilename, char sep)
        {
            LoadSchemaFromFile();

            // scan rdf file once =>: eid_id_dict
            var kvps = EnumerateEntity(rdfFilename, sep).Select(entity =>
            {
                ushort tid = GetTid(entity.POs.Keys.ToList());
                if (!tid_cur_pos.ContainsKey(tid))
                {
                    tid_cur_pos.Add(tid, 1);
                }
                long cur_id_pos = tid_cur_pos[tid];
                tid_cur_pos[tid] += 1;
                long id = TidUtil.CloneMaskTid(cur_id_pos, tid);
                return new KeyValuePair<string, long>(entity.Literal, id);
            });
            IOUtil.SaveEidMapFile(kvps);
        }

        private static void LoadEntity(LoadingEntity entity)
        {
            Dictionary<long, List<long>> pid_oids_dict = new Dictionary<long, List<long>>();
            long eid = literal_to_eid[entity.Literal];

            if (!ClusterUtil.IsLocalEntity(eid))
            {
                return;
            }

            foreach (var kvp in entity.POs)
            {
                List<long> oids = kvp.Value.Select(o => literal_to_eid[o]).ToList();
                TidUtil.SortByTid(oids);
                pid_oids_dict.Add(kvp.Key, oids);
            }

            ushort tid = GetTid(entity.POs.Keys.ToList());

            if (tid != StylusConfig.GenericTid)
            {
                List<int> offsets = new List<int>();
                List<long> obj_vals = new List<long>();

                var pids = StylusSchema.Tid2Pids[tid];

                for (int order = 0; order < pids.Count; order++)
                {
                    obj_vals.AddRange(pid_oids_dict[pids[order]]);
                    offsets.Add(obj_vals.Count);
                }

                xEntity xentity = new xEntity(eid, tid, offsets, obj_vals);
                Global.LocalStorage.SavexEntity(xentity);
            }
            else
            {
                // Load generic xUDTs
                List<Property> props = new List<Property>();
                foreach (var kvp in pid_oids_dict)
                {
                    props.Add(new Property(kvp.Key, kvp.Value));
                }

                GenericPropEntity gpe = new GenericPropEntity(eid, tid, props);
                Global.LocalStorage.SaveGenericPropEntity(gpe);
            }
        }

        private static void LoadEncodedEntity(EncodedLoadingEntity entity)
        {
            if (!ClusterUtil.IsLocalEntity(entity.Literal))
            {
                return;
            }

            foreach (var kvp in entity.POs)
            {
                List<long> oids = kvp.Value;
                TidUtil.SortByTid(oids);
            }

            ushort tid = GetTid(entity.POs.Keys.ToList());

            if (tid != StylusConfig.GenericTid)
            {
                List<int> offsets = new List<int>();
                List<long> obj_vals = new List<long>();

                var pids = StylusSchema.Tid2Pids[tid];

                for (int order = 0; order < pids.Count; order++)
                {
                    obj_vals.AddRange(entity.POs[pids[order]]);
                    offsets.Add(obj_vals.Count);
                }

                xEntity xentity = new xEntity(entity.Literal, tid, offsets, obj_vals);
                Global.LocalStorage.SavexEntity(xentity);
            }
            else
            {
                // Load generic xUDTs
                List<Property> props = new List<Property>();
                foreach (var kvp in entity.POs)
                {
                    props.Add(new Property(kvp.Key, kvp.Value));
                }

                GenericPropEntity gpe = new GenericPropEntity(entity.Literal, tid, props);
                Global.LocalStorage.SaveGenericPropEntity(gpe);
            }
        }

        public static void EncodeFile(string rdfFilename, string encodedFilename, char sep) 
        {
            NTripleUtil.FieldSeparator = sep;
            LoadSchemaFromFile();

            // scan rdf file once =>: eid_id_dict
            literal_to_eid = new XDictionary<string, long>();
            IOUtil.LoadEidMapFile((literal, eid) => literal_to_eid.Add(literal, eid));

            long cnt = 0;
            using (StreamWriter writer = new StreamWriter(encodedFilename))
            {
                foreach (var line in File.ReadLines(rdfFilename))
                {
                    if (++cnt % 1000000 == 0)
                    {
                        Console.WriteLine("Encoded: " + cnt);
                    }
                    string[] splits = NTripleUtil.FastSplit(line);
                    if (splits.Length < 3)
                    {
                        continue;
                    }

                    string subj = splits[0];
                    string pred = splits[1];
                    string obj = splits[2];

                    if (StylusSchema.PredCandidatesForSynPred.Contains(pred))
                    {
                        long subj_encoding = literal_to_eid[subj];
                        long pred_encoding = StylusSchema.Pred2Pid[pred];
                        long obj_encoding = StylusSchema.Pred2Pid[obj];
                        writer.WriteLine("" + subj_encoding + sep + pred_encoding + sep + obj_encoding);
                    }
                    else
                    {
                        long subj_encoding = literal_to_eid[subj];
                        long pred_encoding = StylusSchema.Pred2Pid[pred];
                        long obj_encoding = literal_to_eid[obj];
                        writer.WriteLine("" + subj_encoding + sep + pred_encoding + sep + obj_encoding);
                    }
                }
            }
            Console.WriteLine("Encoded: " + cnt);
        }

        public static void LoadFile(string rdfFilename, char sep = ' ')
        {
            Log.WriteLine(LogLevel.Info, "Load Schema From File...");
            LoadSchemaFromFile();
            StylusSchema.SaveToStorage();

            if (literal_to_eid.Count == 0)
            {
                Log.WriteLine(LogLevel.Info, "Load Id Mapping From File...");
                LoadIdMappingFromFile();
            }

            Log.WriteLine(LogLevel.Info, "Load Entity...");
            foreach (var xentity in EnumerateEntity(rdfFilename, sep))
            {
                LoadEntity(xentity);
            }

            Log.WriteLine(LogLevel.Info, "Totol Cell Count: " + Global.LocalStorage.CellCount);
            Global.LocalStorage.SaveStorage();
        }

        public static void LoadEncodedFile(string encodedFilename, char sep = ' ')
        {
            Log.WriteLine(LogLevel.Info, "Load Schema From File...");
            LoadSchemaFromFile();
            StylusSchema.SaveToStorage();

            Log.WriteLine(LogLevel.Info, "Load Encoded Entity...");
            foreach (var xentity in EnumerateEncodedEntity(encodedFilename, sep))
            {
                LoadEncodedEntity(xentity);
            }

            Log.WriteLine(LogLevel.Info, "Totol Cell Count: " + Global.LocalStorage.CellCount);
            Global.LocalStorage.SaveStorage();
        }

        public static void GenStatFromEncodedFile(string encodedFilename, string statisticsFilename, char sep = ' ') 
        {
            Dictionary<ushort, Dictionary<long, double>> tid2pid2sel 
                = new Dictionary<ushort, Dictionary<long, double>>();

            Dictionary<ushort, double> tid2count = new Dictionary<ushort, double>();

            Dictionary<ushort, Dictionary<long, XHashSet<long>>> tid2pid2oids
                = new Dictionary<ushort, Dictionary<long, XHashSet<long>>>();
            foreach (var ee in EnumerateEncodedEntity(encodedFilename, sep))
            {
                var tid = TidUtil.GetTid(ee.Literal);
                if (!tid2count.ContainsKey(tid))
                {
                    tid2count.Add(tid, 1);
                }
                else
                {
                    tid2count[tid] += 1;
                }

                if (!tid2pid2oids.ContainsKey(tid))
                {
                    tid2pid2oids.Add(tid, new Dictionary<long, XHashSet<long>>());
                }
                foreach (var po in ee.POs)
                {
                    if (!tid2pid2oids[tid].ContainsKey(po.Key))
                    {
                        tid2pid2oids[tid].Add(po.Key, new XHashSet<long>(17));
                    }
                    foreach (var o in po.Value)
                    {
                        tid2pid2oids[tid][po.Key].Add(o);
                    }
                }
            }

            using (StreamWriter writer = new StreamWriter(statisticsFilename))
            {
                foreach (var kvp in tid2pid2oids)
                {
                    ushort tid = kvp.Key;
                    double tid_cnt = tid2count[tid];
                    tid2pid2sel.Add(tid, new Dictionary<long,double>());
                    foreach (var pos in kvp.Value)
                    {
                        double sel = (double)pos.Value.Count / tid_cnt;
                        tid2pid2sel[tid].Add(pos.Key, sel);
                        writer.WriteLine(tid + "\t" + pos.Key + "\t" + sel);
                    }
                }
            }
        }
    }
}
