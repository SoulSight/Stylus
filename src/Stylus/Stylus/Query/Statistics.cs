using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

using Trinity;
using Trinity.Diagnostics;

using Stylus.DataModel;
using Stylus.Storage;
using Stylus.Util;

namespace Stylus.Query
{
    public class Statistics
    {
        internal Dictionary<ushort, Dictionary<long, double>> Tid2Pid2OidSel = null;

        private IStorage server;

        public Statistics()
        {
            if (!File.Exists(GetClusterPersistFilename()) && !File.Exists(GetPersistFilename()))
            {
                BuildFromStorage();
            }
            else
            {
                Log.WriteLine(LogLevel.Info, "Building Statistics from Files...");
                LoadFromFile();
            }
        }

        public Statistics(string filename)
        {
            LoadFromFile(filename);
        }

        public Statistics(IStorage server) 
        {
            this.server = server;
            Log.WriteLine(LogLevel.Info, "Building Statistics from Storage...");
            BuildFromStorage();
            Log.WriteLine(LogLevel.Info, "Statistics Ready.");
        }

        public Statistics(Dictionary<ushort, Dictionary<long, double>> tid2Pid2OidSel) 
        {
            this.Tid2Pid2OidSel = tid2Pid2OidSel;
        }

        public void BuildFromStorage() 
        {
            this.Tid2Pid2OidSel = new Dictionary<ushort, Dictionary<long, double>>();

            foreach (var kvp in server.TidInstances)
            {
                var tid = kvp.Key;
                var eids = kvp.Value;
                Tid2Pid2OidSel.Add(tid, new Dictionary<long, double>());

                List<long> pids = StylusSchema.Tid2Pids[tid];
                double eid_num = (double)eids.Count;

                var pid2oidset = new Dictionary<long, XHashSet<long>>();
                foreach (var eid in eids)
                {
                    var leaves = server.SelectObjects(eid, pids);
                    for (int i = 0; i < pids.Count; i++)
                    {
                        long pid = pids[i];
                        long[] oids = leaves[i];
                        if (!pid2oidset.ContainsKey(pid))
                        {
                            pid2oidset.Add(pid, new XHashSet<long>(37));
                        }

                        foreach (var oid in oids)
                        {
                            pid2oidset[pid].Add(oid);
                        }
                    }
                }

                foreach (var p2oset in pid2oidset)
                {
                    double oid_sel = (double)p2oset.Value.Count / eid_num;
                    Tid2Pid2OidSel[tid].Add(p2oset.Key, oid_sel);
                }
            }
        }

        public static string GetPersistFilename() 
        {
            return StylusConfig.GetStoreMetaRootDir() + StylusConfig.xUDTStatisticsFilename;
        }

        public static string GetClusterPersistFilename()
        {
            return StylusConfig.GetStoreMetaRootDir() + StylusConfig.xUDTClusterStatisticsFilename;
        }

        public void SaveToFile(string filename = null) 
        {
            if (filename == null)
            {
                filename = GetPersistFilename();
            }
            using (StreamWriter writer = new StreamWriter(filename))
            {
                foreach (var tid2pid2oidnum in Tid2Pid2OidSel)
                {
                    ushort tid = tid2pid2oidnum.Key;
                    foreach (var kvp in tid2pid2oidnum.Value)
                    {
                        writer.WriteLine(tid + "\t" + kvp.Key + "\t" + kvp.Value);
                    }
                }
            }
        }

        public void SaveToClusterFile() 
        {
            SaveToFile(GetClusterPersistFilename());
        }

        public void LoadFromFile()
        {
            string cluster_filename = GetClusterPersistFilename();
            string filename = GetPersistFilename();

            if (File.Exists(cluster_filename))
            {
                LoadFromFile(GetPersistFilename());
            }
            else if (File.Exists(filename))
            {
                LoadFromFile(GetPersistFilename());
            }
            else
            {
                throw new Exception("Statistics File Not Found.");
            }
        }

        public void LoadFromFile(string filename)
        {
            Tid2Pid2OidSel = new Dictionary<ushort, Dictionary<long, double>>();

            //string filename = GetPersistFilename();
            foreach (var line in File.ReadLines(filename))
            {
                string[] splits = line.Split('\t');
                if (splits.Length < 3)
                {
                    continue;
                }
                ushort tid = ushort.Parse(splits[0]);
                long pid = long.Parse(splits[1]);
                double oid_sel = double.Parse(splits[2]);

                if (!Tid2Pid2OidSel.ContainsKey(tid))
                {
                    Tid2Pid2OidSel.Add(tid, new Dictionary<long, double>());
                }
                Tid2Pid2OidSel[tid].Add(pid, oid_sel);
            }
        }

        // Update rule: 1) existing value > non-existing value; 2) large value > small value.
        public void UpdateFromCluster(LocalStatInfo localStatInfo) 
        {
            if (this.Tid2Pid2OidSel == null)
            {
                this.Tid2Pid2OidSel = new Dictionary<ushort, Dictionary<long, double>>();
            }

            foreach (var tid_info in localStatInfo.LocalStat)
            {
                ushort tid = tid_info.Tid;
                if (!this.Tid2Pid2OidSel.ContainsKey(tid))
                {
                    this.Tid2Pid2OidSel.Add(tid, new Dictionary<long,double>());
                }
                foreach (var tid_pid_info in tid_info.TidStat)
                {
                    long pid = tid_pid_info.Pid;
                    double sel = tid_pid_info.Sel;

                    if (!this.Tid2Pid2OidSel[tid].ContainsKey(pid))
                    {
                        this.Tid2Pid2OidSel[tid].Add(pid, sel);
                    }
                    else
                    {
                        this.Tid2Pid2OidSel[tid][pid] += sel;
                    }
                    //if (this.Tid2Pid2OidSel[tid][pid] < sel)
                    //{
                    //    this.Tid2Pid2OidSel[tid][pid] = sel;
                    //}
                }
            }
        }

        public void Divide(double cnt) 
        {
            foreach (var tid in Tid2Pid2OidSel.Keys.ToList())
            {
                foreach (var pid in Tid2Pid2OidSel[tid].Keys.ToList())
                {
                    Tid2Pid2OidSel[tid][pid] /= cnt;
                }
            }
        }

        public double EstimateLeafCard(double root_cardinality, ushort tid, long pid) 
        {
            return root_cardinality * Tid2Pid2OidSel[tid][pid];
        }

        public double EstimateRootCard(List<long> pids)
        {
            var sup_tids = StylusSchema.SupType(pids);
            double total = 0.0;
            foreach (var tid in sup_tids)
            {
                total += StylusSchema.GetTidInstCount(tid);
            }
            return total;
        }

        public double EstimateLeafCard(double root_cardinality, List<long> pids, long leaf_pid)
        {
            var sup_tids = StylusSchema.SupType(pids);
            Dictionary<ushort, double> inst_counts = new Dictionary<ushort, double>();

            double total = 0.0;
            foreach (var tid in sup_tids)
            {
                double tid_inst_count = StylusSchema.GetTidInstCount(tid);
                total += tid_inst_count;
                inst_counts.Add(tid, tid_inst_count);
            }
            double ratio = root_cardinality / total;

            double card = 0.0;
            foreach (var tid in sup_tids)
            {
                card += ratio * EstimateLeafCard(inst_counts[tid], tid, leaf_pid);
            }
            return card;
        }

        public double EstimateLeafCard(List<long> pids, long leaf_pid)
        {
            double root_card = EstimateRootCard(pids);
            return EstimateLeafCard(root_card, pids, leaf_pid);
        }
    }
}
