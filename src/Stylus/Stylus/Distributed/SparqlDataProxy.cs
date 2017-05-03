using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Trinity;
using Trinity.Diagnostics;

using Stylus.DataModel;
using Stylus.Parsing;
using Stylus.Storage;
using Stylus.Util;
using Stylus.Query;
using System.Diagnostics;

namespace Stylus.Distributed
{
    public class SparqlDataProxy : SparqlDataProxyBase
    {
        private SparqlParser parser = new SparqlParser();
        private Statistics statistics;
        private XDictionary<string, long> LiteralToId;
        private int cur_query_id = 0;

        public SparqlDataProxy() 
        {
            StylusSchema.LoadFromFile();
            // this.statistics = new Statistics();
            LoadLiteralToId();
        }

        private void LoadLiteralToId()
        {
            // LoadLiteralToEid
            this.LiteralToId = new XDictionary<string, long>(17);
            IOUtil.LoadEidMapFile((literal, eid) => this.LiteralToId.Add(literal, eid));
        }

        #region Handlers
        public override void LoadFileHandler(LoadFileInfoReader request)
        {
            LoadFile((LoadFileInfo)request);
        }

        public override void LoadEncodedFileHandler(LoadFileInfoReader request)
        {
            LoadEncodedFile((LoadFileInfo)request);
        }

        public override void LoadStorageHandler()
        {
            LoadStorage();
        }

        public override void ExecuteQueryHandler(SparqlQueryReader request, QueryResultsWriter response)
        {
            var results = ExecuteQuery(request);
            response = new QueryResultsWriter(results.Variables, results.Records);
        }
        #endregion

        #region Public Methods
        public void LoadFile(LoadFileInfo request)
        {
            Parallel.For(0, Global.ServerCount, i =>
            {
                Global.CloudStorage.LoadFileToSparqlDataServer(i, new LoadFileInfoWriter(request.FilePath, request.SchemaDir, request.UnfoldIsA));
            });
        }

        public void LoadEncodedFile(LoadFileInfo request) 
        {
            Parallel.For(0, Global.ServerCount, i =>
            {
                Global.CloudStorage.LoadEncodedFileToSparqlDataServer(i, new LoadFileInfoWriter(request.FilePath, request.SchemaDir, request.UnfoldIsA));
            });
        }

        public void LoadStorage() 
        {
            // Global.CloudStorage.LoadStorage();
            Parallel.For(0, Global.ServerCount, i =>
            {
                Global.CloudStorage.LoadStorageToSparqlDataServer(i);
            });
        }

        public void SetParserFixFunc(Func<string, string> func) 
        {
            this.parser.FixStrFunc = func;
        }

        public void InitStatistics() 
        {
            if (File.Exists(Statistics.GetClusterPersistFilename()))
            {
                this.statistics = new Statistics(Statistics.GetClusterPersistFilename());
            }
            else
            {
                this.statistics = ClusterAggregateStatistics();
                this.statistics.SaveToClusterFile();
            }
        }

        public QueryResults ExecuteQuery(SparqlQuery request) 
        {
            string query_str = request.Content;
            var qg = parser.ParseQueryFromString(query_str);

            var heads = Plan(qg);

            int query_id;
            lock (this)
            {
                query_id = cur_query_id++;
            }

            //Console.WriteLine("heads.Count: " + heads.Count);
            //foreach (var head in heads)
            //{
            //    Console.WriteLine(head.ToString());
            //}

            Stopwatch sw = new Stopwatch();
            sw.Start();
            // The query execution procedure
            ClusterIssueQuery(query_id, heads);
            //Log.WriteLine(LogLevel.Info, "After IssueQuery Elapsed: " + sw.Elapsed.TotalMilliseconds + " ms");

            if (heads.Count <= 1)
            {
                ClusterExecuteQueryStep(query_id, 0);
                //Log.WriteLine(LogLevel.Info, "After ExecuteQueryStep Elapsed: " + sw.Elapsed.TotalMilliseconds + " ms");
            }
            else
            {
                for (int step_index = 0; step_index < heads.Count; step_index++)
                {
                    ClusterExecuteQueryStep(query_id, step_index);
                    //Log.WriteLine(LogLevel.Info, "After ExecuteQueryStep Elapsed: " + sw.Elapsed.TotalMilliseconds + " ms");
                    if (step_index < heads.Count - 1)
                    {
                        ClusterSyncBinding(query_id, step_index);
                        //Log.WriteLine(LogLevel.Info, "After SyncBinding Elapsed: " + sw.Elapsed.TotalMilliseconds + " ms");
                        ClusterFinishSyncStep(query_id, step_index);
                        //Log.WriteLine(LogLevel.Info, "After FinishSync Elapsed: " + sw.Elapsed.TotalMilliseconds + " ms");
                    }
                }
            }

            TwigAnswers[] intermediate_results = ClusterAggregateQueryResults(query_id, heads.Count);

            //Log.WriteLine(LogLevel.Info, "After AggregateQueryResults Elapsed: " + sw.Elapsed.TotalMilliseconds + " ms");
            
            // Join the final results
            //for (int i = 0; i < intermediate_results.Length; i++)
            //{
            //    Log.WriteLine(LogLevel.Info, "xTwig Size " + i + ": " + intermediate_results[i].TwigCount);
            //    Log.WriteLine(LogLevel.Info, "CalcActualSize " + i + ": " + intermediate_results[i].ActualSize);
            //}

            QuerySolutions qs = null;

            List<TwigSolutions> twig_results = new List<TwigSolutions>();
            for (int i = 0; i < intermediate_results.Length; i++)
            {
                TwigSolutions ts = new TwigSolutions();
                ts.Head = heads[i];
                ts.SetActualSize(intermediate_results[i].ActualSize);
                ts.Solutions = intermediate_results[i].Elements;
                twig_results.Add(ts);
            }
            if (twig_results.Count == 1)
            {
                qs = twig_results[0].Flatten();
            }
            else
            {
                twig_results.Sort((ts1, ts2) => { return ts1.GetActualSize().CompareTo(ts2.GetActualSize()); });
                qs = twig_results[0].JoinFlattenBySizeOrder(twig_results[1]);
                if (twig_results.Count > 2)
                {
                    for (int i = 2; i < twig_results.Count; i++)
                    {
                        qs = twig_results[i].JoinFlatten(qs);
                    }
                }
            }

            sw.Stop();

            ClusterFreeQuery(query_id);

            if (qs == null)
            {
                throw new Exception("Null Final Results.");
            }
            Log.WriteLine(LogLevel.Info, qs.Records.Count + " results, " + sw.Elapsed.TotalMilliseconds + " ms");

            return new QueryResults(qs.Heads, qs.Records.Select(rec => rec.ToList()).ToList()); 
        }

        public QueryResults ExecuteQueryWithParallelJoin(SparqlQuery request)
        {
            string query_str = request.Content;
            var qg = parser.ParseQueryFromString(query_str);

            var heads = Plan(qg);

            int query_id;
            lock (this)
            {
                query_id = cur_query_id++;
            }

            Console.WriteLine("heads.Count: " + heads.Count);
            foreach (var head in heads)
            {
                Console.WriteLine(head.ToString());
            }

            Stopwatch sw = new Stopwatch();
            sw.Start();
            // The query execution procedure
            ClusterIssueQuery(query_id, heads);
            Console.WriteLine("After IssueQuery Elapsed: " + sw.Elapsed.TotalMilliseconds + " ms");

            QuerySolutions qs = null;
            List<long> ids_as_qs = null;
            if (heads.Count <= 1)
            {
                ClusterExecuteQueryStep(query_id, 0);
                Console.WriteLine("After ExecuteQueryStep Elapsed: " + sw.Elapsed.TotalMilliseconds + " ms");

                if (heads[0].SelectLeaves.Count == 0)
                {
                    ids_as_qs = ClusterAggregateIdList(query_id); // todo
                }
                else
                {
                    TwigAnswers intermediate_result = ClusterAggregateQueryResults(query_id, heads.Count)[0];
                    TwigSolutions ts = new TwigSolutions();
                    ts.Head = heads[0];
                    ts.Solutions = intermediate_result.Elements;
                    qs = ts.Flatten(qg.SelectedVariables);
                }
            }
            else
            {
                for (int step_index = 0; step_index < heads.Count; step_index++)
                {
                    ClusterExecuteQueryStep(query_id, step_index);
                    Console.WriteLine("After ExecuteQueryStep Elapsed: " + sw.Elapsed.TotalMilliseconds + " ms");
                    if (step_index < heads.Count - 1)
                    {
                        ClusterSyncBinding(query_id, step_index);
                        Console.WriteLine("After SyncBinding Elapsed: " + sw.Elapsed.TotalMilliseconds + " ms");
                        ClusterFinishSyncStep(query_id, step_index);
                        Console.WriteLine("After FinishSync Elapsed: " + sw.Elapsed.TotalMilliseconds + " ms");
                    }
                }
                ClusterBroadcastSuffixQueryAnswers(query_id);
                ClusterJoinQueryAnswers(query_id);
                qs = ClusterAggregateQueryResults(query_id);
            }

            sw.Stop();

            ClusterFreeQuery(query_id);

            if (qs == null && ids_as_qs == null)
            {
                throw new Exception("Null Final Results.");
            }

            if (qs != null)
            {
                Log.WriteLine(LogLevel.Info, qs.Records.Count + " results, " + sw.Elapsed.TotalMilliseconds + " ms");
                //if (qs.Records.Count > 0)
                //{
                //    Console.WriteLine("Rec length: " + qs.Records[0].Length);
                //}
                return new QueryResults(qs.Heads, qs.Records.Select(rec => rec.ToList()).ToList());
            }
            else
            {
                Log.WriteLine(LogLevel.Info, ids_as_qs.Count + " results, " + sw.Elapsed.TotalMilliseconds + " ms");
                return new QueryResults(qg.SelectedVariables, ids_as_qs.Select(rec => new List<long>() { rec }).ToList());
            }
        }
        #endregion

        #region Query Planning
        private List<xTwigHead> Plan(QueryGraph qg)
        {
            Dictionary<string, Binding> bindings;
            var root_order = RootOrderSelection(qg, this.statistics, out bindings);
            return Decompose(qg, root_order, bindings);
        }

        // Strategy: add all the cardinalities of the query nodes
        private List<string> RootOrderSelection3(QueryGraph qg, Statistics statistics,
          out Dictionary<string, Binding> bindings)
        {
            Dictionary<string, double> node_cardinality = new Dictionary<string, double>();
            Dictionary<string, string> superiors = new Dictionary<string, string>();
            Dictionary<string, List<long>> node_pids = new Dictionary<string, List<long>>();
            bindings = new Dictionary<string, Binding>();

            // Initialize
            foreach (var kvp in qg.NameToNodes)
            {
                string node_name = kvp.Key;
                superiors.Add(node_name, null);
                List<long> pids = kvp.Value.GetStarShapePredIncludingSyn().Select(str => StylusSchema.Pred2Pid[str]).ToList();
                node_pids.Add(node_name, pids);
                double card = kvp.Value.IsVariable ? statistics.EstimateRootCard(pids) : 1.0;
                node_cardinality.Add(node_name, card);

                if (kvp.Value.IsVariable)
                {
                    bindings.Add(node_name, new TidBinding(StylusSchema.SupType(pids)));
                }
                else
                {
                    bindings.Add(node_name, new UniEidBinding(LiteralToId[node_name]));
                }
            }

            HashSet<QueryNode> nodes_to_mark = new HashSet<QueryNode>();
            HashSet<QueryEdge> edges_to_mark = new HashSet<QueryEdge>();
            foreach (var qe in qg.Edges)
            {
                var src_node = qe.SrcNode;
                var tgt_node = qe.TgtNode;
                bool mark_src = false;
                bool mark_tgt = false;
                if (qg.ToSelect(src_node)) // helper var
                {
                    nodes_to_mark.Add(src_node);
                    mark_src = true;
                }
                else if (!src_node.IsVariable)
                {
                    nodes_to_mark.Add(src_node);
                    mark_src = true;
                }

                if (!StylusSchema.IsSynPred(qe.Predicate, tgt_node.Name)) // && the edge is not for synthetic pred
                {
                    if (qg.ToSelect(tgt_node))
                    {
                        nodes_to_mark.Add(tgt_node);
                        mark_tgt = true;
                    }
                    else if (!tgt_node.IsVariable)
                    {
                        nodes_to_mark.Add(tgt_node);
                        mark_tgt = true;
                    }
                }

                if (mark_src && mark_tgt)
                {
                    edges_to_mark.Add(qe);
                }
            }

            if (nodes_to_mark.Count == 1)
            {
                return new List<string>() { nodes_to_mark.ToArray()[0].Name };
            }

            // Select node order // try all 
            double min_cost = double.MaxValue;
            List<string> min_roots = null;
            foreach (var roots in ListUtil.GetPermutation(nodes_to_mark.Select(n => n.Name).ToList()))
            {
                Dictionary<string, double> local_card = new Dictionary<string, double>(node_cardinality);

                HashSet<string> processed = new HashSet<string>();
                foreach (var root in roots)
                {
                    double root_card = local_card[root];
                    var root_pids = node_pids[root];

                    processed.Add(root);

                    foreach (var pe in qg.NameToNodes[root].ParticipatedEdges)
                    {
                        if (!edges_to_mark.Contains(pe))
                        {
                            continue;
                        }
                        string leaf_name = null;
                        long leaf_pid = 0;
                        if (pe.SrcNode.Name == root)
                        {
                            leaf_pid = StylusSchema.Pred2Pid[pe.Predicate];
                            leaf_name = pe.TgtNode.Name;
                        }
                        else
                        {
                            leaf_pid = StylusSchema.Pred2Pid["_" + pe.Predicate];
                            leaf_name = pe.SrcNode.Name;
                        }

                        long rev_leaf_pid = StylusSchema.InvPreds[leaf_pid];
                        if (!processed.Contains(leaf_name))
                        {
                            var orig_leaf_card = local_card[leaf_name];
                            var new_leaf_card = statistics.EstimateLeafCard(root_card, root_pids, leaf_pid);
                            new_leaf_card = new_leaf_card * statistics.EstimateRootCard(node_pids[leaf_name]) /
                                statistics.EstimateRootCard(new List<long>() { rev_leaf_pid });
                            if (new_leaf_card < orig_leaf_card)
                            {
                                local_card[leaf_name] = new_leaf_card;
                            }
                        }

                    }
                }

                double cur_cost = 1.0;
                foreach (var root in roots)
                {
                    cur_cost += local_card[root];
                }
                //Console.WriteLine(string.Join(", ", roots) + ": " + cur_cost); // Debug

                if (cur_cost < min_cost)
                {
                    min_cost = cur_cost;
                    min_roots = roots;
                }
            }
            return min_roots;
        }
        
        // 4 strategy: small node set = permutation; large node set = foreach first + greedy
        private List<string> RootOrderSelection(QueryGraph qg, Statistics statistics,
          out Dictionary<string, Binding> bindings)
        {
            Dictionary<string, double> node_cardinality = new Dictionary<string, double>();
            Dictionary<string, string> superiors = new Dictionary<string, string>();
            Dictionary<string, List<long>> node_pids = new Dictionary<string, List<long>>();
            bindings = new Dictionary<string, Binding>();

            // Initialize
            foreach (var kvp in qg.NameToNodes)
            {
                string node_name = kvp.Key;
                superiors.Add(node_name, null);
                List<long> pids = kvp.Value.GetStarShapePredIncludingSyn().Select(str => StylusSchema.Pred2Pid[str]).ToList();
                node_pids.Add(node_name, pids);
                double card = kvp.Value.IsVariable ? statistics.EstimateRootCard(pids) : 1.0;
                node_cardinality.Add(node_name, card);

                if (kvp.Value.IsVariable)
                {
                    bindings.Add(node_name, new TidBinding(StylusSchema.SupType(pids)));
                }
                else
                {
                    bindings.Add(node_name, new UniEidBinding(LiteralToId[node_name]));
                }
            }

            HashSet<QueryNode> nodes_to_mark = new HashSet<QueryNode>();
            HashSet<QueryEdge> edges_to_mark = new HashSet<QueryEdge>();
            foreach (var qe in qg.Edges)
            {
                var src_node = qe.SrcNode;
                var tgt_node = qe.TgtNode;
                bool mark_src = false;
                bool mark_tgt = false;
                if (qg.ToSelect(src_node)) // helper var
                {
                    nodes_to_mark.Add(src_node);
                    mark_src = true;
                }
                else if (!src_node.IsVariable)
                {
                    nodes_to_mark.Add(src_node);
                    mark_src = true;
                }

                if (!StylusSchema.IsSynPred(qe.Predicate, tgt_node.Name)) // && the edge is not for synthetic pred
                {
                    if (qg.ToSelect(tgt_node))
                    {
                        nodes_to_mark.Add(tgt_node);
                        mark_tgt = true;
                    }
                    else if (!tgt_node.IsVariable)
                    {
                        nodes_to_mark.Add(tgt_node);
                        mark_tgt = true;
                    }
                }

                if (mark_src && mark_tgt)
                {
                    edges_to_mark.Add(qe);
                }
            }

            if (nodes_to_mark.Count == 1)
            {
                return new List<string>() { nodes_to_mark.ToArray()[0].Name };
            }

            // Select node order 
            double min_cost = double.MaxValue;
            List<string> min_roots = null;
            if (nodes_to_mark.Count <= 6) // try all permutations if #nodes is small
            {
                foreach (var roots in ListUtil.GetPermutation(nodes_to_mark.Select(n => n.Name).ToList()))
                {
                    Dictionary<string, double> local_card = new Dictionary<string, double>(node_cardinality);

                    HashSet<string> processed = new HashSet<string>();
                    foreach (var root in roots)
                    {
                        double root_card = local_card[root];
                        var root_pids = node_pids[root];

                        processed.Add(root);

                        foreach (var pe in qg.NameToNodes[root].ParticipatedEdges)
                        {
                            if (!edges_to_mark.Contains(pe))
                            {
                                continue;
                            }
                            string leaf_name = null;
                            long leaf_pid = 0;
                            if (pe.SrcNode.Name == root)
                            {
                                leaf_pid = StylusSchema.Pred2Pid[pe.Predicate];
                                leaf_name = pe.TgtNode.Name;
                            }
                            else
                            {
                                leaf_pid = StylusSchema.Pred2Pid["_" + pe.Predicate];
                                leaf_name = pe.SrcNode.Name;
                            }

                            long rev_leaf_pid = StylusSchema.InvPreds[leaf_pid];
                            if (!processed.Contains(leaf_name))
                            {
                                var orig_leaf_card = local_card[leaf_name];
                                var new_leaf_card = statistics.EstimateLeafCard(root_card, root_pids, leaf_pid);
                                new_leaf_card = new_leaf_card * statistics.EstimateRootCard(node_pids[leaf_name]) /
                                    statistics.EstimateRootCard(new List<long>() { rev_leaf_pid });
                                if (new_leaf_card < orig_leaf_card)
                                {
                                    local_card[leaf_name] = new_leaf_card;
                                }
                            }

                        }
                    }

                    double cur_cost = 0.0;
                    foreach (var root in roots)
                    {
                        cur_cost += local_card[root];
                    }
                    //Console.WriteLine(string.Join(", ", roots) + ": " + cur_cost); // Debug

                    if (cur_cost < min_cost)
                    {
                        min_cost = cur_cost;
                        min_roots = roots;
                    }
                }
            }
            else
            {
                var node_names_to_mark = nodes_to_mark.Select(n => n.Name).ToList();
                foreach (var first_root in node_names_to_mark)
                {
                    Dictionary<string, double> local_card = new Dictionary<string, double>(node_cardinality);
                    List<string> roots = new List<string>();
                    roots.Add(first_root);
                    HashSet<string> processed = new HashSet<string>();
                    processed.Add(first_root);

                    foreach (var pe in qg.NameToNodes[first_root].ParticipatedEdges)
                    {
                        if (!edges_to_mark.Contains(pe))
                        {
                            continue;
                        }
                        string leaf_name = null;
                        long leaf_pid = 0;
                        if (pe.SrcNode.Name == first_root)
                        {
                            leaf_pid = StylusSchema.Pred2Pid[pe.Predicate];
                            leaf_name = pe.TgtNode.Name;
                        }
                        else
                        {
                            leaf_pid = StylusSchema.Pred2Pid["_" + pe.Predicate];
                            leaf_name = pe.SrcNode.Name;
                        }

                        long rev_leaf_pid = StylusSchema.InvPreds[leaf_pid];
                        if (!processed.Contains(leaf_name))
                        {
                            var orig_leaf_card = local_card[leaf_name];
                            var new_leaf_card = statistics.EstimateLeafCard(local_card[first_root], node_pids[first_root], leaf_pid);
                            new_leaf_card = new_leaf_card * statistics.EstimateRootCard(node_pids[leaf_name]) /
                                statistics.EstimateRootCard(new List<long>() { rev_leaf_pid });
                            if (new_leaf_card < orig_leaf_card)
                            {
                                local_card[leaf_name] = new_leaf_card;
                            }
                        }
                    }

                    while (roots.Count < node_names_to_mark.Count)
                    {
                        double min_node_card = double.MaxValue;
                        string min_node = null;
                        foreach (var node_name in node_names_to_mark)
                        {
                            if (processed.Contains(node_name))
                            {
                                continue;
                            }
                            if (local_card[node_name] < min_node_card)
                            {
                                min_node_card = local_card[node_name];
                                min_node = node_name;
                            }
                        }

                        foreach (var pe in qg.NameToNodes[min_node].ParticipatedEdges)
                        {
                            if (!edges_to_mark.Contains(pe))
                            {
                                continue;
                            }
                            string leaf_name = null;
                            long leaf_pid = 0;
                            if (pe.SrcNode.Name == min_node)
                            {
                                leaf_pid = StylusSchema.Pred2Pid[pe.Predicate];
                                leaf_name = pe.TgtNode.Name;
                            }
                            else
                            {
                                leaf_pid = StylusSchema.Pred2Pid["_" + pe.Predicate];
                                leaf_name = pe.SrcNode.Name;
                            }

                            long rev_leaf_pid = StylusSchema.InvPreds[leaf_pid];
                            if (!processed.Contains(leaf_name))
                            {
                                var orig_leaf_card = local_card[leaf_name];
                                var new_leaf_card = statistics.EstimateLeafCard(min_node_card, node_pids[min_node], leaf_pid);
                                new_leaf_card = new_leaf_card * statistics.EstimateRootCard(node_pids[leaf_name]) /
                                    statistics.EstimateRootCard(new List<long>() { rev_leaf_pid });
                                if (new_leaf_card < orig_leaf_card)
                                {
                                    local_card[leaf_name] = new_leaf_card;
                                }
                            }
                        }

                        roots.Add(min_node);
                        processed.Add(min_node);
                    }

                    double cur_cost = 1.0;
                    foreach (var root in roots)
                    {
                        cur_cost += local_card[root];
                    }

                    if (cur_cost < min_cost)
                    {
                        min_cost = cur_cost;
                        min_roots = roots;
                    }
                }
            }

            return min_roots;
        }

        private List<xTwigHead> Decompose(QueryGraph qg, List<string> root_order,
            Dictionary<string, Binding> bindings)
        {
            HashSet<QueryEdge> query_edges = new HashSet<QueryEdge>(qg.Edges);

            List<xTwigHead> heads = new List<xTwigHead>();

            foreach (var root in root_order)
            {
                xTwigHead head = new xTwigHead();
                var node = qg.NameToNodes[root];
                head.Root = root;
                head.LeavePreds = node.GetStarShapePredIncludingSyn();
                head.SelectLeaves = new List<Tuple<string, string>>();
                head.Bindings = new Dictionary<string, Binding>();

                foreach (var edge in node.ParticipatedEdges)
                {
                    if (query_edges.Contains(edge))
                    {
                        QueryNode tgt_node = edge.SrcNode == node ? edge.TgtNode : edge.SrcNode;
                        string label = edge.SrcNode == node ? edge.Predicate : "_" + edge.Predicate;
                        if (!StylusSchema.IsSynPred(label, tgt_node.Name)) // && the edge is not for synthetic pred
                        {
                            if (qg.ToSelect(tgt_node) || !tgt_node.IsVariable)
                            {
                                //head.SelectLeaves.Add(label, tgt_node.Name);
                                head.SelectLeaves.Add(Tuple.Create(label, tgt_node.Name));
                            }
                        }

                        query_edges.Remove(edge);
                    }
                }

                if (head.SelectLeaves.Count > 0 || root_order.Count == 1)
                {
                    head.Bindings.Add(root, bindings[root]);
                    foreach (var leaf in head.SelectLeaves)
                    {
                        head.Bindings.Add(leaf.Item2, bindings[leaf.Item2]);
                    }
                    heads.Add(head);
                }
            }

            return heads;
        }
        #endregion

        #region Query Execution
        private void ClusterExec(Action<int> action) 
        {
            Parallel.For(0, TrinityConfig.Servers.Count, i => {
                action(i);
            });
        }

        private Statistics ClusterAggregateStatistics()
        {
            Statistics cluster_stat = new Statistics(new Dictionary<ushort, Dictionary<long, double>>());
            ClusterExec(i => 
            {
                var local_stat = Global.CloudStorage.AggregateStatInfoToSparqlDataServer(i);
                lock (cluster_stat)
                {
                    cluster_stat.UpdateFromCluster(local_stat);
                }
            });
            cluster_stat.Divide((double)TrinityConfig.Servers.Count);
            return cluster_stat;
        }

        private void ClusterIssueQuery(int query_id, List<xTwigHead> heads) 
        {
            List<TwigQuery> qs = heads.Select(h => PrepareDispatchTwig(h)).ToList();
            PreparedQueryWriter msg = new PreparedQueryWriter(query_id, qs);
            ClusterExec(i => Global.CloudStorage.IssueQueryToSparqlDataServer(i, msg));
        }

        private TwigQuery PrepareDispatchTwig(xTwigHead head) 
        {
            TwigQuery q = new TwigQuery();
            q.Root = head.Root;
            q.Leaves = head.LeavePreds;
            q.SelectLeavePreds = new List<string>();
            q.SelectLeaveVars = new List<string>();
            foreach (var tuple in head.SelectLeaves)
            {
                q.SelectLeavePreds.Add(tuple.Item1);
                q.SelectLeaveVars.Add(tuple.Item2);
            }
            q.Bindings = new List<BindingMessage>();
            foreach (var kvp in head.Bindings)
            {
                BindingMessage bm = new BindingMessage();
                bm.Name = kvp.Key;
                bm.Types = new List<ushort>();
                bm.Values = new List<long>();
                if (kvp.Value is UniEidBinding)
                {
                    UniEidBinding b = kvp.Value as UniEidBinding;
                    bm.Values.Add(b.Id);
                }
                else if (kvp.Value is TidBinding)
                {
                    TidBinding b = kvp.Value as TidBinding;
                    bm.Types.AddRange(b.Tids);
                }
                else
                {
                    throw new Exception("Not supported bindings in query parsing.");
                }
                q.Bindings.Add(bm);
            }

            return q;
        }

        private void ClusterExecuteQueryStep(int query_id, int step_index) 
        {
            ClusterExec(i => {
                var msg = new QueryStepInfoWriter(query_id, step_index);
                Global.CloudStorage.ExecuteQueryStepToSparqlDataServer(i, msg);
            });
        }

        private void ClusterSyncBinding(int query_id, int step_index) 
        {
            ClusterExec(i => {
                var msg = new QueryStepInfoWriter(query_id, step_index);
                Global.CloudStorage.SyncStepToSparqlDataServer(i, msg);
            });
        }

        private void ClusterFinishSyncStep(int query_id, int step_index) 
        {
            ClusterExec(i =>
            {
                var msg = new QueryStepInfoWriter(query_id, step_index);
                Global.CloudStorage.FinishSyncStepToSparqlDataServer(i, msg);
            });
        }

        private TwigAnswers[] ClusterAggregateQueryResults(int query_id, int step_cnt) 
        {
            TwigAnswers[] global_ans = new TwigAnswers[step_cnt];

            List<TwigAnswer>[] tas_list = new List<TwigAnswer>[step_cnt];
            long[] actual_sizes = new long[step_cnt];
            long[] twig_counts = new long[step_cnt];

            for (int s = 0; s < step_cnt; s++)
            {
                tas_list[s] = new List<TwigAnswer>();
                actual_sizes[s] = 0;
                twig_counts[s] = 0;
            }

            ClusterExec(i =>
            {
                var msg = new QueryInfoWriter(query_id);
                var result = (QueryAnswers)Global.CloudStorage.AggregateQueryAnswersToSparqlDataServer(i, msg);
                List<TwigAnswers> ans = result.Results;
                for (int s = 0; s < step_cnt; s++)
                {
                    lock (tas_list[s])
                    {
                        actual_sizes[s] += ans[s].ActualSize;
                        twig_counts[s] += ans[s].TwigCount;
                        tas_list[s].AddRange(ans[s].Elements);
                    }
                }
            });

            for (int s = 0; s < step_cnt; s++)
            {
                global_ans[s] = new TwigAnswers(actual_sizes[s], twig_counts[s], tas_list[s]);
            }
            return global_ans;
        }

        private void ClusterFreeQuery(int query_id) 
        {
            ClusterExec(i =>
            {
                var msg = new QueryInfoWriter(query_id);
                Global.CloudStorage.FreeQueryToSparqlDataServer(i, msg);
            });
        }

        private void ClusterBroadcastSuffixQueryAnswers(int query_id) 
        {
            ClusterExec(i =>
            {
                var msg = new QueryInfoWriter(query_id);
                Global.CloudStorage.BroadcastSuffixQueryAnswersToSparqlDataServer(i, msg);
            });
        }

        private void ClusterJoinQueryAnswers(int query_id) 
        {
            ClusterExec(i =>
            {
                var msg = new QueryInfoWriter(query_id);
                Global.CloudStorage.JoinQueryAnswersToSparqlDataServer(i, msg);
            });
        }

        private QuerySolutions ClusterAggregateQueryResults(int query_id) 
        {
            QuerySolutions qs = new QuerySolutions();
            ClusterExec(i =>
            {
                var msg = new QueryInfoWriter(query_id);
                var results = Global.CloudStorage.AggregateQueryResultsToSparqlDataServer(i, msg);
                lock (qs)
                {
                    qs.Heads = (List<string>)results.Variables;
                    results.Records.ForEach(r => qs.Records.Add(r.ToArray()));
                }
            });
            return qs;
        }

        private List<long> ClusterAggregateIdList(int query_id)
        {
            List<long> qs = new List<long>();
            ClusterExec(i =>
            {
                var msg = new QueryInfoWriter(query_id);
                var results = Global.CloudStorage.AggregateIdListToSparqlDataServer(i, msg);
                lock (qs)
                {
                    qs.AddRange((List<long>)results.Ids);
                }
            });
            return qs;
        }
        #endregion

        #region Query Post-Processing
        private QuerySolutions PruneFlatten(xTwigHead head, List<TwigAnswer> answers)
        {
            QuerySolutions global_ans = new QuerySolutions();
            global_ans.Heads.Add(head.Root);
            global_ans.Heads.AddRange(head.SelectLeaves.Select(l => l.Item2));

            foreach (var ans in answers)
            {
                QuerySolutions local_ans = new QuerySolutions();
                local_ans.Heads = new List<string>() { head.Root };
                local_ans.Records = new List<long[]>() { new long[] { ans.Root } };
                for (int i = 0; i < head.SelectLeaves.Count; i++)
                {
                    var leaf = head.SelectLeaves[i].Item2;
                    local_ans.Product(leaf, ans.LeaveValues[i]);
                }
                global_ans.Records.AddRange(local_ans.Records);
            }
            return global_ans;
        }

        private QuerySolutions FinalJoin(List<QuerySolutions> intermediate_results)
        {
            QuerySolutions results = null;
            for (int i = 0; i < intermediate_results.Count; i++)
            {
                if (i == 0)
                {
                    results = intermediate_results[i];
                }
                else
                {
                    results = results.Join(intermediate_results[i]);
                }
            }
            return results;
        }
        #endregion
    }
}
