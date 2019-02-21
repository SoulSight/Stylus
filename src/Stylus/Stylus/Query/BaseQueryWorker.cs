using Stylus.DataModel;
using Stylus.Parsing;
using Stylus.Util;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Stylus.Query
{
    public abstract class BaseQueryWorker : BaseQueryServer, IQueryWorker
    {
        public BaseQueryWorker() : base() { }

        #region IQueryWorker
        public List<xTwigHead> Plan(QueryGraph qg)
        {
            Dictionary<string, Binding> bindings;
            //var root_order = RootOrderSelection0(qg, CardStatistics, out bindings);
            //var root_order = RootOrderSelection1(qg, CardStatistics, out bindings);
            //var root_order = RootOrderSelection2(qg, CardStatistics, out bindings);
            //var root_order = RootOrderSelection3(qg, CardStatistics, out bindings); 
            var root_order = RootOrderSelection4(qg, CardStatistics, out bindings);
            return Decompose(qg, root_order, bindings);
        }

        // 0 strategy: first choose the node with minimum cardinality, then flooding the query graph
        private List<string> RootOrderSelection0(QueryGraph qg, Statistics statistics,
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
                    bindings.Add(node_name, new TidBinding(Storage.GetUDTs(pids)));
                }
                else
                {
                    bindings.Add(node_name, new UniEidBinding(GetEid(node_name)));
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
                        nodes_to_mark.Add(src_node);
                        mark_tgt = true;
                    }
                }

                if (mark_src && mark_tgt)
                {
                    edges_to_mark.Add(qe);
                }
            }

            double min_card = double.MaxValue;
            string min_node_name = null;
            foreach (var node in nodes_to_mark)
            {
                string node_name = node.Name;
                var card = node_cardinality[node_name];
                if (card < min_card)
                {
                    card = min_card;
                    min_node_name = node_name;
                }
            }

            // Update superiors of each node
            HashSet<string> marked_node_names = new HashSet<string>() { min_node_name };
            while (marked_node_names.Count < nodes_to_mark.Count)
            {
                HashSet<string> new_marked_nodes = new HashSet<string>();
                foreach (var qe in edges_to_mark)
                {
                    string src_name = qe.SrcNode.Name;
                    string tgt_name = qe.TgtNode.Name;
                    if (marked_node_names.Contains(src_name) && !marked_node_names.Contains(tgt_name))
                    {
                        var src_card = node_cardinality[src_name];
                        var leaf_pid = StylusSchema.Pred2Pid[qe.Predicate];

                        var orig_tgt_card = node_cardinality[tgt_name];
                        var new_tgt_card = statistics.EstimateLeafCard(src_card, node_pids[src_name], leaf_pid);

                        if (new_tgt_card < orig_tgt_card)
                        {
                            superiors[tgt_name] = src_name;
                        }
                        new_marked_nodes.Add(tgt_name);
                    }
                    else if (!marked_node_names.Contains(src_name) && marked_node_names.Contains(tgt_name))
                    {
                        var tgt_card = node_cardinality[tgt_name];
                        var leaf_pid = StylusSchema.Pred2Pid["_" + qe.Predicate];

                        var orig_src_card = node_cardinality[src_name];
                        var new_src_card = statistics.EstimateLeafCard(tgt_card, node_pids[tgt_name], leaf_pid);

                        if (new_src_card < orig_src_card)
                        {
                            superiors[src_name] = tgt_name;
                        }
                        new_marked_nodes.Add(src_name);
                    }
                }
                foreach (var item in new_marked_nodes)
                {
                    marked_node_names.Add(item);
                }
            }

            if (nodes_to_mark.Count == 1)
            {
                return new List<string>() { nodes_to_mark.ToArray()[0].Name };
            }

            // Select node order
            List<string> roots = new List<string>();

            //HashSet<QueryEdge> query_edges = new HashSet<QueryEdge>(qg.Edges);
            HashSet<string> current = new HashSet<string>(
                qg.NameToNodes.Where(kvp => nodes_to_mark.Contains(kvp.Value) && superiors[kvp.Key] == null).Select(kvp => kvp.Key));

            while (edges_to_mark.Count > 0)
            {
                HashSet<string> next = new HashSet<string>(
                    qg.NameToNodes.Where(kvp =>
                        current.Contains(superiors[kvp.Key])
                        && kvp.Value.ParticipatedEdges.Any(pe => edges_to_mark.Contains(pe)))
                        .Select(kvp => kvp.Key));
                foreach (var root in current)
                {
                    roots.Add(root);
                    foreach (var pe in qg.NameToNodes[root].ParticipatedEdges)
                    {
                        edges_to_mark.Remove(pe);
                    }
                }
            }

            return roots;
        }

        // 1 strategy: each step choose a (v, u) pair with minimun cardinality sum
        private List<string> RootOrderSelection1(QueryGraph qg, Statistics statistics,
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
                    bindings.Add(node_name, new TidBinding(Storage.GetUDTs(pids)));
                }
                else
                {
                    bindings.Add(node_name, new UniEidBinding(GetEid(node_name)));
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
                        nodes_to_mark.Add(src_node);
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
            List<string> roots = new List<string>();

            HashSet<string> marked_node_names = new HashSet<string>();
            while (edges_to_mark.Count > 0)
            {
                double min_card_sum = double.MaxValue;
                double min_v_card = 0, min_u_card = 0; // for cardinality update
                string min_v = null, min_u = null;
                if (marked_node_names.Count == 0) // first step
                {
                    foreach (var qe in edges_to_mark)
                    {
                        string src_name = qe.SrcNode.Name;
                        string tgt_name = qe.TgtNode.Name;

                        double src_card = node_cardinality[src_name];
                        double tgt_card = node_cardinality[tgt_name];

                        // forward
                        var forward_leaf_pid = StylusSchema.Pred2Pid[qe.Predicate];
                        var forward_esti_tgt_card = statistics.EstimateLeafCard(src_card, node_pids[src_name], forward_leaf_pid);
                        double forward_min_tgt_card = Math.Min(tgt_card, forward_esti_tgt_card);

                        double forward_sum = src_card + forward_min_tgt_card;

                        // backward
                        var backward_leaf_pid = StylusSchema.Pred2Pid["_" + qe.Predicate];
                        var backward_esti_src_card = statistics.EstimateLeafCard(tgt_card, node_pids[tgt_name], backward_leaf_pid);
                        double backward_min_src_card = Math.Min(src_card, backward_esti_src_card);

                        double backward_sum = backward_min_src_card + tgt_card;

                        // compare
                        if (forward_sum < backward_sum
                            && forward_sum < min_card_sum)
                        {
                            min_v = src_name;
                            min_u = tgt_name;

                            min_v_card = src_card;
                            min_u_card = forward_min_tgt_card;
                        }
                        else if (backward_sum < forward_sum
                            && backward_sum < min_card_sum)
                        {
                            min_v = tgt_name;
                            min_u = src_name;

                            min_v_card = tgt_card;
                            min_u_card = backward_min_src_card;
                        }
                    }

                    roots.Add(min_v);
                    node_cardinality[min_v] = min_v_card;
                    marked_node_names.Add(min_v);
                    foreach (var pe in qg.NameToNodes[min_v].ParticipatedEdges)
                    {
                        edges_to_mark.Remove(pe);
                    }

                    roots.Add(min_u);
                    node_cardinality[min_u] = min_u_card;
                    marked_node_names.Add(min_u);
                    foreach (var pe in qg.NameToNodes[min_u].ParticipatedEdges)
                    {
                        if (marked_node_names.Contains(pe.SrcNode.Name)
                            && marked_node_names.Contains(pe.TgtNode.Name))
                        {
                            edges_to_mark.Remove(pe);
                        }
                    }
                }
                else // later step
                {
                    foreach (var qe in edges_to_mark)
                    {
                        string src_name = qe.SrcNode.Name;
                        string tgt_name = qe.TgtNode.Name;

                        if (marked_node_names.Contains(src_name) && marked_node_names.Contains(tgt_name))
                        {
                            continue;
                        }
                        if (!marked_node_names.Contains(src_name) && !marked_node_names.Contains(tgt_name))
                        {
                            continue;
                        }

                        double src_card = node_cardinality[src_name];
                        double tgt_card = node_cardinality[tgt_name];

                        if (marked_node_names.Contains(src_name) && !marked_node_names.Contains(tgt_name))
                        {
                            // forward
                            var forward_leaf_pid = StylusSchema.Pred2Pid[qe.Predicate];
                            var forward_esti_tgt_card = statistics.EstimateLeafCard(src_card, node_pids[src_name], forward_leaf_pid);
                            double forward_min_tgt_card = Math.Min(tgt_card, forward_esti_tgt_card);

                            double forward_sum = src_card + forward_min_tgt_card;

                            if (forward_sum < min_card_sum)
                            {
                                min_v = src_name;
                                min_u = tgt_name;

                                min_v_card = src_card;
                                min_u_card = forward_min_tgt_card;
                            }
                        }
                        else if (!marked_node_names.Contains(src_name) && marked_node_names.Contains(tgt_name))
                        {
                            // backward
                            var backward_leaf_pid = StylusSchema.Pred2Pid["_" + qe.Predicate];
                            var backward_esti_src_card = statistics.EstimateLeafCard(tgt_card, node_pids[tgt_name], backward_leaf_pid);
                            double backward_min_src_card = Math.Min(src_card, backward_esti_src_card);

                            double backward_sum = backward_min_src_card + tgt_card;

                            if (backward_sum < min_card_sum)
                            {
                                min_v = tgt_name;
                                min_u = src_name;

                                min_v_card = tgt_card;
                                min_u_card = backward_min_src_card;
                            }
                        }
                    }

                    roots.Add(min_u);
                    node_cardinality[min_u] = min_u_card;
                    marked_node_names.Add(min_u);
                    foreach (var pe in qg.NameToNodes[min_u].ParticipatedEdges)
                    {
                        if (marked_node_names.Contains(pe.SrcNode.Name)
                            && marked_node_names.Contains(pe.TgtNode.Name))
                        {
                            edges_to_mark.Remove(pe);
                        }
                    }
                }
            }

            return roots;
        }

        // 2 strategy: multiply the cardinalities of the query nodes
        private List<string> RootOrderSelection2(QueryGraph qg, Statistics statistics,
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
                    bindings.Add(node_name, new TidBinding(Storage.GetUDTs(pids)));
                }
                else
                {
                    bindings.Add(node_name, new UniEidBinding(GetEid(node_name)));
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
                foreach (var root in roots)
                {
                    double root_card = local_card[root];
                    var root_pids = node_pids[root];

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
                        var orig_leaf_card = local_card[leaf_name];
                        var new_leaf_card = statistics.EstimateLeafCard(root_card, root_pids, leaf_pid);

                        long rev_leaf_pid = StylusSchema.InvPreds[leaf_pid];
                        new_leaf_card = new_leaf_card * statistics.EstimateRootCard(node_pids[leaf_name]) /
                            statistics.EstimateRootCard(new List<long>() { rev_leaf_pid });

                        if (new_leaf_card < orig_leaf_card)
                        {
                            local_card[leaf_name] = new_leaf_card;
                        }
                    }
                }

                double cur_cost = 1.0;
                foreach (var root in roots)
                {
                    cur_cost *= local_card[root];
                }
                //Console.WriteLine(string.Join(", ", roots) + ": " + cur_cost);

                if (cur_cost < min_cost)
                {
                    min_cost = cur_cost;
                    min_roots = roots;
                }
            }
            return min_roots;
        }

        // 3 strategy: add all the cardinalities of the query nodes
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
                    bindings.Add(node_name, new TidBinding(Storage.GetUDTs(pids)));
                }
                else
                {
                    bindings.Add(node_name, new UniEidBinding(GetEid(node_name)));
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
                // Console.WriteLine("Calculating order: " + string.Join(", ", roots)); // debug
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
            return min_roots;
        }

        // 4 strategy: small node set = permutation; large node set = foreach first + greedy
        private List<string> RootOrderSelection4(QueryGraph qg, Statistics statistics,
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
                    bindings.Add(node_name, new TidBinding(Storage.GetUDTs(pids)));
                }
                else
                {
                    bindings.Add(node_name, new UniEidBinding(GetEid(node_name)));
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

        public abstract List<xTwigAnswer> ExecuteToXTwigAnswer(xTwigHead head);

        public abstract TwigAnswers ExecuteToTwigAnswer(xTwigHead head);

        public abstract QuerySolutions ExecuteFlatten(xTwigHead head);

        public abstract QuerySolutions ExecuteSingleTwig(xTwigHead head);

        public abstract QuerySolutions Execute(List<xTwigHead> heads);
        #endregion
    }
}
