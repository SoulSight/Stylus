using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Trinity;

using Stylus.Storage;
using Stylus.Util;
using Stylus.DataModel;
using Stylus.Parsing;

namespace Stylus.Query
{
    public abstract class BaseQueryWorkerPlus : BaseQueryServer, IQueryWorkerPlus
    {
        public BaseQueryWorkerPlus() : base() { }

        #region IQueryWorkerPlus
        public List<xTwigPlusHead> PlanPlus(QueryGraph qg)
        {
            Dictionary<string, Binding> bindings;
            var root_order = RootOrderSelection(qg, CardStatistics, out bindings);
            return Decompose(qg, root_order, bindings);
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
                List<long> pids = kvp.Value.GetStarShapePredIncludingSyn()
                    .Where(str => !StringUtil.IsVar(str))
                    .Select(str => StylusSchema.Pred2Pid[str])
                    .ToList();
                node_pids.Add(node_name, pids);
                double card = kvp.Value.IsVariable ? statistics.EstimateRootCard(pids) : 1.0;
                node_cardinality.Add(node_name, card);

                if (kvp.Value.IsVariable)
                {
                    bindings.Add(node_name, new TidBinding(Storage.GetUDTs(pids)));
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

                            if (!StringUtil.IsVar(pe.Predicate)) // Non-variable predicate
                            {
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
                            else // Variable predicate
                            {
                                string leaf_name = null;
                                bool forward;
                                if (pe.SrcNode.Name == root)
                                {
                                    leaf_name = pe.TgtNode.Name;
                                    forward = true;
                                }
                                else
                                {
                                    leaf_name = pe.SrcNode.Name;
                                    forward = false;
                                }

                                if (!processed.Contains(leaf_name))
                                {
                                    if (!local_card.ContainsKey(leaf_name))
                                    {
                                        double new_leaf_card = root_card * (forward ? 10.0 : 0.1);
                                        local_card[leaf_name] = new_leaf_card;
                                    }
                                    else
                                    {
                                        // nothing
                                    }
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

                        if (!StringUtil.IsVar(pe.Predicate))  // Non-variable predicate
                        {
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
                        else  // Variable predicate
                        {
                            string leaf_name = null;
                            bool forward;
                            if (pe.SrcNode.Name == first_root)
                            {
                                leaf_name = pe.TgtNode.Name;
                                forward = true;
                            }
                            else
                            {
                                leaf_name = pe.SrcNode.Name;
                                forward = false;
                            }

                            if (!processed.Contains(leaf_name))
                            {
                                if (!local_card.ContainsKey(leaf_name))
                                {
                                    double new_leaf_card = local_card[first_root] * (forward ? 10.0 : 0.1);
                                    local_card[leaf_name] = new_leaf_card;
                                }
                                else
                                {
                                    // nothing
                                }
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

                            if (!StringUtil.IsVar(pe.Predicate))  // Non-variable predicate
                            {
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
                            else // Variable predicate
                            {
                                string leaf_name = null;
                                bool forward;
                                if (pe.SrcNode.Name == min_node)
                                {
                                    leaf_name = pe.TgtNode.Name;
                                    forward = true;
                                }
                                else
                                {
                                    leaf_name = pe.SrcNode.Name;
                                    forward = false;
                                }

                                if (!processed.Contains(leaf_name))
                                {
                                    if (!local_card.ContainsKey(leaf_name))
                                    {
                                        double new_leaf_card = local_card[min_node] * (forward ? 10.0 : 0.1);
                                        local_card[leaf_name] = new_leaf_card;
                                    }
                                    else
                                    {
                                        // nothing
                                    }
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

        private List<xTwigPlusHead> Decompose(QueryGraph qg, List<string> root_order,
            Dictionary<string, Binding> bindings)
        {
            HashSet<QueryEdge> query_edges = new HashSet<QueryEdge>(qg.Edges);

            var heads = new List<xTwigPlusHead>();

            foreach (var root in root_order)
            {
                var head = new xTwigPlusHead();
                var node = qg.NameToNodes[root];
                head.Root = root;
                head.LeavePreds = node.GetStarShapePredIncludingSyn();
                head.SelectVarPreds = new List<string>();
                head.SelectLeaves = new List<Tuple<string, string>>();
                head.Bindings = new Dictionary<string, Binding>();

                foreach (var edge in node.ParticipatedEdges)
                {
                    if (!query_edges.Contains(edge))
                    {
                        continue;
                    }

                    QueryNode tgt_node = edge.SrcNode == node ? edge.TgtNode : edge.SrcNode;
                    string label = edge.SrcNode == node ? edge.Predicate : "_" + edge.Predicate;
                    if (StringUtil.IsVar(edge.Predicate))
                    {
                        if (qg.ToSelectVarPred(edge.Predicate)) // could have two possible vars: "?x" and "_?x"
                        {
                            head.SelectVarPreds.Add(label);
                        }
                        head.SelectLeaves.Add(Tuple.Create(label, tgt_node.Name));
                    }
                    else
                    {
                        if (!StylusSchema.IsSynPred(label, tgt_node.Name)) // && the edge is not for synthetic pred
                        {
                            if (qg.ToSelect(tgt_node) || !tgt_node.IsVariable)
                            {
                                head.SelectLeaves.Add(Tuple.Create(label, tgt_node.Name));
                            }
                        }
                    }

                    query_edges.Remove(edge);
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

        public abstract List<xTwigPlusAnswer> ExecuteToXTwigAnswerPlus(xTwigPlusHead head);

        public abstract TwigAnswers ExecuteToTwigAnswer(xTwigPlusHead head);

        public abstract QuerySolutions ExecuteFlattenPlus(xTwigPlusHead head);

        public abstract QuerySolutions ExecuteSingleTwigPlus(xTwigPlusHead head);

        public abstract QuerySolutions ExecutePlus(List<xTwigPlusHead> heads);
        #endregion
    }
}
