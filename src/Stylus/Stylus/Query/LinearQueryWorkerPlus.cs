using Stylus.DataModel;
using Stylus.Util;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Stylus.Query
{
    public class LinearQueryWorkerPlus : BaseQueryWorkerPlus
    {
        #region Bindings
        private Dictionary<string, Binding> query_bindings = new Dictionary<string, Binding>();

        public override bool ContainsBinding(string variable)
        {
            return query_bindings.ContainsKey(variable);
        }

        public override IEnumerable<long> EnumerateBinding(string variable)
        {
            foreach (var tuple in EnumerateBindingValues(query_bindings[variable]))
            {
                foreach (var item in tuple.Item2)
                {
                    yield return item;
                }
            }
        }

        public override Binding GetBinding(string variable)
        {
            Binding b;
            if (query_bindings.TryGetValue(variable, out b))
            {
                return b;
            }
            return null;
        }

        public override void SetBinding(string variable, Binding binding)
        {
            query_bindings[variable] = binding;
        }

        public override void ReplaceBinding(string variable, IEnumerable<long> bindings)
        {
            query_bindings[variable] = new EidSetBinding(bindings);
        }

        public override void ReplaceBinding(string variable, Binding bindings)
        {
            query_bindings[variable] = bindings;
        }

        public override void AppendBinding(string variable, IEnumerable<long> bindings)
        {
            if (!query_bindings.ContainsKey(variable))
            {
                query_bindings[variable] = new EidSetBinding(bindings);
            }
            else
            {
                query_bindings[variable].AddEids(bindings);
            }
        }

        private IEnumerable<Tuple<ushort, ICollection<long>>> EnumerateBindingValues(Binding binding)
        {
            if (binding is UniEidBinding)
            {
                long id = (binding as UniEidBinding).Id;
                ushort tid = TidUtil.GetTid(id);
                ICollection<long> values = new List<long>() { id };
                yield return Tuple.Create(tid, values);
            }
            else if (binding is TidBinding)
            {
                var tids = (binding as TidBinding).Tids;
                foreach (var tid in tids)
                {
                    ICollection<long> values = this.Storage.LoadEids(tid);
                    yield return Tuple.Create(tid, values);
                }
            }
            else if (binding is EidSetBinding)
            {
                EidSetBinding esbinding = (binding as EidSetBinding);
                foreach (var kvp in esbinding)
                {
                    ICollection<long> values = kvp.Value;
                    yield return Tuple.Create(kvp.Key, values);
                }
            }
            else
            {
                throw new NotSupportedException();
            }
        }
        #endregion

        #region BaseQueryWorkerPlus
        public override List<xTwigPlusAnswer> ExecuteToXTwigAnswerPlus(xTwigPlusHead head)
        {
            var root = head.Root;
            var selected_leaf_pairs = head.SelectLeaves;
            var selected_var_pred_set = new HashSet<string>(head.SelectVarPreds);
            var selected_leaves = new List<Tuple<string, string>>();

            // is_select_predicate, is_reverse, pred, obj, p_binding, o_binding
            List<bool> is_select_preds = new List<bool>();
            List<bool> is_reverse_s = new List<bool>();
            List<Tuple<string, string>> var_pred_objs = new List<Tuple<string, string>>();
            List<Tuple<Binding, Binding>> var_pred_bindings = new List<Tuple<Binding, Binding>>();
            List<Tuple<Binding, Binding>> new_var_pred_bindings = new List<Tuple<Binding, Binding>>();

            foreach (var kvp in head.SelectLeaves)
            {
                if (StringUtil.IsVar(kvp.Item1))
                {
                    string var_p = kvp.Item1;
                    string var_o = kvp.Item2;
                    is_select_preds.Add(selected_var_pred_set.Contains(var_p));
                    is_reverse_s.Add(var_p.StartsWith("_"));
                    var_p = var_p.StartsWith("_") ? var_p.Substring(1) : var_p;
                    var p_binding = query_bindings.ContainsKey(var_p) ? query_bindings[var_p] : null;
                    var o_binding = query_bindings.ContainsKey(var_o) ? query_bindings[var_o] : null;
                    var_pred_objs.Add(new Tuple<string, string>(var_p, var_o));
                    var_pred_bindings.Add(new Tuple<Binding, Binding>(p_binding, o_binding));
                    new_var_pred_bindings.Add(new Tuple<Binding, Binding>(new EidSetBinding(), 
                        new EidSetBinding()));
                }
                else
                {
                    selected_leaves.Add(kvp);
                }
            }

            int var_pred_len = is_select_preds.Count;

            List<long> pids = new List<long>();
            List<Binding> filter_bindings = new List<Binding>();
            List<Binding> new_bindings = new List<Binding>();
            Binding new_root_binding = new EidSetBinding();

            foreach (var kvp in selected_leaves)
            {
                pids.Add(StylusSchema.Pred2Pid[kvp.Item1]);
                filter_bindings.Add(query_bindings[kvp.Item2]);
                new_bindings.Add(new EidSetBinding());
            }

            List<xTwigPlusAnswer> results = new List<xTwigPlusAnswer>();

            Binding root_binding = query_bindings[root];
            var root_candidates = EnumerateBindingValues(root_binding);
            foreach (var tid_eids in root_candidates)
            {
                ushort tid = tid_eids.Item1;
                var eids = tid_eids.Item2;
                if (tid == StylusConfig.GenericTid)
                {
                    foreach (var root_eid in eids)
                    {
                        List<long[]> select_objs = Storage.SelectObjects(root_eid, pids, filter_bindings);

                        // matching var pred
                        var var_pred_leaves = new List<KeyValuePair<long, List<long>>[]>();
                        for (int i = 0; i < var_pred_len; i++)
                        {
                            var kvp_list = new List<KeyValuePair<long, List<long>>>();
                            var p_binding = var_pred_bindings[i].Item1;
                            var o_binding = var_pred_bindings[i].Item2;
                            foreach (var pid in StylusSchema.GenericInclusivePids)
                            {
                                if (p_binding == null || p_binding.ContainEid(pid))
                                {
                                    var list = Storage.SelectObjectToList(root_eid, pid, o_binding);
                                    if (list.Count == 0)
                                    {
                                        continue;
                                    }
                                    if (is_reverse_s[i])
                                    {
                                        long rev_pid = StylusSchema.InvPreds[pid];
                                        kvp_list.Add(new KeyValuePair<long, List<long>>(rev_pid, list));
                                    }
                                    else
                                    {
                                        kvp_list.Add(new KeyValuePair<long, List<long>>(pid, list));
                                    }
                                }
                            }
                            var_pred_leaves.Add(kvp_list.ToArray());
                        }

                        xTwigPlusAnswer ans = new xTwigPlusAnswer();
                        ans.Head = head;
                        ans.Root = root_eid;
                        ans.Leaves = select_objs;
                        ans.VarPredLeaves = var_pred_leaves;

                        if (!ans.IsValid())
                        {
                            continue;
                        }

                        results.Add(ans);

                        // prepare updating bindings
                        new_root_binding.AddEid(root_eid);
                        for (int i = 0; i < pids.Count; i++)
                        {
                            foreach (var x in ans.Leaves[i])
                            {
                                new_bindings[i].AddEid(x);
                            }
                        }

                        // update bindings for var preds
                        for (int i = 0; i < var_pred_len; i++)
                        {
                            if (is_select_preds[i])
                            {
                                foreach (var x in ans.VarPredLeaves[i])
                                {
                                    new_var_pred_bindings[i].Item1.AddEid(x.Key);
                                    new_var_pred_bindings[i].Item2.AddEids(x.Value);
                                }
                            }
                            else
                            {
                                foreach (var x in ans.VarPredLeaves[i])
                                {
                                    new_var_pred_bindings[i].Item2.AddEids(x.Value);
                                }
                            }
                        }
                    }
                }
                else
                {
                    List<int> offsets = pids.Select(pid => StylusSchema.TidPid2Index[tid][pid]).ToList();
                    var tid_all_pids = StylusSchema.Tid2Pids[tid];

                    foreach (var root_eid in eids)
                    {
                        List<long[]> select_objs = Storage.SelectOffsets(root_eid, offsets, filter_bindings);
                        // matching var pred
                        var var_pred_leaves = new List<KeyValuePair<long, List<long>>[]>();
                        for (int i = 0; i < var_pred_len; i++)
                        {
                            var kvp_list = new List<KeyValuePair<long, List<long>>>();
                            var p_binding = var_pred_bindings[i].Item1;
                            var o_binding = var_pred_bindings[i].Item2;
                            foreach (var pid in tid_all_pids)
                            {
                                if (StylusSchema.Synpid2PidOid.ContainsKey(pid))
                                {
                                    var orig_tuple = StylusSchema.Synpid2PidOid[pid];
                                    long orig_pid = orig_tuple.Item1;
                                    long orig_oid = orig_tuple.Item2; // the short oid, not the oid in the storage
                                    long oid = StylusSchema.SchemaSynOid2Oid[orig_oid];
                                    if (o_binding != null && !o_binding.ContainEid(oid))
                                    {
                                        continue;
                                    }
                                    var oid_list = new List<long>() { oid };
                                    if (is_reverse_s[i])
                                    {
                                        long rev_pid = StylusSchema.InvPreds[orig_pid];
                                        kvp_list.Add(new KeyValuePair<long, List<long>>(rev_pid, oid_list));
                                    }
                                    else
                                    {
                                        kvp_list.Add(new KeyValuePair<long, List<long>>(orig_pid, oid_list));
                                    }
                                }
                                else
                                {
                                    if (p_binding == null || p_binding.ContainEid(pid))
                                    {
                                        var list = Storage.SelectObjectToList(root_eid, pid, o_binding);
                                        if (list.Count == 0)
                                        {
                                            continue;
                                        }
                                        if (is_reverse_s[i])
                                        {
                                            if (!StylusSchema.InvPreds.ContainsKey(pid))
                                            {
                                                continue;
                                            }
                                            long rev_pid = StylusSchema.InvPreds[pid];
                                            kvp_list.Add(new KeyValuePair<long, List<long>>(rev_pid, list));
                                        }
                                        else
                                        {
                                            kvp_list.Add(new KeyValuePair<long, List<long>>(pid, list));
                                        }
                                    }
                                }
                            }
                            var_pred_leaves.Add(kvp_list.ToArray());
                        }

                        xTwigPlusAnswer ans = new xTwigPlusAnswer();
                        ans.Head = head;
                        ans.Root = root_eid;
                        ans.Leaves = select_objs;
                        ans.VarPredLeaves = var_pred_leaves;

                        if (!ans.IsValid())
                        {
                            continue;
                        }

                        results.Add(ans);

                        // prepare updating bindings
                        new_root_binding.AddEid(root_eid);
                        for (int i = 0; i < pids.Count; i++)
                        {
                            foreach (var x in ans.Leaves[i])
                            {
                                new_bindings[i].AddEid(x);
                            }
                        }

                        // update bindings for var preds
                        for (int i = 0; i < var_pred_len; i++)
                        {
                            if (is_select_preds[i])
                            {
                                foreach (var x in ans.VarPredLeaves[i])
                                {
                                    new_var_pred_bindings[i].Item1.AddEid(x.Key);
                                    new_var_pred_bindings[i].Item2.AddEids(x.Value);
                                }
                            }
                            else
                            {
                                foreach (var x in ans.VarPredLeaves[i])
                                {
                                    new_var_pred_bindings[i].Item2.AddEids(x.Value);
                                }
                            }
                        }
                    }
                }
            }

            // update query bindings
            this.query_bindings[root] = new_root_binding;
            for (int i = 0; i < selected_leaves.Count; i++)
            {
                var sel_var = selected_leaves[i].Item2;
                this.query_bindings[sel_var] = new_bindings[i];
            }

            for (int i = 0; i < var_pred_len; i++)
            {
                if (is_select_preds[i])
                {
                    string pred = var_pred_objs[i].Item1;
                    this.query_bindings[pred] = new_var_pred_bindings[i].Item1;
                }
                string obj = var_pred_objs[i].Item2;
                this.query_bindings[obj] = new_var_pred_bindings[i].Item2;
            }

            return results;
        }

        public override TwigAnswers ExecuteToTwigAnswer(xTwigPlusHead head)
        {
            var root = head.Root;
            var selected_leaf_pairs = head.SelectLeaves;
            var selected_var_pred_set = new HashSet<string>(head.SelectVarPreds);
            var selected_leaves = new List<Tuple<string, string>>();

            // is_select_predicate, is_reverse, pred, obj, p_binding, o_binding
            List<bool> is_select_preds = new List<bool>();
            List<bool> is_reverse_s = new List<bool>();
            List<Tuple<string, string>> var_pred_objs = new List<Tuple<string, string>>();
            List<Tuple<Binding, Binding>> var_pred_bindings = new List<Tuple<Binding, Binding>>();
            List<Tuple<Binding, Binding>> new_var_pred_bindings = new List<Tuple<Binding, Binding>>();

            foreach (var kvp in head.SelectLeaves)
            {
                if (StringUtil.IsVar(kvp.Item1))
                {
                    string var_p = kvp.Item1;
                    string var_o = kvp.Item2;
                    is_select_preds.Add(selected_var_pred_set.Contains(var_p));
                    is_reverse_s.Add(var_p.StartsWith("_"));
                    var_p = var_p.StartsWith("_") ? var_p.Substring(1) : var_p;
                    var p_binding = query_bindings.ContainsKey(var_p) ? query_bindings[var_p] : null;
                    var o_binding = query_bindings.ContainsKey(var_o) ? query_bindings[var_o] : null;
                    var_pred_objs.Add(new Tuple<string, string>(var_p, var_o));
                    var_pred_bindings.Add(new Tuple<Binding, Binding>(p_binding, o_binding));
                    new_var_pred_bindings.Add(new Tuple<Binding, Binding>(new EidSetBinding(),
                        new EidSetBinding()));
                }
                else
                {
                    selected_leaves.Add(kvp);
                }
            }

            int var_pred_len = is_select_preds.Count;

            List<long> pids = new List<long>();
            List<Binding> filter_bindings = new List<Binding>();
            List<Binding> new_bindings = new List<Binding>();
            Binding new_root_binding = new EidSetBinding();

            foreach (var kvp in selected_leaves)
            {
                pids.Add(StylusSchema.Pred2Pid[kvp.Item1]);
                filter_bindings.Add(query_bindings[kvp.Item2]);
                new_bindings.Add(new EidSetBinding());
            }

            List<TwigAnswer> results = new List<TwigAnswer>();
            long actual_size = 0;
            long twig_count = 0;

            Binding root_binding = query_bindings[root];
            var root_candidates = EnumerateBindingValues(root_binding);
            foreach (var tid_eids in root_candidates)
            {
                ushort tid = tid_eids.Item1;
                var eids = tid_eids.Item2;
                if (tid == StylusConfig.GenericTid)
                {
                    foreach (var root_eid in eids)
                    {
                        List<List<long>> select_objs = Storage.SelectObjectsToList(root_eid, pids, filter_bindings);

                        // matching var pred
                        var var_pred_leaves = new List<List<VarPredMatch>>();
                        for (int i = 0; i < var_pred_len; i++)
                        {
                            var var_pred_match_list = new List<VarPredMatch>();
                            var p_binding = var_pred_bindings[i].Item1;
                            var o_binding = var_pred_bindings[i].Item2;
                            foreach (var pid in StylusSchema.GenericInclusivePids)
                            {
                                if (p_binding == null || p_binding.ContainEid(pid))
                                {
                                    var list = Storage.SelectObjectToList(root_eid, pid, o_binding);
                                    if (list.Count == 0)
                                    {
                                        continue;
                                    }
                                    if (is_reverse_s[i])
                                    {
                                        long rev_pid = StylusSchema.InvPreds[pid];
                                        var_pred_match_list.Add(new KeyValuePair<long, List<long>>(rev_pid, list));
                                    }
                                    else
                                    {
                                        var_pred_match_list.Add(new KeyValuePair<long, List<long>>(pid, list));
                                    }
                                }
                            }
                            var_pred_leaves.Add(var_pred_match_list);
                        }

                        TwigAnswer ans = new TwigAnswer();
                        ans.Root = root_eid;
                        ans.LeaveValues = select_objs;
                        ans.LeaveVarValues = var_pred_leaves;

                        results.Add(ans);
                        twig_count++;
                        long local_actual_size = 1;
                        foreach (var l in select_objs)
                        {
                            local_actual_size *= l.Count;
                        }
                        foreach (var l in var_pred_leaves)
                        {
                            long leaf_cnt = 0;
                            foreach (var m in l)
                            {
                                leaf_cnt += m.Oids.Count;
                            }
                            local_actual_size *= leaf_cnt;
                        }
                        actual_size += local_actual_size;

                        // prepare updating bindings
                        new_root_binding.AddEid(root_eid);
                        for (int i = 0; i < pids.Count; i++)
                        {
                            foreach (var x in ans.LeaveValues[i])
                            {
                                new_bindings[i].AddEid(x);
                            }
                        }

                        // update bindings for var preds
                        for (int i = 0; i < var_pred_len; i++)
                        {
                            if (is_select_preds[i])
                            {
                                foreach (var x in ans.LeaveVarValues[i])
                                {
                                    new_var_pred_bindings[i].Item1.AddEid(x.Pid);
                                    new_var_pred_bindings[i].Item2.AddEids(x.Oids);
                                }
                            }
                            else
                            {
                                foreach (var x in ans.LeaveVarValues[i])
                                {
                                    new_var_pred_bindings[i].Item2.AddEids(x.Oids);
                                }
                            }
                        }
                    }
                }
                else
                {
                    List<int> offsets = pids.Select(pid => StylusSchema.TidPid2Index[tid][pid]).ToList();
                    var tid_all_pids = StylusSchema.Tid2Pids[tid];

                    foreach (var root_eid in eids)
                    {
                        List<List<long>> select_objs = Storage.SelectObjectsToList(root_eid, pids, filter_bindings);

                        // matching var pred
                        var var_pred_leaves = new List<List<VarPredMatch>>();
                        for (int i = 0; i < var_pred_len; i++)
                        {
                            var var_pred_match_list = new List<VarPredMatch>();
                            var p_binding = var_pred_bindings[i].Item1;
                            var o_binding = var_pred_bindings[i].Item2;
                            foreach (var pid in tid_all_pids)
                            {
                                if (StylusSchema.Synpid2PidOid.ContainsKey(pid))
                                {
                                    var orig_tuple = StylusSchema.Synpid2PidOid[pid];
                                    long orig_pid = orig_tuple.Item1;
                                    long orig_oid = orig_tuple.Item2;

                                    long oid = StylusSchema.SchemaSynOid2Oid[orig_oid];
                                    if (o_binding != null && !o_binding.ContainEid(oid))
                                    {
                                        continue;
                                    }
                                    var oid_list = new List<long>() { oid };

                                    if (is_reverse_s[i])
                                    {
                                        long rev_pid = StylusSchema.InvPreds[orig_pid];
                                        var_pred_match_list.Add(new KeyValuePair<long, List<long>>(rev_pid, oid_list));
                                    }
                                    else
                                    {
                                        var_pred_match_list.Add(new KeyValuePair<long, List<long>>(orig_pid, oid_list));
                                    }
                                }
                                else
                                {
                                    if (p_binding == null || p_binding.ContainEid(pid))
                                    {
                                        var list = Storage.SelectObjectToList(root_eid, pid, o_binding);
                                        if (list.Count == 0)
                                        {
                                            continue;
                                        }
                                        if (is_reverse_s[i])
                                        {
                                            long rev_pid = StylusSchema.InvPreds[pid];
                                            var_pred_match_list.Add(new KeyValuePair<long, List<long>>(rev_pid, list));
                                        }
                                        else
                                        {
                                            var_pred_match_list.Add(new KeyValuePair<long, List<long>>(pid, list));
                                        }
                                    }
                                }
                            }
                            var_pred_leaves.Add(var_pred_match_list);
                        }

                        TwigAnswer ans = new TwigAnswer();
                        ans.Root = root_eid;
                        ans.LeaveValues = select_objs;
                        ans.LeaveVarValues = var_pred_leaves;

                        results.Add(ans);
                        twig_count++;
                        long local_actual_size = 1;
                        foreach (var l in select_objs)
                        {
                            local_actual_size *= l.Count;
                        }
                        foreach (var l in var_pred_leaves)
                        {
                            long leaf_cnt = 0;
                            foreach (var m in l)
                            {
                                leaf_cnt += m.Oids.Count;
                            }
                            local_actual_size *= leaf_cnt;
                        }
                        actual_size += local_actual_size;

                        // prepare updating bindings
                        new_root_binding.AddEid(root_eid);
                        for (int i = 0; i < pids.Count; i++)
                        {
                            foreach (var x in ans.LeaveValues[i])
                            {
                                new_bindings[i].AddEid(x);
                            }
                        }

                        // update bindings for var preds
                        for (int i = 0; i < var_pred_len; i++)
                        {
                            if (is_select_preds[i])
                            {
                                foreach (var x in ans.LeaveVarValues[i])
                                {
                                    new_var_pred_bindings[i].Item1.AddEid(x.Pid);
                                    new_var_pred_bindings[i].Item2.AddEids(x.Oids);
                                }
                            }
                            else
                            {
                                foreach (var x in ans.LeaveVarValues[i])
                                {
                                    new_var_pred_bindings[i].Item2.AddEids(x.Oids);
                                }
                            }
                        }
                    }
                }
            }

            // update query bindings
            this.query_bindings[root] = new_root_binding;
            for (int i = 0; i < selected_leaves.Count; i++)
            {
                var sel_var = selected_leaves[i].Item2;
                this.query_bindings[sel_var] = new_bindings[i];
            }

            for (int i = 0; i < var_pred_len; i++)
            {
                if (is_select_preds[i])
                {
                    string pred = var_pred_objs[i].Item1;
                    this.query_bindings[pred] = new_var_pred_bindings[i].Item1;
                }
                string obj = var_pred_objs[i].Item2;
                this.query_bindings[obj] = new_var_pred_bindings[i].Item2;
            }

            return new TwigAnswers(actual_size, twig_count, results);
        }

        public override QuerySolutions ExecuteFlattenPlus(xTwigPlusHead head)
        {
            var results = ExecuteToXTwigAnswerPlus(head);
            return PruneFlattenPlus(head, results);
        }

        public override QuerySolutions ExecuteSingleTwigPlus(xTwigPlusHead head)
        {
            var root = head.Root;
            var selected_var_pred_set = new HashSet<string>(head.SelectVarPreds);
            var selected_leaf_pairs = head.SelectLeaves;
            List<long> pids = new List<long>();

            bool include_root = StringUtil.IsVar(head.Root);

            List<Binding> leaf_bindings = new List<Binding>();

            foreach (var kvp in selected_leaf_pairs)
            {
                string pred = kvp.Item1;
                if (!StringUtil.IsVar(pred))
                {
                    pids.Add(StylusSchema.Pred2Pid[pred]);
                }
                leaf_bindings.Add(query_bindings[kvp.Item2]);
            }
            Binding root_binding = query_bindings[root];
            var root_candidates = EnumerateBindingValues(root_binding);

            QuerySolutions results = new QuerySolutions();
            if (include_root)
            {
                results.Heads = new List<string>() { root };
            }
            else
            {
                results.Heads = new List<string>();
            }

            if (head.SelectLeaves.Count == 0)
            {
                foreach (var kvp in root_candidates)
                {
                    foreach (var item in kvp.Item2)
                    {
                        results.Records.Add(new long[] { item });
                    }
                }
                return results;
            }

            // is_select_predicate, is_reverse, pred, obj, p_binding, o_binding
            List<bool> is_select_preds = new List<bool>();
            List<bool> is_reverse_s = new List<bool>();
            List<Tuple<string, string>> var_pred_objs = new List<Tuple<string, string>>();
            List<Tuple<Binding, Binding>> var_pred_bindings = new List<Tuple<Binding, Binding>>();

            foreach (var kvp in head.SelectLeaves)
            {
                if (StringUtil.IsVar(kvp.Item1))
                {
                    string var_p = kvp.Item1;
                    string var_o = kvp.Item2;
                    is_select_preds.Add(selected_var_pred_set.Contains(var_p));
                    is_reverse_s.Add(var_p.StartsWith("_"));
                    var_p = var_p.StartsWith("_") ? var_p.Substring(1) : var_p;
                    var p_binding = query_bindings.ContainsKey(var_p) ? query_bindings[var_p] : null;
                    var o_binding = query_bindings.ContainsKey(var_o) ? query_bindings[var_o] : null;
                    var_pred_objs.Add(new Tuple<string, string>(var_p, var_o));
                    var_pred_bindings.Add(new Tuple<Binding, Binding>(p_binding, o_binding));
                }
                else
                {
                    results.Heads.Add(kvp.Item2);
                }
            }

            int var_pred_len = is_select_preds.Count;
            for (int i = 0; i < var_pred_len; i++)
            {
                if (is_select_preds[i])
                {
                    results.Heads.Add(var_pred_objs[i].Item1);
                }
                results.Heads.Add(var_pred_objs[i].Item2);
            }

            foreach (var tid_eids in root_candidates)
            {
                ushort tid = tid_eids.Item1;
                var eids = tid_eids.Item2;

                var tid_all_pids = StylusSchema.Tid2Pids[tid];

                List<int> offsets = new List<int>(pids.Count);
                foreach (var pid in pids)
                {
                    offsets.Add(StylusSchema.TidPid2Index[tid][pid]);
                }

                foreach (var root_eid in eids)
                {
                    // Non-variable predicates
                    List<long[]> select_objs = Storage.SelectOffsets(root_eid, offsets, leaf_bindings);
                    List<long[]> to_product = new List<long[]>();

                    if (include_root)
                    {
                        to_product.Add(new long[] { root_eid });
                    }
                    to_product.AddRange(select_objs);
                    var iter = ListUtil.Product(to_product);

                    // Variable predicates
                    for (int i = 0; i < var_pred_len; i++)
                    {
                        if (is_select_preds[i])
                        {
                            var kvp_list = new List<KeyValuePair<long, List<long>>>();
                            var p_binding = var_pred_bindings[i].Item1;
                            var o_binding = var_pred_bindings[i].Item2;
                            foreach (var pid in tid_all_pids)
                            {
                                if (StylusSchema.Synpid2PidOid.ContainsKey(pid))
                                {
                                    var orig_tuple = StylusSchema.Synpid2PidOid[pid];
                                    long orig_pid = orig_tuple.Item1;
                                    long orig_oid = orig_tuple.Item2;
                                    long oid = StylusSchema.SchemaSynOid2Oid[orig_oid];
                                    if (o_binding != null && !o_binding.ContainEid(oid))
                                    {
                                        continue;
                                    }
                                    var oid_list = new List<long>() { oid };
                                    if (is_reverse_s[i])
                                    {
                                        long rev_pid = StylusSchema.InvPreds[orig_pid];
                                        kvp_list.Add(new KeyValuePair<long, List<long>>(rev_pid, oid_list));
                                    }
                                    else
                                    {
                                        kvp_list.Add(new KeyValuePair<long, List<long>>(orig_pid, oid_list));
                                    }
                                }
                                else
                                {
                                    if (p_binding == null || p_binding.ContainEid(pid))
                                    {
                                        var list = Storage.SelectObjectToList(root_eid, pid, o_binding);
                                        if (list.Count == 0)
                                        {
                                            continue;
                                        }
                                        kvp_list.Add(new KeyValuePair<long, List<long>>(pid, list));
                                    }
                                }
                            }
                            iter = ListUtil.Extend(iter, kvp_list);
                        }
                        else
                        {
                            if (is_reverse_s[i])
                            {
                                var oids = this.GetAllOids(root_eid);
                                iter = ListUtil.Extend(iter, oids);
                            }
                            else
                            {
                                var oids = this.GetAllRevOids(root_eid);
                                iter = ListUtil.Extend(iter, oids);
                            }
                        }
                    }

                    results.Records.AddRange(iter);
                }
            }
            return results;
        }

        public override QuerySolutions ExecutePlus(List<xTwigPlusHead> heads)
        {
            foreach (var head in heads)
            {
                var root = head.Root;
                this.query_bindings[root] = head.Bindings[root];
                foreach (var leaf in head.SelectLeaves)
                {
                    this.query_bindings[leaf.Item2] = head.Bindings[leaf.Item2];
                }
            }

            if (heads.Count == 0)
            {
                throw new Exception("Cannot parse query into twigs");
            }
            else if (heads.Count == 1)
            {
                return ExecuteSingleTwigPlus(heads[0]);
            }
            else
            {
                List<List<xTwigPlusAnswer>> intermediate_results = new List<List<xTwigPlusAnswer>>();
                foreach (var head in heads)
                {
                    intermediate_results.Add(this.ExecuteToXTwigAnswerPlus(head));
                }

                List<QuerySolutions> prune_flatten_results = new List<QuerySolutions>();
                for (int i = 0; i < intermediate_results.Count; i++)
                {
                    var solutions = PruneFlattenPlus(heads[i], intermediate_results[i]);
                    prune_flatten_results.Add(solutions);
                }

                return FinalJoin(prune_flatten_results);
            }
        }

        private QuerySolutions PruneFlattenPlus(xTwigPlusHead head, List<xTwigPlusAnswer> answers)
        {
            QuerySolutions global_ans = new QuerySolutions();
            var selected_var_pred_set = new HashSet<string>(head.SelectVarPreds);
            var selected_leaf_pairs = head.SelectLeaves;
            var selected_leaves = new List<Tuple<string, string>>();
            bool include_root = StringUtil.IsVar(head.Root);

            if (include_root)
            {
                global_ans.Heads.Add(head.Root);
            }
            else
            {
                global_ans.Heads = new List<string>();
            }

            // is_select_predicate, is_reverse, pred, obj, p_binding, o_binding
            List<bool> is_select_preds = new List<bool>();
            List<bool> is_select_objs = new List<bool>();
            List<bool> is_reverse_s = new List<bool>();
            List<Tuple<string, string>> var_pred_objs = new List<Tuple<string, string>>();

            foreach (var kvp in head.SelectLeaves)
            {
                if (StringUtil.IsVar(kvp.Item1))
                {
                    string var_p = kvp.Item1;
                    string var_o = kvp.Item2;
                    is_select_preds.Add(selected_var_pred_set.Contains(var_p));
                    is_select_objs.Add(StringUtil.IsVar(var_o));
                    is_reverse_s.Add(var_p.StartsWith("_"));
                    var_p = var_p.StartsWith("_") ? var_p.Substring(1) : var_p;
                    var_pred_objs.Add(new Tuple<string, string>(var_p, var_o));
                }
                else
                {
                    global_ans.Heads.Add(kvp.Item2);
                    selected_leaves.Add(kvp);
                }
            }

            int var_pred_len = is_select_preds.Count;
            for (int i = 0; i < var_pred_len; i++)
            {
                if (is_select_preds[i])
                {
                    global_ans.Heads.Add(var_pred_objs[i].Item1);
                }
                if (is_select_objs[i])
                {
                    global_ans.Heads.Add(var_pred_objs[i].Item2);
                }
            }

            //Console.WriteLine("global_ans.Heads: [" + string.Join(", ", global_ans.Heads) + "]");

            foreach (var ans in answers)
            {
                if (!this.query_bindings[head.Root].ContainEid(ans.Root))
                {
                    continue;
                }

                IEnumerable<long[]> iter = null;
                if (include_root)
                {
                    iter = new List<long[]>() { new long[] { ans.Root } };
                }
                for (int i = 0; i < selected_leaves.Count; i++)
                {
                    var leaf = selected_leaves[i].Item2;
                    iter = ListUtil.ExtendOrFlatten(iter, this.query_bindings[leaf].FilterEids(ans.Leaves[i]));
                }

                for (int i = 0; i < var_pred_len; i++)
                {
                    var kvps = ans.VarPredLeaves[i];
                    var pred_obj = var_pred_objs[i];
                    var pred = pred_obj.Item1;
                    var obj = pred_obj.Item2;
                    if (is_select_preds[i] && is_select_objs[i])
                    {
                        iter = ListUtil.ExtendOrFlatten(iter, kvps); // reverse the pid in execution
                    }
                    else if (!is_select_preds[i] && is_select_objs[i])
                    {
                        iter = ListUtil.ExtendOrFlatten(iter, kvps.SelectMany(kvp => kvp.Value));
                    }
                    else if (is_select_preds[i] && !is_select_objs[i])
                    {
                        iter = ListUtil.ExtendOrFlatten(iter, kvps.Select(kvp => kvp.Key));
                    }
                    else
                    {
                        // Nothing 
                    }
                }
                global_ans.Records.AddRange(iter);
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
