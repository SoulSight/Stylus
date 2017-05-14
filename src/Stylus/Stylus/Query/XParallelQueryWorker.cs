using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Stylus.Util;
using Stylus.DataModel;

namespace Stylus.Query
{
    public class XParallelQueryWorker : BaseQueryWorker
    {
        private ConcurrentDictionary<string, Binding> query_bindings
            = new ConcurrentDictionary<string,Binding>();
        private ParallelOptions options = new ParallelOptions { MaxDegreeOfParallelism
                    = Environment.ProcessorCount };

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
                    ICollection<long> values = this.Storage.LoadIndex(tid);
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

        public override List<xTwigAnswer> ExecuteToXTwigAnswer(xTwigHead head)
        {
            var root = head.Root;
            var selected_leaves = head.SelectLeaves;
            List<long> pids = new List<long>();
            List<Binding> filter_bindings = new List<Binding>();
            List<Binding> new_bindings = new List<Binding>();
            Binding new_root_binding = new ConcurrentEidSetBinding();

            foreach (var kvp in selected_leaves)
            {
                pids.Add(StylusSchema.Pred2Pid[kvp.Item1]);
                filter_bindings.Add(query_bindings[kvp.Item2]);
                new_bindings.Add(new ConcurrentEidSetBinding());
            }
            
            List<xTwigAnswer> results = new List<xTwigAnswer>();

            Binding root_binding = query_bindings[root];
            var root_candidates = EnumerateBindingValues(root_binding);
            //foreach(var tid_eids in root_candidates)

            foreach(var tid_eids in root_candidates)
            {
                ushort tid = tid_eids.Item1;
                var eids = tid_eids.Item2;
                if (tid == StylusConfig.GenericTid)
                {
                    Parallel.ForEach(eids, options, root_eid =>
                    {
                        xTwigAnswer ans = new xTwigAnswer();
                        ans.Head = head;
                        ans.Root = root_eid;
                        ans.Leaves = Storage.SelectObjects(root_eid, pids, filter_bindings);

                        if (!ans.IsValid())
                        {
                            return;
                        }

                        lock (results)
                        {
                            results.Add(ans);
                        }

                        // prepare updating bindings
                        new_root_binding.AddEid(root_eid);

                        for (int i = 0; i < pids.Count; i++)
                        {
                            new_bindings[i].AddEids(ans.Leaves[i]);
                        }
                    });
                }
                else
                {
                    List<int> offsets = pids.Select(pid => StylusSchema.TidPid2Index[tid][pid]).ToList();
                    Parallel.ForEach(eids, options, root_eid =>
                    {
                        List<long[]> select_objs = Storage.SelectOffsets(root_eid, offsets, filter_bindings);
                        xTwigAnswer ans = new xTwigAnswer();
                        ans.Head = head;
                        ans.Root = root_eid;
                        ans.Leaves = select_objs;

                        if (!ans.IsValid())
                        {
                            return;
                        }

                        lock (results)
                        {
                            results.Add(ans);
                        }

                        // prepare updating bindings
                        new_root_binding.AddEid(root_eid);
                        for (int i = 0; i < pids.Count; i++)
                        {
                            new_bindings[i].AddEids(ans.Leaves[i]);
                        }
                    });
                }
            }

            // update query bindings
            this.query_bindings[root] = new_root_binding;
            for (int i = 0; i < head.SelectLeaves.Count; i++)
            {
                var sel_var = head.SelectLeaves[i].Item2;
                this.query_bindings[sel_var] = new_bindings[i];
            }

            return results;
        }

        public override TwigAnswers ExecuteToTwigAnswer(xTwigHead head)
        {
            var root = head.Root;
            var selected_leaves = head.SelectLeaves;
            List<long> pids = new List<long>();
            List<Binding> filter_bindings = new List<Binding>();
            List<Binding> new_bindings = new List<Binding>();
            Binding new_root_binding = new ConcurrentEidSetBinding();

            foreach (var kvp in selected_leaves)
            {
                pids.Add(StylusSchema.Pred2Pid[kvp.Item1]);
                filter_bindings.Add(query_bindings[kvp.Item2]);
                new_bindings.Add(new ConcurrentEidSetBinding());
            }

            List<TwigAnswer> results = new List<TwigAnswer>();

            long actual_size = 0;
            long twig_count = 0;

            Binding root_binding = query_bindings[root];
            var root_candidates = EnumerateBindingValues(root_binding);
            //foreach(var tid_eids in root_candidates)

            foreach (var tid_eids in root_candidates)
            {
                ushort tid = tid_eids.Item1;
                var eids = tid_eids.Item2;
                if (tid == StylusConfig.GenericTid)
                {
                    Parallel.ForEach(eids, options, root_eid =>
                    {
                        TwigAnswer ans = new TwigAnswer();
                        ans.Root = root_eid;
                        ans.LeaveValues = Storage.SelectObjectsToList(root_eid, pids, filter_bindings);

                        lock (results)
                        {
                            results.Add(ans);

                            twig_count++;
                            long local_actual_size = 1;
                            foreach (var l in ans.LeaveValues)
                            {
                                local_actual_size *= l.Count;
                            }
                            actual_size += local_actual_size;
                        }

                        // prepare updating bindings
                        new_root_binding.AddEid(root_eid);

                        for (int i = 0; i < pids.Count; i++)
                        {
                            new_bindings[i].AddEids(ans.LeaveValues[i]);
                        }
                    });
                }
                else
                {
                    List<int> offsets = pids.Select(pid => StylusSchema.TidPid2Index[tid][pid]).ToList();
                    Parallel.ForEach(eids, options, root_eid =>
                    {
                        List<List<long>> select_objs = Storage.SelectOffsetsToList(root_eid, offsets, filter_bindings);
                        TwigAnswer ans = new TwigAnswer();
                        ans.Root = root_eid;
                        ans.LeaveValues = select_objs;

                        lock (results)
                        {
                            results.Add(ans);

                            twig_count++;
                            long local_actual_size = 1;
                            foreach (var l in select_objs)
                            {
                                local_actual_size *= l.Count;
                            }
                            actual_size += local_actual_size;
                        }

                        // prepare updating bindings
                        new_root_binding.AddEid(root_eid);
                        for (int i = 0; i < pids.Count; i++)
                        {
                            new_bindings[i].AddEids(ans.LeaveValues[i]);
                        }
                    });
                }
            }

            // update query bindings
            this.query_bindings[root] = new_root_binding;
            for (int i = 0; i < head.SelectLeaves.Count; i++)
            {
                var sel_var = head.SelectLeaves[i].Item2;
                this.query_bindings[sel_var] = new_bindings[i];
            }

            //return results;
            return new TwigAnswers(actual_size, twig_count, results);
        }

        public override QuerySolutions ExecuteFlatten(xTwigHead head)
        {
            var results = ExecuteToXTwigAnswer(head);
            return PruneFlatten(head, results);
        }

        public override QuerySolutions Execute(List<xTwigHead> heads)
        {
            //this.query_bindings = new ConcurrentDictionary<string, Binding>();
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
                return ExecuteSingleTwig(heads[0]);
            }
            else
            {
                List<List<xTwigAnswer>> intermediate_results = new List<List<xTwigAnswer>>();
                foreach (var head in heads)
                {
                    intermediate_results.Add(this.ExecuteToXTwigAnswer(head));
                }

                List<QuerySolutions> prune_flatten_results = new List<QuerySolutions>();
                for (int i = 0; i < intermediate_results.Count; i++)
                {
                    prune_flatten_results.Add(PruneFlatten(heads[i], intermediate_results[i]));
                }

                return FinalJoin(prune_flatten_results);
            }
        }

        private QuerySolutions PruneFlatten(xTwigHead head, List<xTwigAnswer> answers)
        {
            QuerySolutions global_ans = new QuerySolutions();
            global_ans.Heads.Add(head.Root);
            global_ans.Heads.AddRange(head.SelectLeaves.Select(l => l.Item2));

            Parallel.ForEach(answers, options, ans =>
            {
                if (!this.query_bindings[head.Root].ContainEid(ans.Root))
                {
                    return;
                }
                QuerySolutions local_ans = new QuerySolutions();
                local_ans.Heads = new List<string>() { head.Root };
                local_ans.Records = new List<long[]>() { new long[] { ans.Root } };
                for (int i = 0; i < head.SelectLeaves.Count; i++)
                {
                    var leaf = head.SelectLeaves[i].Item2;
                    local_ans.Product(leaf, this.query_bindings[leaf].FilterEids(ans.Leaves[i]));
                }
                lock (global_ans)
                {
                    global_ans.Records.AddRange(local_ans.Records);
                }
            });
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
                    //results = results.Join(intermediate_results[i]);
                    if (results.Records.Count < intermediate_results[i].Records.Count)
                    {
                        results = results.Join(intermediate_results[i]);
                    }
                    else
                    {
                        results = intermediate_results[i].Join(results);
                    }
                }
            }
            return results;
        }

        public override QuerySolutions ExecuteSingleTwig(xTwigHead head)
        {
            var root = head.Root;
            var selected_leaf_dict = head.SelectLeaves;
            List<long> pids = new List<long>();

            List<Binding> leaf_bindings = new List<Binding>();

            foreach (var kvp in selected_leaf_dict)
            {
                pids.Add(StylusSchema.Pred2Pid[kvp.Item1]);
                leaf_bindings.Add(query_bindings[kvp.Item2]);
            }

            Binding root_binding = query_bindings[root];
            var root_candidates = EnumerateBindingValues(root_binding);

            QuerySolutions results = new QuerySolutions();
            results.Heads = new List<string>() { root };

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

            results.Heads.AddRange(head.SelectLeaves.Select(l => l.Item2));

            foreach(var tid_eids in root_candidates)
            {
                ushort tid = tid_eids.Item1;
                var eids = tid_eids.Item2;
                List<int> offsets = new List<int>(pids.Count);
                foreach (var pid in pids)
                {
                    offsets.Add(StylusSchema.TidPid2Index[tid][pid]);
                }

                Parallel.ForEach(eids, options, root_eid =>
                {
                    List<long[]> select_objs = Storage.SelectOffsets(root_eid, offsets, leaf_bindings);
                    List<long[]> to_product = new List<long[]>();
                    to_product.Add(new long[] { root_eid });
                    to_product.AddRange(select_objs);
                    lock (results)
                    {
                        results.Records.AddRange(ListUtil.Product(to_product));
                    }
                });
            }
            return results;
        }
    }
}
