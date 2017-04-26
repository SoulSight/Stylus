using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Stylus.DataModel;
using Stylus.Util;

namespace Stylus.Query
{
    public class TwigSolutions
    {
        public xTwigHead Head { set; get; }

        public List<TwigAnswer> Solutions { set; get; }

        private long actual_size = -1;
        public long GetActualSize()
        {
            if (actual_size == -1)
            {
                actual_size = TwigUtil.CalcActualSize(this.Solutions);
            }
            return actual_size;
        }

        public void SetActualSize(long actual_size) 
        {
            this.actual_size = actual_size;
        }

        public QuerySolutions Flatten() 
        {
            QuerySolutions global_ans = new QuerySolutions();
            global_ans.Heads.Add(this.Head.Root);
            global_ans.Heads.AddRange(Head.SelectLeaves.Select(l => l.Item2));

            foreach (var ans in this.Solutions)
            {
                QuerySolutions local_ans = new QuerySolutions();
                local_ans.Heads = new List<string>() { this.Head.Root };
                local_ans.Records = new List<long[]>() { new long[] { ans.Root } };
                for (int i = 0; i < this.Head.SelectLeaves.Count; i++)
                {
                    var leaf = this.Head.SelectLeaves[i].Item2;
                    local_ans.Product(leaf, ans.LeaveValues[i]);
                }
                global_ans.Records.AddRange(local_ans.Records);
            }
            return global_ans;
        }

        public QuerySolutions Flatten(List<string> selected_heads)
        {
            QuerySolutions global_ans = new QuerySolutions();
            HashSet<string> head_set = new HashSet<string>(selected_heads);
            global_ans.Heads = new List<string>();
            if (head_set.Contains(this.Head.Root))
            {
                global_ans.Heads.Add(this.Head.Root);
            }
            global_ans.Heads.AddRange(Head.SelectLeaves.Select(l => l.Item2)
                .Where(item => selected_heads.Contains(item)));

            foreach (var ans in this.Solutions)
            {
                QuerySolutions local_ans = new QuerySolutions();
                if (head_set.Contains(this.Head.Root))
                {
                    local_ans.Heads = new List<string>() { this.Head.Root };
                    local_ans.Records = new List<long[]>() { new long[] { ans.Root } };
                }
                for (int i = 0; i < this.Head.SelectLeaves.Count; i++)
                {
                    var leaf = this.Head.SelectLeaves[i].Item2;
                    if (head_set.Contains(leaf))
                    {
                        local_ans.Product(leaf, ans.LeaveValues[i]);
                    }
                }
                global_ans.Records.AddRange(local_ans.Records);
            }
            return global_ans;
        }

        private bool PartialEquals(long[] array1, List<int> indexes1, long[] array2, List<int> indexes2)
        {
            int cnt = indexes1.Count;
            if (cnt != indexes2.Count)
            {
                return false;
            }
            for (int i = 0; i < cnt; i++)
            {
                if (array1[indexes1[i]] != array2[indexes2[i]])
                {
                    return false;
                }
            }
            return true;
        }

        private long[] MergeRecord(long[] srecord, long[] orecord, int[] signed_indexes)
        {
            long[] result = new long[signed_indexes.Length];
            for (int i = 0; i < signed_indexes.Length; i++)
            {
                int index = signed_indexes[i];
                if (index >= 0)
                {
                    result[i] = srecord[index];
                }
                else
                {
                    result[i] = orecord[-index - 1];
                }
            }
            return result;
        }

        public QuerySolutions JoinFlatten(QuerySolutions querySolutions)
        {
            var twig_head_dict = new Dictionary<string, int>();
            string twig_head_root = this.Head.Root;
            twig_head_dict.Add(twig_head_root, 0); // root = 0; leaves[i] = i + 1
            for (int i = 0; i < this.Head.SelectLeaves.Count; i++)
            {
                twig_head_dict.Add(this.Head.SelectLeaves[i].Item2, i + 1);
            }

            var query_solutions_head_dict = new Dictionary<string, int>(); ;
            for (int i = 0; i < querySolutions.Heads.Count; i++)
            {
                query_solutions_head_dict.Add(querySolutions.Heads[i], i);
            }

            var join_cols = new Dictionary<string, Tuple<int, int>>();
            List<int> twig_join_indexes = new List<int>();
            List<int> qs_join_indexes = new List<int>();
            foreach (var twig_kvp in twig_head_dict)
            {
                string key = twig_kvp.Key;
                if (query_solutions_head_dict.ContainsKey(key))
                {
                    join_cols.Add(key, Tuple.Create(twig_kvp.Value, query_solutions_head_dict[key]));
                    twig_join_indexes.Add(twig_kvp.Value);
                    qs_join_indexes.Add(query_solutions_head_dict[key]);
                }
            }

            if (join_cols.Count == 0)
            {
                throw new Exception("No join.");
            }

            List<string> union_head = new List<string>(twig_head_dict.Keys);
            union_head.AddRange(querySolutions.Heads);
            union_head = union_head.Distinct().ToList();
            QuerySolutions join_binding = new QuerySolutions();
            join_binding.Heads = union_head;

            int[] signed_indexes = new int[union_head.Count];
            for (int i = 0; i < union_head.Count; i++)
            {
                string col_name = union_head[i];
                if (twig_head_dict.ContainsKey(col_name))
                {
                    signed_indexes[i] = twig_head_dict[col_name];
                }
                else
                {
                    signed_indexes[i] = -(query_solutions_head_dict[col_name] + 1);
                }
            }

            join_binding.Records = new List<long[]>();

            int qs_root_index = -1;
            HashSet<long> root_candidates = null;
            bool query_contains_twig_head_root = query_solutions_head_dict.ContainsKey(twig_head_root);

            if (query_contains_twig_head_root)
            {
                qs_root_index = query_solutions_head_dict[twig_head_root];
                root_candidates = new HashSet<long>();
            }
            Dictionary<int, List<int>> hash_qs_indexes = new Dictionary<int, List<int>>();
            //for (int i = 0; i < querySolutions.Records.Count; i++)
            Parallel.For(0, querySolutions.Records.Count, StylusConfig.DegreeOfParallelismOption, i =>
            {
                var qs_record = querySolutions.Records[i];

                if (query_contains_twig_head_root)
                {
                    long root_candidate = qs_record[qs_root_index];
                    root_candidates.Add(root_candidate);
                }

                int qs_hash = ListUtil.GetHashCode(qs_record, qs_join_indexes);
                lock (hash_qs_indexes)
                {
                    List<int> recs;
                    if (!hash_qs_indexes.TryGetValue(qs_hash, out recs))
                    {
                        recs = new List<int>() { i };
                        hash_qs_indexes[qs_hash] = recs;
                    }
                    else
                    {
                        recs.Add(i);
                    }
                }
            });

            Parallel.ForEach(this.Solutions, StylusConfig.DegreeOfParallelismOption, ta =>
            {
                if (query_contains_twig_head_root && !root_candidates.Contains(ta.Root))
                {
                    return;
                }
                if (ta.LeaveValues.Any(l => l.Count == 0))
                {
                    return;
                }
                List<List<long>> bindings = new List<List<long>>();
                bindings.Add(new List<long>() { ta.Root });
                bindings.AddRange(ta.LeaveValues);
                foreach (var twig_record in ListUtil.EnumerateCombination(bindings))
                {
                    int twig_hash = ListUtil.GetHashCode(twig_record, twig_join_indexes);
                    List<int> qsr_indexes;
                    if (!hash_qs_indexes.TryGetValue(twig_hash, out qsr_indexes))
                    {
                        continue;
                    }
                    else
                    {
                        foreach (var qsr_index in qsr_indexes)
                        {
                            var qs_record = querySolutions.Records[qsr_index];
                            if (PartialEquals(twig_record, twig_join_indexes, qs_record, qs_join_indexes))
                            {
                                lock (join_binding)
                                {
                                    join_binding.Records.Add(MergeRecord(twig_record, qs_record, signed_indexes));
                                }
                            }
                        }
                    }
                }
            });

            return join_binding;
        }

        public QuerySolutions JoinFlattenBySizeOrder(TwigSolutions other)
        {
            long actual_size_other = other.GetActualSize();
            long actual_size_this = this.GetActualSize();
            if (actual_size_other < actual_size_this)
            {
                return JoinFlatten(other.Flatten());
            }
            else
            {
                return other.JoinFlatten(this.Flatten());
            }
        }

        public QuerySolutions JoinFlatten(TwigSolutions other)
        {
            return JoinFlatten(other.Flatten());
        }
    }
}
