using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Stylus.Util
{
    public class ListUtil
    {
        public static IEnumerable<List<string>> GetPermutation(List<string> list)
        {
            if (list.Count == 1)
            {
                yield return list;
            }
            else
            {
                for (int i = 0; i < list.Count; i++)
                {
                    List<string> list_copy = new List<string>(list);
                    string head = list_copy[i];
                    list_copy.RemoveAt(i);

                    foreach (var suffix in GetPermutation(list_copy))
                    {
                        List<string> new_list = new List<string>() { head };
                        new_list.AddRange(suffix);
                        yield return new_list;
                    }
                }
            }
        }

        public static IEnumerable<long[]> Product(List<long[]> lists)
        {
            List<long[]> results = new List<long[]>();
            foreach (var item in lists[0])
            {
                results.Add(new long[] { item });
            }

            for (int i = 1; i < lists.Count; i++)
            {
                List<long[]> next_results = new List<long[]>();
                foreach (var result in results)
                {
                    foreach (var item in lists[i])
                    {
                        long[] rec = new long[i + 1];
                        Array.Copy(result, rec, i);
                        rec[i] = item;
                        next_results.Add(rec);
                    }
                }
                results = next_results;
            }

            return results;
        }

        // p_1 [o_11, o_12, ..., o_1n1], p_2 [o_21, o_22, ..., o_2n2], ..., p_m [o_m1, ..., o_mnm]
        public static IEnumerable<long[]> Product(List<long[]> lists, 
            List<KeyValuePair<long, List<long>>[]> kvp_lists)
        {
            List<long[]> results = new List<long[]>();
            foreach (var item in lists[0])
            {
                results.Add(new long[] { item });
            }

            for (int i = 1; i < lists.Count; i++)
            {
                List<long[]> next_results = new List<long[]>();
                foreach (var result in results)
                {
                    foreach (var item in lists[i])
                    {
                        long[] rec = new long[i + 1];
                        Array.Copy(result, rec, i);
                        rec[i] = item;
                        next_results.Add(rec);
                    }
                }
                results = next_results;
            }

            int cur_len = lists.Count;
            foreach (var kvp_list in kvp_lists)
            {
                List<long[]> next_results = new List<long[]>();
                foreach (var result in results)
                {
                    foreach (var kvp in kvp_list)
                    {
                        foreach (var item in kvp.Value)
                        {
                            long[] rec = new long[cur_len + 2];
                            Array.Copy(result, rec, cur_len);
                            rec[cur_len] = kvp.Key;
                            rec[cur_len + 1] = item;
                            next_results.Add(rec);
                        }
                    }
                }
                results = next_results;
                cur_len++;
            }

            return results;
        }

        public static IEnumerable<long[]> Extend(IEnumerable<long[]> records, IEnumerable<long> list) 
        {
            foreach (var record in records)
            {
                int rec_len = record.Length;
                foreach (var item in list)
                {
                    long[] new_rec = new long[rec_len + 1];
                    Array.Copy(record, new_rec, rec_len);
                    new_rec[rec_len] = item;
                    yield return new_rec;
                }
            }
        }

        public static IEnumerable<long[]> Extend(IEnumerable<long[]> records, 
            IEnumerable<KeyValuePair<long, List<long>>> kvp_list) 
        {
            foreach (var record in records)
            {
                int rec_len = record.Length;
                foreach (var kvp in kvp_list)
                {
                    long rec_key = kvp.Key;
                    foreach (var item in kvp.Value)
                    {
                        long[] new_rec = new long[rec_len + 2];
                        Array.Copy(record, new_rec, rec_len);
                        new_rec[rec_len] = rec_key;
                        new_rec[rec_len + 1] = item;
                        yield return new_rec;
                    }
                }
            }
        }

        public static IEnumerable<long[]> Product(List<List<long>> lists)
        {
            List<long[]> results = new List<long[]>();
            foreach (var item in lists[0])
            {
                results.Add(new long[] { item });
            }

            for (int i = 1; i < lists.Count; i++)
            {
                List<long[]> next_results = new List<long[]>();
                foreach (var result in results)
                {
                    foreach (var item in lists[i])
                    {
                        long[] rec = new long[i + 1];
                        Array.Copy(result, rec, i);
                        rec[i] = item;
                        next_results.Add(rec);
                    }
                }
                results = next_results;
            }

            return results;
        }

        public static IEnumerable<long[]> EnumerateCombination(List<List<long>> lists)
        {
            int list_cnt = lists.Count;
            int[] counts = lists.Select(l => l.Count).ToArray();

            int[] current_index_values = new int[list_cnt];
            for (int i = 0; i < list_cnt; i++)
            {
                current_index_values[i] = 0;
            }

            long[] init_result = new long[list_cnt];
            for (int i = 0; i < list_cnt; i++)
            {
                init_result[i] = lists[i][0];
            }
            yield return init_result;

            while (NextIndexArray(current_index_values, counts, list_cnt))
            {
                long[] result = new long[list_cnt];
                for (int i = 0; i < list_cnt; i++)
                {
                    result[i] = lists[i][current_index_values[i]];
                }
                yield return result;
            }
        }

        private static bool NextIndexArray(int[] current_index_values, int[] max_index_values, int index_cnt)
        {
            bool carry_over = true;
            int cur_pos = index_cnt - 1;
            while (carry_over)
            {
                if (current_index_values[cur_pos] + 1 < max_index_values[cur_pos])
                {
                    current_index_values[cur_pos] = current_index_values[cur_pos] + 1;
                    carry_over = false;
                    return true;
                }
                else
                {
                    if (cur_pos == 0)
                    {
                        return false;
                    }
                    current_index_values[cur_pos] = 0;
                    carry_over = true;
                    cur_pos--;
                }
            }
            return false;
        }

        public static int GetHashCode(long[] array, List<int> indexes)
        {
            int hc = 37;
            foreach (var index in indexes)
            {
                hc = unchecked(hc * 17 + array[index].GetHashCode());
            }
            return hc;
        }
    }
}
