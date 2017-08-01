using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Concurrent;

using Trinity;

using Stylus.DataModel;
using Stylus.Query;
using Stylus.Storage;
using Stylus.Loading;
using Stylus.Util;

namespace Stylus.Distributed
{
    public class SparqlDataServer : SparqlDataServerBase
    {
        private RAMStorage storage = RAMStorage.Singleton;
        private Statistics local_stat = null;

        private Dictionary<int, List<TwigQuery>> query_dict = new Dictionary<int, List<TwigQuery>>();
        private Dictionary<int, IQueryWorker> query_works = new Dictionary<int, IQueryWorker>();
        private Dictionary<int, List<TwigAnswers>> query_intermediate_results
            = new Dictionary<int, List<TwigAnswers>>();
        private Dictionary<int, Dictionary<string, Binding>> sync_bindings 
            = new Dictionary<int,Dictionary<string,Binding>>();
        private Dictionary<int, QuerySolutions> query_results // for parallel join
            = new Dictionary<int, QuerySolutions>();
        private Dictionary<int, List<long>> query_ids = new Dictionary<int, List<long>>();

        private Binding ToBinding(BindingMessage bm)
        {
            if (bm.Types != null && bm.Types.Count > 0)
            {
                return new TidBinding(bm.Types);
            }
            else
            {
                return new EidSetBinding(bm.Values);
            }
        }

        public override void LoadFileHandler(LoadFileInfoReader request)
        {
            string rdfFilename = request.FilePath;
            string meta_dir = request.SchemaDir;
            bool unfold_isa = request.UnfoldIsA;

            StylusConfig.SetStoreMetaRootDir(meta_dir);
            if (!unfold_isa)
            {
                StylusSchema.PredCandidatesForSynPred = new HashSet<string>();
            }

            DataScanner.LoadFile(rdfFilename);

            storage.ReloadIndices();
        }

        public override void LoadEncodedFileHandler(LoadFileInfoReader request)
        {
            string encodedFilename = request.FilePath;
            string meta_dir = request.SchemaDir;
            bool unfold_isa = request.UnfoldIsA;

            StylusConfig.SetStoreMetaRootDir(meta_dir);
            if (!unfold_isa)
            {
                StylusSchema.PredCandidatesForSynPred = new HashSet<string>();
            }

            DataScanner.LoadEncodedFile(encodedFilename);

            storage.ReloadIndices();
        }

        public override void LoadStorageHandler()
        {
            //TrinityConfig.ReadOnly = true;
            Global.LocalStorage.LoadStorage();
            storage.ReloadIndices();
            //local_stat = RAMStorage.CardStatistics;
        }

        public override void AggregateStatInfoHandler(LocalStatInfoWriter response)
        {
            local_stat = RAMStorage.CardStatistics;

            List<TidStatInfo> tid_infos = new List<TidStatInfo>();
            foreach (var tid_pid_info in local_stat.Tid2Pid2OidSel)
            {
                var tid = tid_pid_info.Key;
                List<TidPidStatInfo> pid_infos = new List<TidPidStatInfo>();
                foreach (var oid_sel in tid_pid_info.Value)
                {
                    pid_infos.Add(new TidPidStatInfo(oid_sel.Key, oid_sel.Value));
                }
                TidStatInfo tid_info = new TidStatInfo(tid, pid_infos);
                tid_infos.Add(tid_info);
            }

            response.LocalStat = tid_infos;
        }

        public override void ExecuteQueryStepHandler(QueryStepInfoReader request)
        {
            int query_id = request.QueryId;
            int step_index = request.StepIndex;
            var tq = query_dict[query_id][step_index];
            var worker = this.query_works[query_id];

            xTwigHead head = new xTwigHead();
            head.Root = tq.Root;
            head.LeavePreds = tq.Leaves;
            head.SelectLeaves = new List<Tuple<string, string>>();
            head.Bindings = new Dictionary<string, Binding>();

            for (int i = 0; i < tq.SelectLeaveFixPreds.Count; i++)
            {
                var variable_name = tq.SelectLeaveVars[i];
                head.SelectLeaves.Add(Tuple.Create(tq.SelectLeaveFixPreds[i], tq.SelectLeaveVars[i]));
            }

            //var results = worker.ExecuteToXTwigAnswer(head);
            //var elements = new List<TwigAnswer>();
            //foreach (var xtwig in results)
            //{
            //    TwigAnswer ans = new TwigAnswer();
            //    ans.Root = xtwig.Root;
            //    ans.LeaveValues = xtwig.Leaves.Select(l => l.ToList()).ToList();
            //    elements.Add(ans);
            //}

            var twig_results = worker.ExecuteToTwigAnswer(head);

            if (query_dict[query_id].Count == 1 && head.SelectLeaves.Count == 0)
            {
                this.query_ids.Add(query_id, twig_results.Elements.Select(e => e.Root).ToList());
            }
            else
            {
                if (!query_intermediate_results.ContainsKey(query_id))
                {
                    query_intermediate_results.Add(query_id, new List<TwigAnswers>());
                }
                query_intermediate_results[query_id].Add(twig_results);
            }
        }

        public override void IssueQueryHandler(PreparedQueryReader request, QueryInfoWriter response)
        {
            int query_id = request.QueryId;
            this.query_dict.Add(query_id, (List<TwigQuery>)request.TwigQueries);
            this.query_intermediate_results.Add(query_id, new List<TwigAnswers>());
            var worker = new ParallelQueryWorker();
            worker.Storage = this.storage;
            this.query_works[query_id] = worker;
            foreach (var tq in (List<TwigQuery>)request.TwigQueries)
            {
                foreach (var bm in tq.Bindings)
                {
                    var binding = ToBinding(bm);
                    this.query_works[query_id].SetBinding(bm.Name, binding);
                }
            }
            response.QueryId = query_id;
        }

        public override void SyncStepHandler(QueryStepInfoReader request)
        {
            int query_id = request.QueryId;
            int step_index = request.StepIndex;
            TwigQuery tq = this.query_dict[query_id][step_index];
            var worker = this.query_works[query_id];

            Dictionary<string, Binding> local_bindings = new Dictionary<string, Binding>();

            var list_bm = new List<BindingMessage>();
            if (!(worker.GetBinding(tq.Root) is TidBinding))
            {
                list_bm.Add(new BindingMessage(tq.Root, new List<ushort>(), worker.EnumerateBinding(tq.Root).ToList()));
                local_bindings.Add(tq.Root, worker.GetBinding(tq.Root));
            }
            foreach (var leaf in tq.SelectLeaveVars)
            {
                if (!(worker.GetBinding(leaf) is TidBinding))
                {
                    list_bm.Add(new BindingMessage(leaf, new List<ushort>(), worker.EnumerateBinding(leaf).ToList()));
                    local_bindings.Add(leaf, worker.GetBinding(leaf));
                }
            }

            BindingMessagesWriter msg = new BindingMessagesWriter(query_id, step_index, list_bm);

            Parallel.For(0, TrinityConfig.Servers.Count, i =>
            {
                if (i == Global.MyServerID)
                {
                    lock (this.sync_bindings)
                    {
                        this.sync_bindings[query_id] = local_bindings;
                    }
                }
                else
                {
                    Global.CloudStorage.SyncBindingsToSparqlDataServer(i, msg);
                }
            });
        }

        public override void SyncBindingsHandler(BindingMessagesReader request)
        {
            int query_id = request.QueryId;
            var worker = this.query_works[query_id];
            if (!this.sync_bindings.ContainsKey(query_id)) // double check
            {
                lock (this.sync_bindings)
                {
                    if (!this.sync_bindings.ContainsKey(query_id))
                    {
                        this.sync_bindings[query_id] = new Dictionary<string, Binding>();
                    }
                }
            }

            Parallel.ForEach((List<BindingMessage>)request.Elements, StylusConfig.DegreeOfParallelismOption, binding => {
                if (!this.sync_bindings[query_id].ContainsKey(binding.Name)) // double check
                {
                    lock (this.sync_bindings[query_id])
                    {
                        if (!this.sync_bindings[query_id].ContainsKey(binding.Name))
                        {
                            this.sync_bindings[query_id].Add(binding.Name, new EidSetBinding());
                        }
                    }
                }

                lock (this.sync_bindings[query_id])
                {
                    if (this.sync_bindings[query_id][binding.Name] is TidBinding)
                    {
                        this.sync_bindings[query_id][binding.Name] = new EidSetBinding(this.sync_bindings[query_id][binding.Name].FilterEids((List<long>)binding.Values));
                    }
                    else
                    {
                        this.sync_bindings[query_id][binding.Name].AddEids((List<long>)binding.Values);
                    }
                }
            });
        }

        public override void FinishSyncStepHandler(QueryStepInfoReader request)
        {
            var query_id = request.QueryId;
            var step_index = request.StepIndex;

            var binding_dict = this.sync_bindings[query_id];
            var worker = query_works[query_id];
            foreach (var kvp in binding_dict)
            {
                worker.ReplaceBinding(kvp.Key, kvp.Value);
            }
        }

        public override void AggregateQueryAnswersHandler(QueryInfoReader request, QueryAnswersWriter response)
        {
            int query_id = request.QueryId;
            var results = query_intermediate_results[query_id];
            var twig_queries = query_dict[query_id];

            response.Results = results;

            //// prune earlier results before sending # no effect
            //var query_worker = query_works[query_id];
            //var filter_results = new List<TwigAnswers>();
            //for (int i = 0; i < results.Count; i++)
            //{
            //    filter_results.Add(PruneAggregate(query_worker, twig_queries[i], results[i]));
            //}
            //response.Results = filter_results;
        }

        private TwigAnswers PruneAggregate(IQueryWorker worker, TwigQuery tq, TwigAnswers ans) 
        {
            List<TwigAnswer> ta_list = new List<TwigAnswer>();
            var root_binding = worker.GetBinding(tq.Root);
            var leaf_bindings = new List<Binding>();
            foreach (var leaf in tq.SelectLeaveVars)
            {
                leaf_bindings.Add(worker.GetBinding(leaf));
            }
            int leaf_cnt = tq.SelectLeaveVars.Count;

            foreach (var ta in ans.Elements)
            {
                if (!root_binding.ContainEid(ta.Root))
                {
                    continue;
                }
                List<List<long>> ta_filter_recs = new List<List<long>>();
                for (int i = 0; i < leaf_cnt; i++)
                {
                    var results = leaf_bindings[i].FilterEids(ta.LeaveValues[i]).ToList();
                    ta_filter_recs.Add(results);
                }
                if (ta_filter_recs.All(l => l.Count > 0))
                {
                    ta_list.Add(new TwigAnswer(ta.Root, ta_filter_recs));
                }
            }
            return new TwigAnswers(-1, -1, ta_list);
        }

        public override void FreeQueryHandler(QueryInfoReader request)
        {
            int query_id = request.QueryId;
            lock (this)
            {
                this.query_dict.Remove(query_id);
                this.query_works.Remove(query_id);
                if (this.query_intermediate_results.ContainsKey(query_id))
                {
                    this.query_intermediate_results.Remove(query_id);
                }
                if (this.sync_bindings.ContainsKey(query_id))
                {
                    this.sync_bindings.Remove(query_id);
                }

                if (this.query_results.ContainsKey(query_id))
                {
                    this.query_results.Remove(query_id);
                }
                if (this.query_ids.ContainsKey(query_id))
                {
                    this.query_ids.Remove(query_id);
                }
            }
        }

        public override void BroadcastSuffixQueryAnswersHandler(QueryInfoReader request)
        {
            int query_id = request.QueryId;
            QueryInfo qInfo = new QueryInfo(query_id);
            var qAnswers = new QueryAnswers(query_intermediate_results[query_id].Skip(1).ToList());
            QueryInfoAnswersWriter msg = new QueryInfoAnswersWriter(qInfo, qAnswers);
            Parallel.For(0, TrinityConfig.Servers.Count, i =>
            {
                if (i == Global.MyServerID)
                {
                    return;
                }
                Global.CloudStorage.AcceptSuffixQueryAnswersToSparqlDataServer(i, msg);
            });
        }

        public override void AcceptSuffixQueryAnswersHandler(QueryInfoAnswersReader request)
        {
            int query_id = request.QInfo.QueryId;
            var intermediate_results = this.query_intermediate_results[query_id];
            var q_answers = (QueryAnswers)request.QAnswers;
            for (int i = 0; i < q_answers.Results.Count; i++)
            {
                lock (intermediate_results)
                {
                    intermediate_results[i + 1].Elements.AddRange(q_answers.Results[i].Elements);
                }
            }
        }

        private xTwigHead ToXTwigHead(TwigQuery tq) 
        {
            xTwigHead head = new xTwigHead();
            head.Root = tq.Root;
            head.SelectLeaves = new List<Tuple<string, string>>();
            for (int i = 0; i < tq.SelectLeaveFixPreds.Count; i++)
            {
                head.SelectLeaves.Add(Tuple.Create(tq.SelectLeaveFixPreds[i], tq.SelectLeaveVars[i]));
            }
            return head;
        }

        public override void JoinQueryAnswersHandler(QueryInfoReader request)
        {
            int query_id = request.QueryId;
            var intermediate_results = this.query_intermediate_results[query_id];

            QuerySolutions qs = null;

            List<TwigSolutions> twig_results = new List<TwigSolutions>();
            for (int i = 0; i < intermediate_results.Count; i++)
            {
                TwigSolutions ts = new TwigSolutions();
                ts.Head = ToXTwigHead(this.query_dict[query_id][i]);
                //ts.SetActualSize(intermediate_results[i].ActualSize);
                ts.Solutions = intermediate_results[i].Elements;
                twig_results.Add(ts);
            }
            if (twig_results.Count == 1)
            {
                qs = twig_results[0].Flatten();
            }
            else
            {
                // twig_results.Sort((ts1, ts2) => { return ts1.GetActualSize().CompareTo(ts2.GetActualSize()); });
                // qs = twig_results[0].JoinFlattenBySizeOrder(twig_results[1]);
                qs = twig_results[0].JoinFlatten(twig_results[1]);
                if (twig_results.Count > 2)
                {
                    for (int i = 2; i < twig_results.Count; i++)
                    {
                        qs = twig_results[i].JoinFlatten(qs);
                    }
                }
            }

            this.query_results.Add(query_id, qs);
        }

        public override void AggregateQueryResultsHandler(QueryInfoReader request, QueryResultsWriter response)
        {
            int query_id = request.QueryId;
            QuerySolutions qs = this.query_results[query_id];
            response.Variables = qs.Heads;
            response.Records = qs.Records.Select(l => l.ToList()).ToList();
        }

        public override void AggregateIdListHandler(QueryInfoReader request, IdListWriter response)
        {
            int query_id = request.QueryId;
            response.Ids = query_ids[query_id];
        }
    }
}
