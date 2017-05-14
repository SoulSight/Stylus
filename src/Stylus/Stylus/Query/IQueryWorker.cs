using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Stylus.Parsing;
using Stylus.Storage;
using Stylus.DataModel;

namespace Stylus.Query
{
    public interface IQueryWorker
    {
        IStorage Storage { set; get; }

        Dictionary<string, long> LiteralToId { set; get; }


        #region SPARQL
        List<xTwigHead> Plan(QueryGraph qg);

        bool ContainsBinding(string variable);

        IEnumerable<long> EnumerateBinding(string variable);

        Binding GetBinding(string variable);

        void SetBinding(string variable, Binding binding);

        void ReplaceBinding(string variable, IEnumerable<long> bindings);

        void ReplaceBinding(string variable, Binding bindings);

        void AppendBinding(string variable, IEnumerable<long> bindings);

        List<xTwigAnswer> ExecuteToXTwigAnswer(xTwigHead head);

        TwigAnswers ExecuteToTwigAnswer(xTwigHead head);

        QuerySolutions ExecuteFlatten(xTwigHead head);

        QuerySolutions ExecuteSingleTwig(xTwigHead head);

        QuerySolutions Execute(List<xTwigHead> heads);

        IEnumerable<List<string>> ResolveQuerySolutions(QuerySolutions querySolutions);
        #endregion

        #region Graph API
        List<long> GetPids(long eid);

        List<string> GetPreds(long eid);

        List<string> GetPreds(string entity);

        List<long> GetOids(long eid, long pid);

        List<string> GetObjs(long eid, long pid);

        List<string> GetObjs(string subj, string pred);

        List<long> GetSids(long pid, long oid);

        List<string> GetSubjs(string pred, string obj);

        List<long> GetAllOids(long eid);

        IEnumerable<long> GetSids(IEnumerable<long> pids);

        IEnumerable<long> GetSids(IEnumerable<string> preds);

        IEnumerable<string> GetSubjs(IEnumerable<long> pids);

        IEnumerable<string> GetSubjs(IEnumerable<string> preds);
        #endregion
    }
}
