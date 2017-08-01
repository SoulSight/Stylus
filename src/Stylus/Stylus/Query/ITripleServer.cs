using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Stylus.Query
{
    public interface ITripleServer
    {
        List<long> GetPids(long eid);

        List<string> GetPreds(long eid);

        List<string> GetPreds(string entity);

        List<long> GetOids(long eid, long pid);

        List<string> GetObjs(long eid, long pid);

        List<string> GetObjs(string subj, string pred);

        List<long> GetSids(long pid, long oid);

        List<string> GetSubjs(string pred, string obj);

        List<long> GetAllOids(long eid);

        List<long> GetAllRevOids(long eid);

        IEnumerable<long> GetSids(IEnumerable<long> pids);

        IEnumerable<long> GetSids(IEnumerable<string> preds);

        IEnumerable<string> GetSubjs(IEnumerable<long> pids);

        IEnumerable<string> GetSubjs(IEnumerable<string> preds);
    }
}
