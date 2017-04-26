using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Stylus.Query;

namespace Stylus.Storage
{
    public interface IStorage
    {
        Dictionary<ushort, List<long>> TidInstances { set; get; }

        HashSet<ushort> SupType(IEnumerable<long> pids);

        List<long> LoadIndex(ushort tid);

        // Select object
        long[] SelectOffset(long eid, int offsetIndex);

        List<long> SelectOffsetToList(long eid, int offsetIndex);

        List<long[]> SelectOffsets(long eid, List<int> offsetIndexes);

        List<List<long>> SelectOffsetsToList(long eid, List<int> offsetIndexes);

        long[] SelectObjectFromSynpred(long eid, long pid);

        List<long> SelectObjectFromSynpredToList(long eid, long pid);

        long[] SelectObject(long eid, long pid);

        List<long> SelectObjectToList(long eid, long pid);

        List<long[]> SelectObjects(long eid, List<long> pids);

        List<List<long>> SelectObjectsToList(long eid, List<long> pids);

        // Select objects with bindings
        long[] SelectOffset(long eid, int offsetIndex, Binding binding);

        List<long> SelectOffsetToList(long eid, int offsetIndex, Binding binding);

        List<long[]> SelectOffsets(long eid, List<int> offsetIndexes, List<Binding> bindings);

        List<List<long>> SelectOffsetsToList(long eid, List<int> offsetIndexes, List<Binding> bindings);

        long[] SelectObjectFromSynpred(long eid, long pid, Binding binding);

        List<long> SelectObjectFromSynpredToList(long eid, long pid, Binding binding);

        long[] SelectObject(long eid, long pid, Binding binding);

        List<long> SelectObjectToList(long eid, long pid, Binding binding);

        List<long[]> SelectObjects(long eid, List<long> pids, List<Binding> bindings);

        List<List<long>> SelectObjectsToList(long eid, List<long> pids, List<Binding> bindings);
    }
}
