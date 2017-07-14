using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Stylus.Storage
{
    public interface IUpdatableStorage : IStorage 
    {
        void StartMonitor();

        void AddTriples(List<Tuple<long, long, long>> triples);

        void RemoveTriples(List<Tuple<long, long, long>> triples);
    }
}
