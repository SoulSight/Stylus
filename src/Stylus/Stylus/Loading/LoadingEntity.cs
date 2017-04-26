using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Stylus.Loading
{
    public class LoadingEntity
    {
        public string Literal { set; get; }

        public Dictionary<long, List<string>> POs { set; get; }
    }

    public class EncodedLoadingEntity
    {
        public long Literal { set; get; }

        public Dictionary<long, List<long>> POs { set; get; }
    }
}
