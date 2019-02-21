using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Stylus.Literal
{
    public enum LiteralType
    {
        URI, // <http:xxx/xxx>
        LANG, // "xxx"@en
        NUMBER, // "xxx"^^<xsd:Integer>
        RAW // "xxx" or xxx
    }

    public class Literal
    {
        public LiteralType LiteralType { set; get; }

        public string PrefixSuffix { set; get; }

        public string Value { set; get; }
    }
}
