using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Stylus.DataModel;

namespace Stylus.Literal
{
    internal abstract class LiteralParser
    {
        public abstract bool CanParse(string str);

        public abstract Literal ParseLiteral(string str);

        public abstract bool CanParse(Literal literal);

        public abstract string ToString(Literal literal);

        public abstract IndexEntry ToIndexEntry(Literal literal, Func<string, int> str2prefixSuffix);

        public abstract bool CanParse(IndexEntry indexEntry);

        public abstract Literal FromIndexEntry(IndexEntry indexEntry, Func<int, string> prefixSuffix2str);
    }
}
