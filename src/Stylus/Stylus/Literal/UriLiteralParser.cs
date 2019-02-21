using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Stylus.DataModel;

namespace Stylus.Literal
{
    // <http:xxx/xxx>
    internal class UriLiteralParser : LiteralParser
    {
        public override bool CanParse(string str)
        {
            return str.StartsWith("<") && str.EndsWith(">");
        }

        public override bool CanParse(Literal literal)
        {
            return literal.LiteralType == LiteralType.URI;
        }

        public override bool CanParse(IndexEntry indexEntry)
        {
            return indexEntry.LiteralType == (int)LiteralType.URI;
        }

        public override Literal FromIndexEntry(IndexEntry indexEntry, Func<int, string> prefixSuffix2str)
        {
            return new Literal()
            {
                LiteralType = LiteralType.URI,
                PrefixSuffix = prefixSuffix2str(indexEntry.PrefixSuffixId),
                Value = indexEntry.EntryKey.ToString()
            };
        }

        public override Literal ParseLiteral(string str)
        {
            Literal ret = new Literal
            {
                LiteralType = LiteralType.URI
            };
            str = str.Substring(1, str.Length - 2);
            int backslashIdx = str.LastIndexOf('/');
            if (backslashIdx > 0)
            {
                ret.PrefixSuffix = str.Substring(0, backslashIdx);
                ret.Value = str.Substring(backslashIdx + 1);
            }
            else
            {
                ret.PrefixSuffix = "";
                ret.Value = str;
            }
            return ret;
        }

        public override IndexEntry ToIndexEntry(Literal literal, Func<string, int> str2prefixSuffix)
        {
            return new IndexEntry()
            {
                LiteralType = (int)LiteralType.URI,
                PrefixSuffixId = str2prefixSuffix(literal.PrefixSuffix),
                EntryKey = literal.Value
            };
        }

        public override string ToString(Literal literal)
        {
            StringBuilder builder = new StringBuilder();
            builder.Append("<");
            builder.Append(literal.PrefixSuffix);
            builder.Append("/");
            builder.Append(literal.Value);
            builder.Append(">");
            return builder.ToString();
        }
    }
}
