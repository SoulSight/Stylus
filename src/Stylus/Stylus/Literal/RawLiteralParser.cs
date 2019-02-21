using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Stylus.DataModel;

namespace Stylus.Literal
{
    // "xxx" or xxx
    internal class RawLiteralParser : LiteralParser
    {
        public override bool CanParse(string str)
        {
            return true;
        }

        public override bool CanParse(Literal literal)
        {
            return literal.LiteralType == LiteralType.RAW;
        }

        public override bool CanParse(IndexEntry indexEntry)
        {
            return indexEntry.LiteralType == (int)LiteralType.RAW;
        }

        public override Literal FromIndexEntry(IndexEntry indexEntry, Func<int, string> prefixSuffix2str)
        {
            return new Literal
            {
                LiteralType = LiteralType.RAW,
                PrefixSuffix = prefixSuffix2str(indexEntry.PrefixSuffixId),
                Value = indexEntry.EntryKey.ToString()
            };
        }

        public override Literal ParseLiteral(string str)
        {
            Literal ret = new Literal
            {
                LiteralType = LiteralType.RAW
            };
            if (str.StartsWith("\"") && str.EndsWith("\""))
            {
                ret.PrefixSuffix = "\"";
                ret.Value = str.Substring(1, str.Length - 2);
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
                LiteralType = (int)LiteralType.RAW,
                PrefixSuffixId = str2prefixSuffix(literal.PrefixSuffix),
                EntryKey = literal.Value
            };
        }

        public override string ToString(Literal literal)
        {
            return literal.PrefixSuffix + literal.Value + literal.PrefixSuffix;
        }
    }
}
