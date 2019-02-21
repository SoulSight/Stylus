using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Stylus.DataModel;

namespace Stylus.Literal
{
    public class LiteralTool
    {
        private static readonly List<LiteralParser> chain = new List<LiteralParser>()
        {
            new UriLiteralParser(),
            new NumberLiteralParser(),
            new LangLiteralParser(),
            new RawLiteralParser()
        };

        private static Exception CannotParseException()
        {
            return new Exception("Parse literal error.");
        }

        public static Literal Parse(string str)
        {
            foreach (var parser in chain)
            {
                if (parser.CanParse(str))
                {
                    return parser.ParseLiteral(str);
                }
            }
            throw CannotParseException();
        }

        public static string ToString(Literal literal)
        {
            foreach (var parser in chain)
            {
                if (parser.CanParse(literal))
                {
                    return parser.ToString(literal);
                }
            }
            throw CannotParseException();
        }

        public static Literal FromIndexEntry(IndexEntry indexEntry, Func<int, string> prefixSuffix2str)
        {
            foreach (var parser in chain)
            {
                if (parser.CanParse(indexEntry))
                {
                    return parser.FromIndexEntry(indexEntry, prefixSuffix2str);
                }
            }

            throw CannotParseException();
        }

        public static IndexEntry ToIndexEntry(Literal literal, long literalCellId, Func<string, int> str2prefixSuffix)
        {
            foreach (var parser in chain)
            {
                if (parser.CanParse(literal))
                {
                    var indexEntry = parser.ToIndexEntry(literal, str2prefixSuffix);
                    indexEntry.EntityCellId = literalCellId;
                    return indexEntry;
                }
            }

            throw CannotParseException();
        }
    }
}
