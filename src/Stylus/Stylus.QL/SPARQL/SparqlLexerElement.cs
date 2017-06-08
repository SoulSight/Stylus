using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Stylus.QL.SPARQL
{
    internal enum Token
    {
        None, 
        Error, 
        Eof, 
        IRI,
        String, 
        Variable, 
        Identifier,
        Colon, 
        Semicolon,
        Comma, 
        Dot, 
        Underscore,
        LCurly, 
        RCurly, 
        LParen, 
        RParen,
        LBracket, 
        RBracket, 
        Anon,
        Equal, 
        NotEqual, 
        Less, 
        LessOrEqual, 
        Greater, 
        GreaterOrEqual,
        At, 
        Type, 
        Not, 
        Or,
        And, 
        Plus, 
        Minus, 
        Mul, 
        Div,
        Integer, 
        Decimal, 
        Double
    }
}
