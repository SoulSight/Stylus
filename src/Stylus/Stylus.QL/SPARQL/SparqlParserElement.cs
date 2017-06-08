using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Stylus.QL.SPARQL
{
    internal enum ElementType 
    { 
        Variable, 
        Literal, 
        IRI 
    }

    internal enum ElementSubType 
    { 
        None, 
        CustomLanguage, 
        CustomType 
    }

    internal class Element
    {
        internal ElementType ElementType { set; get; }

        internal ElementSubType SubType { set; get; }

        internal string SubTypeValue { set; get; }

        internal string Value { set; get; }
    }

    internal class Pattern
    {
        internal Element Subj { set; get; }
        internal Element Pred { set; get; }
        internal Element Obj { set; get; }

        public Pattern(Element subj, Element pred, Element obj)
        {
            this.Subj = subj;
            this.Pred = pred;
            this.Obj = obj;
        }
    }

    internal enum FilterType
    {
        Or, And, Equal, NotEqual, Less, LessOrEqual, Greater, GreaterOrEqual, Plus, Minus, Mul, Div,
        Not, UnaryPlus, UnaryMinus, Literal, Variable, IRI, Function, ArgumentList,
        Builtin_str, Builtin_lang, Builtin_langmatches, Builtin_datatype, Builtin_bound, Builtin_sameterm,
        Builtin_isiri, Builtin_isblank, Builtin_isliteral, Builtin_regex, Builtin_in
    }

    internal class Filter
    {
        internal FilterType FilterType { set; get; }

        internal Filter Arg1 { set; get; }
        internal Filter Arg2 { set; get; }
        internal Filter Arg3 { set; get; }

        internal string Value { set; get; }
        internal string ValueType { set; get; }
        internal uint ValueArg { set; get; }
    }

    internal class PatternGroup
    {
        internal List<Pattern> Patterns { set; get; }

        internal List<Filter> Filters { set; get; }

        internal List<PatternGroup> Optional { set; get; }

        internal List<List<PatternGroup>> Unions { set; get; }
    }

    internal enum ProjectionModifier
    {
        Modifier_None, 
        Modifier_Distinct,
        Modifier_Reduced, 
        Modifier_Count, 
        Modifier_Duplicates
    }

    internal class Order
    {
        internal uint Id { set; get; }

        internal bool Descending { set; get; }
    }
}
