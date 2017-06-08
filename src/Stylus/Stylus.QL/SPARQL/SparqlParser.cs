using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Stylus.QL.SPARQL
{
    internal class SparqlParser
    {
        protected SparqlLexer lexer;
        protected Dictionary<string, string> prefixes;
        protected Dictionary<string, uint> namedVariables;
        protected uint variableCount;

        protected ProjectionModifier projectionModifier;
        protected List<uint> projection;
        protected PatternGroup patterns;
        protected List<Order> order;
        protected uint limit;

        internal uint NameVariable(string name) 
        {
            throw new NotImplementedException();
        }

        internal void ParseRDFLiteral(string value, ElementSubType subType, string valueType)
        {
            throw new NotImplementedException();
        }

        internal Filter ParseIRIrefOrFunction(Dictionary<string, uint> localVars, bool mustCall)
        {
            throw new NotImplementedException();
        }

        internal Filter ParseBuiltInCall(Dictionary<string, uint> localVars)
        {
            throw new NotImplementedException();
        }

        internal Filter ParsePrimaryExpression(Dictionary<string, uint> localVars)
        {
            throw new NotImplementedException();
        }

        internal Filter ParseUnaryExpression(Dictionary<string, uint> localVars)
        {
            throw new NotImplementedException();
        }

        internal Filter ParseMultiplicativeExpression(Dictionary<string, uint> localVars)
        {
            throw new NotImplementedException();
        }

        internal Filter ParseAdditiveExpression(Dictionary<string, uint> localVars)
        {
            throw new NotImplementedException();
        }

        internal Filter ParseNumericExpression(Dictionary<string, uint> localVars)
        {
            throw new NotImplementedException();
        }

        internal Filter ParseRelationalExpression(Dictionary<string, uint> localVars)
        {
            throw new NotImplementedException();
        }

        internal Filter ParseValueLogical(Dictionary<string, uint> localVars)
        {
            throw new NotImplementedException();
        }

        internal Filter ParseConditionalAndExpression(Dictionary<string, uint> localVars)
        {
            throw new NotImplementedException();
        }

        internal Filter ParseConditionalOrExpression(Dictionary<string, uint> localVars)
        {
            throw new NotImplementedException();
        }

        internal Filter ParseExpression(Dictionary<string, uint> localVars)
        {
            throw new NotImplementedException();
        }

        internal Filter ParseBrackettedExpression(Dictionary<string, uint> localVars)
        {
            throw new NotImplementedException();
        }

        internal Filter ParseConstraint(Dictionary<string, uint> localVars)
        {
            throw new NotImplementedException();
        }

        internal void ParseFilter(PatternGroup group, Dictionary<string, uint> localVars)
        {
            throw new NotImplementedException();
        }

        internal Element ParsePatternElement(PatternGroup group, Dictionary<string, uint> localVars)
        {
            throw new NotImplementedException();
        }

        internal Element ParseBlankNode(PatternGroup group, Dictionary<string, uint> localVars)
        {
            throw new NotImplementedException();
        }

        internal void ParseGraphPattern(PatternGroup group)
        {
            throw new NotImplementedException();
        }

        internal void ParseGroupGraphPattern(PatternGroup group)
        {
            throw new NotImplementedException();
        }

        internal void ParsePrefix()
        {
            throw new NotImplementedException();
        }

        internal void ParseProjection()
        {
            throw new NotImplementedException();
        }

        internal void ParseFrom()
        {
            throw new NotImplementedException();
        }

        internal void ParseWhere()
        {
            throw new NotImplementedException();
        }

        internal void ParseOrderBy()
        {
            throw new NotImplementedException();
        }

        internal void ParseLimit()
        {
            throw new NotImplementedException();
        }

        internal void Parse(bool multiQuery = false)
        {
            throw new NotImplementedException();
        }

        internal PatternGroup GetPatterns() 
        { 
            return patterns; 
        }

        internal string GetVariableName(uint id)
        {
            throw new NotImplementedException();
        }

        internal ProjectionModifier GetProjectionModifier() 
        { 
            return projectionModifier; 
        }
        
        internal uint GetLimit() 
        { 
            return limit; 
        }
    }
}
