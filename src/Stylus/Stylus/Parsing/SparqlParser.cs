using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using VDS.RDF.Parsing;
using VDS.RDF.Query;
using VDS.RDF.Query.Patterns;

namespace Stylus.Parsing
{
    public class SparqlParser
    {
        public Func<string, string> FixStrFunc { set; get; }

        private SparqlQueryParser dotnetrdf_parser = new SparqlQueryParser();

        private SparqlQuery ParseSparqlQueryFromString(string queryStr) 
        {
            return dotnetrdf_parser.ParseFromString(queryStr);
        }

        private void ExtractPatternRecursive(GraphPattern gp, QueryGraph qg)
        {
            if (gp.IsUnion) // Process UNION expression of [?x p1 ?y . ?x p2 ?y . ... ?x pn ?y .]
            {
                throw new NotSupportedException();
            }

            // Terminal graph pattern
            if (gp.ChildGraphPatterns.Count == 0)
            {
                // If there are no Child Graph Patterns then this is a BGP
                foreach (var tp in gp.TriplePatterns.Cast<BaseTriplePattern>())
                {
                    ExtractTriplePattern(tp, qg);
                }
                if (gp.IsGraph)
                {
                    throw new NotSupportedException("Graph");
                }
                if (gp.IsService)
                {
                    throw new NotSupportedException("Service");
                }
                if (gp.UnplacedAssignments.Count() > 0)
                {
                    throw new NotSupportedException("Assignment");
                }
                if (gp.IsMinus)
                {
                    throw new NotSupportedException("Minus");
                }
                if (gp.IsOptional)
                {
                    throw new NotSupportedException("Optional");
                    #region placeholder
                    //if (gp.IsExists || gp.IsNotExists)
                    //{
                    //    // 
                    //}
                    //else
                    //{
                    //    if (gp.IsFiltered && gp.Filter != null)
                    //    {
                    //        // 
                    //    }
                    //    else
                    //    {
                    //        // 
                    //    }
                    //}
                    #endregion
                }

                // Apply Inline Data
                if (gp.HasInlineData)
                {
                    // qg.AddInlineData(gp.InlineData);
                    throw new NotSupportedException("Inline Data");
                }

                // Apply Filters
                if (gp.IsFiltered && (gp.Filter != null || gp.UnplacedFilters.Count() > 0))
                {
                    if (gp.IsOptional && !(gp.IsExists || gp.IsNotExists))
                    {
                        throw new NotSupportedException("Optional"); // placeholder
                    }

                    // subgraph.AddFilter(gp.Filter);
                    throw new NotSupportedException("Filter");
                }
                return;
            }

            // Apply Inline Data
            if (gp.HasInlineData)
            {
                // subgraph.AddInlineData(gp.InlineData);
                throw new NotSupportedException("Inline Data");
            }

            if (gp.IsFiltered && (gp.Filter != null || gp.UnplacedFilters.Count() > 0))
            {
                // subgraph.AddFilter(gp.Filter);
                throw new NotSupportedException("Filter");
            }

            if (gp.TriplePatterns.Count > 0)
            {
                foreach (var tp in gp.TriplePatterns.Cast<BaseTriplePattern>())
                {
                    ExtractTriplePattern(tp, qg);
                }
            }

            foreach (var cgp in gp.ChildGraphPatterns)
            {
                ExtractPatternRecursive(cgp, qg);
            }
        }

        private void ExtractTriplePattern(BaseTriplePattern tp, QueryGraph qg)
        {
            switch (tp.PatternType)
            {
                case TriplePatternType.Match:
                    var ntp = (TriplePattern)tp;
                    if (this.FixStrFunc == null)
                    {
                        string subj = ntp.Subject.ToString().Trim();
                        string pred = ntp.Predicate.ToString().Trim();
                        string obj = ntp.Object.ToString().Trim();
                        qg.AddMatch(subj, pred, obj);
                    }
                    else
                    {
                        string subj = FixStrFunc(ntp.Subject.ToString().Trim());
                        string pred = FixStrFunc(ntp.Predicate.ToString().Trim());
                        string obj = FixStrFunc(ntp.Object.ToString().Trim());
                        qg.AddMatch(subj, pred, obj);
                    }
                    break;
                case TriplePatternType.SubQuery:
                    ExtractPatternRecursive(((SubQueryPattern)tp).SubQuery.RootGraphPattern, qg);
                    break;
                case TriplePatternType.Filter:
                case TriplePatternType.BindAssignment:
                case TriplePatternType.LetAssignment:
                case TriplePatternType.Path:
                case TriplePatternType.PropertyFunction:
                default:
                    throw new Exception(tp.PatternType.ToString());
            }
        }

        private QueryGraph ParseQueryFromSparqlQuery(SparqlQuery query)
        {
            var selected_vars = query.Variables.Where(v => v.IsResultVariable).Select(v => v.ToString()).ToList();
            QueryGraph qg = new QueryGraph();
            qg.SelectedVariables = selected_vars;
            ExtractPatternRecursive(query.RootGraphPattern, qg);
            return qg;
        }

        public QueryGraph ParseQueryFromString(string queryStr) 
        {
            SparqlQuery sparql_query = ParseSparqlQueryFromString(queryStr);
            return ParseQueryFromSparqlQuery(sparql_query);
        }
    }
}
