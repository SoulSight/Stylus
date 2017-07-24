using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Stylus.Parsing
{
    public class QueryGraph
    {
        public QueryGraph() 
        {
            this.SelectedVariables = new List<string>();
            this.NameToNodes = new Dictionary<string, QueryNode>();
            this.Edges = new List<QueryEdge>();
            this.VarPredFreq = new Dictionary<string, int>();
        }

        public List<string> SelectedVariables { set; get; }

        public Dictionary<string, int> VarPredFreq { set; get; }

        public Dictionary<string, QueryNode> NameToNodes { set; get; }

        public List<QueryEdge> Edges { set; get; }

        private void VerifyNode(string str) 
        {
            QueryNode qn = new QueryNode();
            qn.Name = str;
            qn.IsVariable = str.StartsWith("?") || str.StartsWith("$");
            qn.ParticipatedEdges = new List<QueryEdge>();
            NameToNodes.Add(str, qn);
        }

        public void AddMatch(string subj, string pred, string obj) 
        {
            if (!NameToNodes.ContainsKey(subj))
            {
                VerifyNode(subj);
            }
            if (!NameToNodes.ContainsKey(obj))
            {
                VerifyNode(obj);
            }

            QueryEdge qe = new QueryEdge();
            qe.SrcNode = NameToNodes[subj];
            qe.Predicate = pred;
            qe.TgtNode = NameToNodes[obj];
            Edges.Add(qe);

            NameToNodes[subj].ParticipatedEdges.Add(qe);
            NameToNodes[obj].ParticipatedEdges.Add(qe);

            if (pred.StartsWith("?"))
            {
                if (!this.VarPredFreq.ContainsKey(pred))
                {
                    this.VarPredFreq.Add(pred, 1);
                }
                else
                {
                    this.VarPredFreq[pred] += 1;
                }
            }
        }

        // To be selected for further query processing
        public bool ToSelect(QueryNode node) 
        {
            if (node.ParticipatedEdges.Count > 1 || SelectedVariables.Contains(node.Name))
            {
                return true;
            }
            return false;
        }

        public bool ToSelectVarPred(string varPred) 
        {
            if (this.VarPredFreq.ContainsKey(varPred) && this.VarPredFreq[varPred] > 1)
            {
                return true;
            }
            if (this.SelectedVariables.Contains(varPred))
            {
                return true;
            }
            return false;
        }

        public override string ToString()
        {
            StringBuilder builder = new StringBuilder();
            builder.AppendLine("SELECT: " + string.Join(" ", SelectedVariables));
            foreach (var e in Edges)
            {
                builder.AppendLine("EDGE: " + e.ToString());
            }
            return builder.ToString();
        }
    }

    public class QueryNode 
    {
        public string Name { set; get; }

        public bool IsVariable { set; get; }

        public List<QueryEdge> ParticipatedEdges { set; get; }

        public List<string> GetStarShapePred() 
        {
            return GetStarShapePred(ParticipatedEdges);
        }

        public List<string> GetStarShapePred(IEnumerable<QueryEdge> edges)
        {
            return edges
                .Select(pe => pe.SrcNode == this ? pe.Predicate : "_" + pe.Predicate)
                .ToList();
        }

        public List<string> GetStarShapePredIncludingSyn() 
        {
            return GetStarShapePredIncludingSyn(ParticipatedEdges);
        }

        public List<string> GetStarShapePredIncludingSyn(IEnumerable<QueryEdge> edges)
        {
            List<string> results = new List<string>();
            foreach (var pe in edges)
            {
                if (pe.SrcNode == this && !pe.TgtNode.IsVariable
                    && StylusSchema.IsSynPred(pe.Predicate, pe.TgtNode.Name))
                {
                    string synpred = StylusSchema.ConcatSynPred(pe.Predicate, pe.TgtNode.Name);
                    results.Add(synpred);
                }
                else
                {
                    results.Add(pe.SrcNode == this ? pe.Predicate : "_" + pe.Predicate);
                }
            }
            return results;
        }

        public override string ToString()
        {
            return Name;
        }
    }

    public class QueryEdge 
    {
        public QueryNode SrcNode { set; get; }

        public string Predicate { set; get; }

        public QueryNode TgtNode { set; get; }

        public override string ToString()
        {
            return SrcNode + " - " + Predicate + " -> " + TgtNode;
        }
    }
}
