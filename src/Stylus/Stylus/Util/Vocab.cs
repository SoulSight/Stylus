using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Stylus.Util
{
    public class Vocab
    {
        public static string RdfPrefix = @"http://www.w3.org/1999/02/22-rdf-syntax-ns";
        public static string RdfsPrefix = @"http://www.w3.org/2000/01/rdf-schema";
        public static string OwlPrefix = @"http://www.w3.org/2002/07/owl";

        // public static string DBpediaOntPrefix = @"http://dbpedia.org/ontology/";

        #region RDF
        public static string RdfType = @"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";
        #endregion

        #region RDFS
        public static string RdfsLabel = @"<http://www.w3.org/2000/01/rdf-schema#label>";
        public static string RdfsDomain = @"<http://www.w3.org/2000/01/rdf-schema#domain>";
        public static string RdfsRange = @"<http://www.w3.org/2000/01/rdf-schema#range>";
        public static string RdfsComment = @"<http://www.w3.org/2000/01/rdf-schema#comment>";
        public static string RdfsSubClassOf = @"<http://www.w3.org/2000/01/rdf-schema#subPropertyOf>";
        public static string RdfsSubPropertyOf = @"<http://www.w3.org/2000/01/rdf-schema#subClassOf>";
        #endregion

        #region OWL
        public static string OwlThing = @"<http://www.w3.org/2002/07/owl#Thing>";
        public static string OwlClass = @"<http://www.w3.org/2002/07/owl#Class>";
        public static string OwlObjectProperty = @"<http://www.w3.org/2002/07/owl#ObjectProperty>";
        public static string OwlDatatypeProperty = @"<http://www.w3.org/2002/07/owl#DatatypeProperty>";
        public static string OwlFunctionalProperty = @"<http://www.w3.org/2002/07/owl#FunctionalProperty>";
        public static string OwlInverseOf = @"<http://www.w3.org/2002/07/owl#inverseOf>";
        #endregion

        #region Literals
        public static string LiteralString = "<http://www.w3.org/2001/XMLSchema#string>";
        public static string LiteralInteger = "<http://www.w3.org/2001/XMLSchema#integer>";
        public static string LiteralDouble = "<http://www.w3.org/2001/XMLSchema#double>";
        public static string LiteralDate = "<http://www.w3.org/2001/XMLSchema#date>";
        public static string LiteralNonNegativeInteger = "<http://www.w3.org/2001/XMLSchema#nonNegativeInteger>";
        public static string LiteralPositiveInteger = "<http://www.w3.org/2001/XMLSchema#positiveInteger>";
        public static string LiteralGYear = "<http://www.w3.org/2001/XMLSchema#gYear>";
        public static string LiteralFloat = "<http://www.w3.org/2001/XMLSchema#float>";
        public static string LiteralBoolean = "<http://www.w3.org/2001/XMLSchema#boolean>";
        #endregion
    }
}
