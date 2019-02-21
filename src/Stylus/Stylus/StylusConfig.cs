using Stylus.Util;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Trinity;

namespace Stylus
{
    public class StylusConfig
    {
        public static readonly string AppName = "Stylus";

        internal static readonly string PidMapFilename = "pid.map";
        internal static readonly string SynPredMapFilename = "synthetic.map";
        internal static readonly string EidMapFilename = "eid.map";
        internal static readonly string LiteralPrefixSuffixFilename = "prefix_suffix.dat";
        internal static readonly string xUDTFilename = "xUDT.dat";
        internal static readonly string xUDTStatisticsFilename = "xUDT_stat.dat";
        internal static readonly string xUDTClusterStatisticsFilename = "xUDT_stat.cluster.dat";

        internal static readonly long SchemaCell = 0L;
        internal static readonly ushort LiteralTid = (ushort)0;
        internal static readonly ushort GenericTid = (ushort)1;
        internal static readonly ushort TidStart = (ushort)2;

        private static int max_xudt = 50000;
        private static string storeMetaRootDir { set; get; }

        public static void SetStoreMetaRootDir(string root) 
        {
            storeMetaRootDir = root;
        }

        public static string GetStoreMetaRootDir()
        {
            if (storeMetaRootDir == null)
            {
                return TrinityConfig.StorageRoot + AppName + "\\";
            }
            else
            {
                return storeMetaRootDir;
            }
        }

        public static string ComposeStoreMetaRootDir(string root = null) 
        {
            if (root == null)
            {
                return TrinityConfig.StorageRoot + "/" + AppName + "/";
            }
            else
            {
                return root + "/" + AppName + "/";
            }
        }

        public static int MaxXudt { set { max_xudt = value; } get { return max_xudt; } }

        public static bool CombineIsA
        {
            set
            {
                if (!value)
                {
                    StylusSchema.PredCandidatesForSynPred = new HashSet<string>();
                }
                else
                {
                    StylusSchema.PredCandidatesForSynPred = new HashSet<string>() { Vocab.RdfType };
                }
            }
            get
            {
                return StylusSchema.PredCandidatesForSynPred.Contains(Vocab.RdfType);
            }
        }

        public static ParallelOptions DegreeOfParallelismOption = new ParallelOptions
        {
            MaxDegreeOfParallelism
                = Environment.ProcessorCount - 1
        };

        internal static readonly int RefreshInterval = 10000; // milliseconds
    }
}
