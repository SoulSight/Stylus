using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Trinity;

using Stylus;
using Stylus.DataModel;
using Stylus.Distributed;
using Stylus.Loading;
using Stylus.Parsing;
using Stylus.Preprocess;
using Stylus.Query;
using Stylus.Util;

namespace Stylus.Console
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length == 0)
            {
                InteractiveConsole();
            }
            else // accept params
            {
                if (args.Length != 3)
                {
                    System.Console.WriteLine(ErrorInfo());
                    return;
                }
                TrinityConfig.StorageRoot = args[0];
                StylusConfig.SetStoreMetaRootDir(args[1]);
                bool unfold_isa;
                if (bool.TryParse(args[2], out unfold_isa))
                {
                    if (!unfold_isa)
                    {
                        StylusSchema.PredCandidatesForSynPred = new HashSet<string>();
                    }
                }
                else
                {
                    System.Console.WriteLine(ErrorInfo());
                    return;
                }

                var backup_foreground_color = System.Console.ForegroundColor;
                System.Console.ForegroundColor = ConsoleColor.DarkRed;

                System.Console.WriteLine();
                System.Console.WriteLine("=> " + StylusConsoleTerm.StorageRootName + ": " + TrinityConfig.StorageRoot);
                System.Console.WriteLine("=> " + StylusConsoleTerm.MetadataRootName + ": " + StylusConfig.GetStoreMetaRootDir());
                System.Console.WriteLine("=> " + StylusConsoleTerm.CombineIsAName + ": " + StylusSchema.PredCandidatesForSynPred.Contains(Vocab.RdfType));
                System.Console.WriteLine();
                
                System.Console.ForegroundColor = backup_foreground_color;
                
                InteractiveConsole();
            }
        }

        static void InteractiveConsole() 
        {
            var backup_foreground_color = System.Console.ForegroundColor;
            var backup_background_color = System.Console.BackgroundColor;

            ConsoleColor fg_color = ConsoleColor.DarkCyan;
            System.Console.ForegroundColor = fg_color;
            //System.Console.BackgroundColor = ConsoleColor.DarkGray;

            StylusCommandParser parser = new StylusCommandParser();
            string line;
            System.Console.WriteLine(WelcomeInfo());
            System.Console.ForegroundColor = backup_foreground_color;
            System.Console.Write(">> ");

            
            while ((line = System.Console.ReadLine()).Trim().ToLower() != "exit")
            {
                System.Console.ForegroundColor = fg_color;
                try
                {
                    ExecuteCmd(parser.Parse(line));
                }
                catch (Exception e)
                {
                    System.Console.WriteLine(e);
                }
                System.Console.ForegroundColor = backup_foreground_color;
                System.Console.Write(">> ");
            }

            System.Console.ForegroundColor = backup_foreground_color;
            System.Console.BackgroundColor = backup_background_color;
        }

        static string WelcomeInfo() 
        {
            string info = "\n";
            info += " ************************* Welcome to Stylus ************************* \n\n";
            info += HelpInfo();
            info += "\n";
            info += " ********************************************************************* \n";
            return info;
        }

        static string HelpInfo() 
        {
            string info = "";
            info += " Command list: \n";
            info += " +---------------------------\n";
            info += "  [-] set <key> <value>\n";
            info += "        <key> = {" + StylusConsoleTerm.MetadataRootName + ", "
                + StylusConsoleTerm.StorageRootName + ", " + StylusConsoleTerm.CombineIsAName + "}\n"; //, max_xudt, sep
            info += "        <value> = <dir_path> for {" + StylusConsoleTerm.MetadataRootName + ", "
                + StylusConsoleTerm.StorageRootName + "};\n"; //
            info += "                  <bool> for " + StylusConsoleTerm.CombineIsAName + "}\n"; //
            //info += "                  <int> for max_xudt;\n"; //
            //info += "                  <char> for sep}\n"; //
            info += "  [-] get <key> = {" + StylusConsoleTerm.MetadataRootName + ", "
                + StylusConsoleTerm.StorageRootName + ", " + StylusConsoleTerm.CombineIsAName + ", "
                + StylusConsoleTerm.MaxXUDTName + ", " + StylusConsoleTerm.SeperatorName + "}\n"; 
            info += "  [-] start -server|-proxy\n";
            info += " +---------------------------\n";
            info += "  [-] prepare <raw_nt_filename> [<paired_filename>]\n";
            info += "  [-] scan <paired_filename>\n";
            info += "  [-] assign <paired_filename>\n";
            info += "  [-] encode <paired_filename> <encoded_paired_filename>\n";
            info += "  [-] load <paired_filename>\n";
            info += "  [-] loadx <encoded_paired_filename>\n";
            info += " +---------------------------\n";
            info += "  [-] fast_load <raw_nt_filename> [<paired_filename>]\n";
            info += " +---------------------------\n";
            info += "  [-] repo\n";
            info += "  [-] query <query_filename>\n";
            info += " +---------------------------\n";
            //info += "  [-] convert <file_dir> <output_filename>\n";
            //info += "  [-] dload <paired_filename> <local_storage_dir>\n";
            //info += "  [-] dloadx <encoded_paired_filename> <local_storage_dir>\n";
            //info += "  [-] drepo\n";
            //info += "  [-] dstat\n";
            //info += "  [-] dquery <sparql_query_filename> [lubm]\n";
            //info += " +---------------------------\n";
            return info;
        }

        static string ErrorInfo() 
        {
            return "Cannot parse command line.";
        }

        private static SparqlParser parser = new SparqlParser();
        private static IQueryWorker q_server = null;
        static void ExecuteCmd(StylusCommand cmd) 
        {
            char sep = NTripleUtil.FieldSeparator;
            switch (cmd.Name)
            {
                case "set":
                    if (cmd.Parameters.Count != 2)
                    {
                        System.Console.WriteLine("Too less/many parameters for 'set'.");
                    }
                    else
                    {
                        switch (cmd.Parameters[0].ToLower())
                        {
                            case StylusConsoleTerm.MetadataRootName:
                                StylusConfig.SetStoreMetaRootDir(cmd.Parameters[1]);
                                System.Console.WriteLine("=> " + StylusConsoleTerm.MetadataRootName + ": " + StylusConfig.GetStoreMetaRootDir());
                                break;
                            case StylusConsoleTerm.StorageRootName:
                                TrinityConfig.StorageRoot = cmd.Parameters[1];
                                System.Console.WriteLine("=> " + StylusConsoleTerm.StorageRootName + ": " + TrinityConfig.StorageRoot);
                                break;
                            case StylusConsoleTerm.CombineIsAName:
                                bool combine_all_isA;
                                if (bool.TryParse(cmd.Parameters[1], out combine_all_isA))
                                {
                                    if (!combine_all_isA)
                                    {
                                        StylusSchema.PredCandidatesForSynPred = new HashSet<string>();
                                    }
                                }
                                else
                                {
                                    System.Console.WriteLine("<bool> param is required for 'set " + StylusConsoleTerm.CombineIsAName + "'.");
                                }
                                System.Console.WriteLine("=> " + StylusConsoleTerm.CombineIsAName + ": " + StylusSchema.PredCandidatesForSynPred.Contains(Vocab.RdfType));
                                break;
                            case StylusConsoleTerm.MaxXUDTName:
                                int max_xudt;
                                if (int.TryParse(cmd.Parameters[1], out max_xudt))
                                {
                                    System.Console.WriteLine("<int> param is required for 'set " + StylusConsoleTerm.MaxXUDTName + "'.");
                                }
                                else
                                {
                                    StylusConfig.MaxXudt = int.Parse(cmd.Parameters[1]);
                                }
                                System.Console.WriteLine("=> " + StylusConsoleTerm.MaxXUDTName + ": " + StylusConfig.MaxXudt);
                                break;
                            case StylusConsoleTerm.SeperatorName:
                                switch (cmd.Parameters[1])
	                            {
                                    case "\\t":
                                        NTripleUtil.FieldSeparator = '\t';
                                        break;
		                            default:
                                        NTripleUtil.FieldSeparator = cmd.Parameters[1][0];
                                        break;
	                            }
                                System.Console.WriteLine("=> " + StylusConsoleTerm.SeperatorName + ": " + NTripleUtil.FieldSeparator);
                                break;
                            default:
                                break;
                        }
                    }
                    break;
                case "get":
                    if (cmd.Parameters.Count != 1)
                    {
                        System.Console.WriteLine("Too less/many parameters for 'get'.");
                    }
                    else
                    {
                        switch (cmd.Parameters[0].ToLower())
                        {
                            case StylusConsoleTerm.MetadataRootName:
                                System.Console.WriteLine("=> " + StylusConsoleTerm.MetadataRootName + ": " + StylusConfig.GetStoreMetaRootDir());
                                break;
                            case StylusConsoleTerm.StorageRootName:
                                System.Console.WriteLine("=> " + StylusConsoleTerm.StorageRootName + ": " + TrinityConfig.StorageRoot);
                                break;
                            case StylusConsoleTerm.CombineIsAName:
                                System.Console.WriteLine("=> " + StylusConsoleTerm.CombineIsAName + ": " + StylusSchema.PredCandidatesForSynPred.Contains(Vocab.RdfType));
                                break;
                            case StylusConsoleTerm.MaxXUDTName:
                                System.Console.WriteLine("=> " + StylusConsoleTerm.MaxXUDTName + ": " + StylusConfig.MaxXudt);
                                break;
                            case StylusConsoleTerm.SeperatorName:
                                System.Console.WriteLine("=> " + StylusConsoleTerm.SeperatorName + ": " + NTripleUtil.FieldSeparator);
                                break;
                            default:
                                break;
                        }
                    }
                    break;
                case "start":
                    if (cmd.HasOption("server"))
                    {
                        TrinityConfig.CurrentRunningMode = RunningMode.Server;
                        SparqlDataServer server = new SparqlDataServer();
                        server.Start();
                    }
                    else if (cmd.HasOption("proxy"))
                    {
                        TrinityConfig.CurrentRunningMode = RunningMode.Proxy;
                        SparqlQueryCoordinator proxy = new SparqlQueryCoordinator();
                        proxy.Start();

                        System.Console.ForegroundColor = ConsoleColor.White;
                        System.Console.Write("? ");
                        string line;
                        while ((line = System.Console.ReadLine()).Trim().ToLower() != "exit")
                        {
                            System.Console.ForegroundColor = ConsoleColor.DarkCyan;
                            try
                            {
                                string[] splits = line.Split(' ');
                                switch (splits[0])
                                {
                                    case "dload":
                                        LoadFileInfoWriter load_msg = new LoadFileInfoWriter(splits[1], splits[2], bool.Parse(splits[3]));
                                        Parallel.For(0, Global.ServerCount, i =>
                                        {
                                            Global.CloudStorage.LoadFileToSparqlDataServer(i, load_msg);
                                        });
                                        break;
                                    case "drepo":
                                        //Global.CloudStorage.LoadStorage();
                                        Parallel.For(0, Global.ServerCount, i =>
                                        {
                                            Global.CloudStorage.LoadStorageToSparqlDataServer(i);
                                        });
                                        proxy.InitStatistics();
                                        break;
                                    case "dstat":
                                        proxy.InitStatistics();
                                        break;
                                    case "dloadx":
                                        load_msg = new LoadFileInfoWriter(splits[1], splits[2], bool.Parse(splits[3]));
                                        Parallel.For(0, Global.ServerCount, i =>
                                        {
                                            Global.CloudStorage.LoadEncodedFileToSparqlDataServer(i, load_msg);
                                        });
                                        break;
                                    case "dquery":
                                        if (splits.Length == 3 && splits[2] == "fix")
                                        {
                                            proxy.SetParserFixFunc(FixUriString);
                                        }
                                        SparqlQueryWriter query_msg = new SparqlQueryWriter(File.ReadAllText(splits[1]));
                                        System.Console.WriteLine("Query from File: " + splits[1]);
                                        //Global.CloudStorage.ExecuteQueryToSparqlDataProxy(0, query_msg);
                                        //var results = proxy.ExecuteQuery(query_msg);
                                        var results = proxy.ExecuteQueryWithParallelJoin(query_msg);

                                        int min = Math.Min(results.Records.Count, 10);
                                        if (min > 0)
                                        {
                                            foreach (var sol in proxy.ResolveQueryResults(results).Take(min))
                                            {
                                                System.Console.WriteLine(string.Join(", ", sol));
                                            }
                                            if (results.Records.Count > 10)
                                            {
                                                System.Console.WriteLine("More...");
                                            }
                                        }
                                        break;
                                    default:
                                        break;
                                }
                            }
                            catch (Exception e)
                            {
                                System.Console.WriteLine(e);
                            }
                            System.Console.ForegroundColor = ConsoleColor.White;
                            System.Console.Write("? ");
                        }
                    }
                    else
                    {
                        System.Console.ForegroundColor = ConsoleColor.DarkCyan;
                        System.Console.WriteLine(ErrorInfo());
                    }
                    break;
                case "prepare":
                    if (cmd.Parameters.Count < 1)
                    {
                        System.Console.WriteLine("Too less params for 'prepare' command.");
                    }
                    else if (cmd.Parameters.Count < 2)
                    {
                        System.Console.WriteLine("Save to path: " + cmd.Parameters[0] + ".paired");
                        Preprocessor.PrepareFile(cmd.Parameters[0], cmd.Parameters[0] + ".paired", sep);
                    }
                    else if (cmd.Parameters.Count == 2)
                    {
                        Preprocessor.PrepareFile(cmd.Parameters[0], cmd.Parameters[1], sep);
                    }
                    else
                    {
                        System.Console.WriteLine("Too many params for 'prepare' command.");
                    }
                    break;
                case "load":
                    TrinityConfig.CurrentRunningMode = RunningMode.Embedded;
                    //StylusSchema.ScanFrom(cmd.Parameters[0], sep);
                    //DataLoader.Load(cmd.Parameters[0], sep);
                    DataScanner.LoadFile(cmd.Parameters[0], sep);
                    break;
                case "repo":
                    TrinityConfig.CurrentRunningMode = RunningMode.Embedded;
                    TrinityConfig.ReadOnly = true;
                    Global.LocalStorage.LoadStorage();
                    break;
                case "query":
                    TrinityConfig.CurrentRunningMode = RunningMode.Embedded;
                    if (cmd.Parameters.Count > 1 && cmd.Parameters[1] == "fix")
                    {
                        parser.FixStrFunc = FixUriString;
                    }
                    else
                    {
                        parser.FixStrFunc = null;
                    }
                    if (q_server == null)
                    {
                        q_server = new ParallelQueryWorker();
                        //IQueryWorker q_server = new LinearQueryWorker();
                        //IQueryWorker q_server = new ParallelQueryWorker();
                        //IQueryWorker q_server = new XParallelQueryWorker(); // some issues
                    }
                    Query(q_server, parser, File.ReadAllText(cmd.Parameters[0]));
                    break;
                case "scan":
                    TrinityConfig.CurrentRunningMode = RunningMode.Embedded;
                    StylusSchema.ScanFrom(cmd.Parameters[0], sep);
                    break;
                case "assign":
                    TrinityConfig.CurrentRunningMode = RunningMode.Embedded;
                    DataScanner.AssignEids(cmd.Parameters[0], sep);
                    break;
                case "fast_load":
                    break;
                    TrinityConfig.CurrentRunningMode = RunningMode.Embedded;
                    string srcFilename = cmd.Parameters[0];
                    string pairedFilename = null;
                    if (cmd.Parameters.Count < 1)
                    {
                        System.Console.WriteLine("Too less params for 'prepare' command.");
                        break;
                    }
                    else if (cmd.Parameters.Count < 2)
                    {
                        pairedFilename = srcFilename + ".paired";
                    }
                    else if (cmd.Parameters.Count == 2)
                    {
                        pairedFilename = cmd.Parameters[1];
                    }
                    else
                    {
                        System.Console.WriteLine("Too many params for 'prepare' command.");
                        break;
                    }
                    System.Console.WriteLine("Paired file save to: " + pairedFilename);
                    Preprocessor.PrepareFile(srcFilename, pairedFilename, sep);
                    StylusSchema.ScanFrom(pairedFilename, sep);
                    DataScanner.AssignEids(pairedFilename, sep);
                    DataScanner.LoadFile(pairedFilename, sep);
                    break;
                case "encode":
                    TrinityConfig.CurrentRunningMode = RunningMode.Embedded;
                    DataScanner.EncodeFile(cmd.Parameters[0], cmd.Parameters[1], sep);
                    break;
                case "loadx":
                    if (cmd.HasOption("role", "embedded"))
                    {
                        TrinityConfig.CurrentRunningMode = RunningMode.Embedded;
                    }
                    else if (cmd.HasOption("role", "server"))
                    {
                        TrinityConfig.CurrentRunningMode = RunningMode.Server;
                    }
                    else if (cmd.HasOption("role", "proxy"))
                    {
                        TrinityConfig.CurrentRunningMode = RunningMode.Proxy;
                    }
                    else
                    {
                        TrinityConfig.CurrentRunningMode = RunningMode.Server;
                    }
                    DataScanner.LoadEncodedFile(cmd.Parameters[0], sep);
                    break;
                case "statx":
                    DataScanner.GenStatFromEncodedFile(cmd.Parameters[0], cmd.Parameters[1], sep);
                    break;
                default:
                    System.Console.WriteLine(ErrorInfo());
                    break;
            }
        }

        private static void Query(IQueryWorker server, SparqlParser parser, string query_str, bool peak = true)
        {
            var query = parser.ParseQueryFromString(query_str);
            var plan = server.Plan(query);
            Stopwatch sw = new Stopwatch();
            sw.Start();
            var results = server.Execute(plan);
            sw.Stop();
            System.Console.WriteLine("=> {0} results, {1} ms", results.Records.Count, sw.Elapsed.TotalMilliseconds);
            if (results.Records.Count > 0)
            {
                System.Console.WriteLine("[" + string.Join(", ", query.SelectedVariables) + "]");
            }

            if (peak)
            {
                int min = Math.Min(results.Records.Count, 10);
                if (min > 0)
                {
                    foreach (var sol in server.ResolveQuerySolutions(results).Take(min))
                    {
                        System.Console.WriteLine(string.Join(", ", sol));
                    }
                    if (results.Records.Count > 10)
                    {
                        System.Console.WriteLine((results.Records.Count - 10) + " ...more");
                    }
                }
            }
            else
            {
                foreach (var sol in server.ResolveQuerySolutions(results))
                {
                    System.Console.WriteLine(string.Join(", ", sol));
                }
            }
        }

        private static string FixUriString(string str) // for LUBM queries
        {
            if (str.Contains("/>"))
            {
                str = str.Replace("/>", ">");
                string[] segments = str.Split('.');
                // The first and last are fine
                for (int i = 1; i < segments.Length - 1; i++)
                {
                    segments[i] = string.Concat(char.ToUpper(segments[i][0]), segments[i].Substring(1));
                }
                return string.Join(".", segments);
            }
            else
            {
                return str;
            }
        }
    }
}
