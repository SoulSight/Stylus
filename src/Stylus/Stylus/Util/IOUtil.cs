using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Stylus.Util
{
    public class IOUtil
    {
        private static string GetEidMapFilename()
        {
            return StylusConfig.GetStoreMetaRootDir() + StylusConfig.EidMapFilename;
        }

        public static void LoadEidMapFile(Action<string, long> action) 
        {
            var eidMapFilename = GetEidMapFilename();
            FileStream fs_read = new FileStream(eidMapFilename, FileMode.Open);
            Console.WriteLine("Loading from " + eidMapFilename);
            
            using (StreamReader reader = new StreamReader(new GZipStream(fs_read, CompressionMode.Decompress)))
            {
                string line; 
                long cnt = 0;
                while ((line = reader.ReadLine()) != null)
                {
                    if (++cnt % 1000000 == 0)
                    {
                        Console.WriteLine("Loading Eid Mapping: " + cnt);
                    }
                    string[] splits = line.Split('\t');
                    if (splits.Length < 2)
                    {
                        continue;
                    }
                    string literal = splits[0];
                    long eid = long.Parse(splits[1]);
                    action(literal, eid);
                }
                Console.WriteLine("Loading Eid Mapping: " + cnt);
            }
        }

        public static void SaveEidMapFile(IEnumerable<KeyValuePair<string, long>> kvps)
        {
            var eidMapFilename = GetEidMapFilename();

            FileStream fs = new FileStream(eidMapFilename, FileMode.OpenOrCreate);
            using (StreamWriter writer = new StreamWriter(new GZipStream(fs, CompressionMode.Compress)))
            {
                long cnt = 0;
                foreach (var kvp in kvps)
                {
                    if (++cnt % 1000000 == 0)
                    {
                        Console.WriteLine("Save Eids: " + cnt);
                    }
                    writer.WriteLine(kvp.Key + "\t" + kvp.Value);
                }
                Console.WriteLine("Save Eids: " + cnt);
            }
        }
    }
}
