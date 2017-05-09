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
            if (File.Exists(eidMapFilename))
            {
                File.Move(eidMapFilename, eidMapFilename + ".bak");
            }

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

        // Reference: http://stackoverflow.com/a/17993002/6324307
        public static string CompressString(string text)
        {
            byte[] buffer = Encoding.UTF8.GetBytes(text);
            var memoryStream = new MemoryStream();
            using (var gZipStream = new GZipStream(memoryStream, CompressionMode.Compress, true))
            {
                gZipStream.Write(buffer, 0, buffer.Length);
            }

            memoryStream.Position = 0;

            var compressedData = new byte[memoryStream.Length];
            memoryStream.Read(compressedData, 0, compressedData.Length);

            var gZipBuffer = new byte[compressedData.Length + 4];
            Buffer.BlockCopy(compressedData, 0, gZipBuffer, 4, compressedData.Length);
            Buffer.BlockCopy(BitConverter.GetBytes(buffer.Length), 0, gZipBuffer, 0, 4);
            return Convert.ToBase64String(gZipBuffer);
        }

        // Reference: http://stackoverflow.com/a/17993002/6324307
        public static string DecompressString(string compressedText)
        {
            byte[] gZipBuffer = Convert.FromBase64String(compressedText);
            using (var memoryStream = new MemoryStream())
            {
                int dataLength = BitConverter.ToInt32(gZipBuffer, 0);
                memoryStream.Write(gZipBuffer, 4, gZipBuffer.Length - 4);

                var buffer = new byte[dataLength];

                memoryStream.Position = 0;
                using (var gZipStream = new GZipStream(memoryStream, CompressionMode.Decompress))
                {
                    gZipStream.Read(buffer, 0, buffer.Length);
                }

                return Encoding.UTF8.GetString(buffer);
            }
        }
    }
}
