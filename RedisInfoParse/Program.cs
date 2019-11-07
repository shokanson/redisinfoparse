using System;
using System.IO;

namespace RedisInfoParse
{
    class RedisInfo
    {
        public DateTime InfoTimeUtc { get; set; }
        public long InstOpsThisSec { get; set; }
        public long OpsThisSec { get; set; }
        public long UsedMem { get; set; }
        public long BytesInThisSec { get; set; }
        public long BytesOutThisSec { get; set; }
    }

    class Program
    {
        static void Main(string[] args)
        {
            if (args == null || args.Length != 2) throw new Exception("must supply in-file and out-file as arguments");

            string inFilePath = args[0];
            string outFilePath = args[1];

            using (var inFile = File.OpenText(inFilePath))
            using (var outFile = new StreamWriter(outFilePath))
            {
                outFile.WriteLine("InfoTimeUtc,UsedMem,CmdThisSec,InstOpsThisSec,NetInBytesThisSec,NetOutBytesThisSec");

                RedisInfo redisInfo = null;
                long lastTcp = Int64.MaxValue;
                long lastTnib = Int64.MaxValue;
                long lastTnob = Int64.MaxValue;

                while (!inFile.EndOfStream)
                {
                    string line = inFile.ReadLine();
                    if (String.IsNullOrWhiteSpace(line)) continue;

                    if (line.StartsWith("[INFO]"))
                    {
                        redisInfo = new RedisInfo
                        {
                            InfoTimeUtc = DateTime.Parse(line.Substring(7))
                        };
                    }
                    else if (line.StartsWith("used_memory:"))
                    {
                        redisInfo.UsedMem = Int64.Parse(line.Substring(12));
                    }
                    else if (line.StartsWith("total_commands_processed:"))
                    {
                        long tcp = Int64.Parse(line.Substring(25));
                        if (tcp > lastTcp)
                        {
                            redisInfo.OpsThisSec = tcp - lastTcp;
                        }
                        lastTcp = tcp;
                    }
                    else if (line.StartsWith("instantaneous_ops_per_sec:"))
                    {
                        redisInfo.InstOpsThisSec = Int64.Parse(line.Substring(26));
                    }
                    else if (line.StartsWith("total_net_input_bytes:"))
                    {
                        long tnib = Int64.Parse(line.Substring(22));
                        if (tnib > lastTnib)
                        {
                            redisInfo.BytesInThisSec = tnib - lastTnib;
                        }
                        lastTnib = tnib;
                    }
                    else if (line.StartsWith("total_net_output_bytes:"))
                    {
                        long tnob = Int64.Parse(line.Substring(23));
                        if (tnob > lastTnob)
                        {
                            redisInfo.BytesOutThisSec = tnob - lastTnob;
                        }
                        lastTnob = tnob;
                        WriteInfo(outFile, redisInfo);
                    }
                }
            }
        }

        static void WriteInfo(StreamWriter file, RedisInfo redisInfo)
        {
            file.WriteLine($"{redisInfo.InfoTimeUtc.ToString("yyyy-MM-dd HH:mm:ss")},{redisInfo.UsedMem},{redisInfo.OpsThisSec},{redisInfo.InstOpsThisSec},{redisInfo.BytesInThisSec},{redisInfo.BytesOutThisSec}");
        }
    }
}
