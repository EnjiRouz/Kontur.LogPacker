using System;
using System.IO;

namespace Kontur.LogPacker
{
    internal static class EntryPoint
    {
        private const string TempFile = "tempFile.txt";

        public static void Main(string[] args)
        {
            if (args.Length == 2)
            {
                var (inputFile, outputFile) = (args[0], args[1]);
                if (File.Exists(inputFile))
                {
                    Compress(inputFile, outputFile);
                    return;
                }
            }

            if (args.Length == 3 && args[0] == "-d")
            {
                var (inputFile, outputFile) = (args[1], args[2]);
                if (File.Exists(inputFile))
                {
                    Decompress(inputFile, outputFile);
                    return;
                }
            }
            ShowUsage();
        }

        private static void ShowUsage()
        {
            Console.WriteLine("Usage:");
            Console.WriteLine($"{AppDomain.CurrentDomain.FriendlyName} [-d] <inputFile> <outputFile>");
            Console.WriteLine("\t-d flag turns on the decompression mode");
            Console.WriteLine();
        }

        private static void Compress(string inputFile, string outputFile) 
        {
            using (var inputStream = File.OpenRead(inputFile))
            using (var outputStream = File.OpenWrite(outputFile))
                new LogPacker().Compress(inputStream, outputStream);
        }

        private static void Decompress(string inputFile, string outputFile) 
        {
            using (var inputStream = File.OpenRead(inputFile))
            using (var outputStream = File.OpenWrite(outputFile))
                new LogPacker().Decompress(inputStream, outputStream);
        }
    }
}