using System;
using System.IO;
using System.IO.Compression;

namespace Kontur.LogPacker
{
    /// <summary>
    /// Используется алгоритм связной подмены дополнительным байтом (COBS):
    /// 1. Построчное чтение данных
    /// 2. Сравнение соседних строк
    /// 3. Если существует несколько подобных подстрок,
    ///    производится запись только длины подстроки, закодированной в одном байте (128 + длина)
    /// 4. Если существуют некоторые случайные двоичные данные,
    ///    то кодирование длины избегается
    /// </summary>
    public class LogPacker
    {
        private enum EndBytes
        {
            CarriageReturn = 0xD, 
            NewLine= 0xA, 
            Escape= 0x7F
        }
        
        private const byte InitialLength = 128;
        private const int BufferSize = 8192*2;

        public void Compress(FileStream inputStream, FileStream outputStream) 
        {
            using (var gZipStream = new GZipStream(outputStream, CompressionLevel.Optimal))
            using (var compressed = new BufferedStream(gZipStream, BufferSize))
                StartCompression(inputStream, compressed);
        }

        private static void StartCompression(FileStream inputStream, BufferedStream compressed)
        {
            var blocksAmount = inputStream.Length / BufferSize;
            if (inputStream.Length % BufferSize != 0) blocksAmount++;
            
            var buffer = new byte[BufferSize];
            var currentLine = new byte[BufferSize];
            var output = new byte[BufferSize];
            var lastLine = (byte[]) null;
            
            int bytesRead, currentLineCount = 0, lastLineCount = 0, blocks = 0;
            while ((bytesRead = inputStream.Read(buffer, 0, buffer.Length)) != 0)
            {
                blocks++;
                for (var i = 0; i < bytesRead; i++)
                {
                    var byt = buffer[i];
                    currentLine[currentLineCount++] = byt;
                    if (byt != (byte)EndBytes.NewLine && (i != bytesRead - 1 || blocks != blocksAmount)) continue;
                    WriteEncodedData(ref lastLine, currentLine, currentLineCount, compressed, lastLineCount, output);
                    lastLineCount = currentLineCount;
                    currentLineCount = 0;
                }
            }
        }

        private static void WriteEncodedData(ref byte[] lastLine, byte[] currentLine, int currentLineCount,
            BufferedStream compressed, int lastLineCount, byte[] output)
        {
            if (lastLine == null)
            {
                lastLine = new byte[BufferSize];
                Array.Copy(currentLine, lastLine, currentLineCount);
                compressed.Write(currentLine, 0, currentLineCount);
            }
            else
            {
                GetMaxString(lastLine, currentLine, currentLineCount, lastLineCount, 
                            out var maxString, out var minStringLength, out var maxStringLength);

                var outputIndex = FindOutputIndex(lastLine, currentLine, output, 
                                                  minStringLength, maxString, maxStringLength);

                compressed.Write(output, 0, outputIndex);
                Array.Copy(currentLine, lastLine, currentLineCount);
            }
        }
        
        private static void GetMaxString(byte[] lastLine, byte[] currentLine, int currentLineCount, int lastLineCount,
            out byte[] maxString, out int minStringLength, out int maxStringLength)
        {
            if (lastLineCount > currentLineCount)
            {
                maxString = lastLine;
                minStringLength = currentLineCount;
                maxStringLength = lastLineCount;
            }
            else
            {
                maxString = currentLine;
                minStringLength = lastLineCount;
                maxStringLength = currentLineCount;
            }
        }
        
        private static int FindOutputIndex(byte[] lastLine, byte[] currentLine, byte[] output, int minStringLength,
            byte[] maxString, int maxStringLength)
        {
            int length = 0, outputIndex = 0;
            byte lastByte = 0;
            for (var j = 0; j < minStringLength; j++)
                length = CheckLineLength(lastLine, currentLine, output, j, length, ref lastByte, ref outputIndex);

            if (length > 1)
            {
                output[outputIndex++] = (byte)EndBytes.Escape;
                output[outputIndex++] = Convert.ToByte(InitialLength + length);
            }
            else if (length == 1)
            {
                if (lastByte == (byte)EndBytes.Escape)
                    output[outputIndex++] = (byte)EndBytes.Escape;

                output[outputIndex++] = lastByte;
            }

            if (maxString != currentLine) return outputIndex;
            {
                for (var j = minStringLength; j < maxStringLength; j++)
                {
                    if (maxString[j] == (byte)EndBytes.Escape)
                        output[outputIndex++] = (byte)EndBytes.Escape;

                    output[outputIndex++] = maxString[j];
                }
            }
            return outputIndex;
        }

        private static int CheckLineLength(byte[] lastLine, byte[] currentLine, byte[] output, int j, int length,
            ref byte lastByte, ref int outputIndex)
        {
            if (lastLine[j] == currentLine[j] && currentLine[j] != (byte)EndBytes.NewLine &&
                currentLine[j] != (byte)EndBytes.CarriageReturn)
            {
                length++;
                lastByte = currentLine[j];
            }
            else
            {
                if (length > 1)
                {
                    output[outputIndex++] = (byte)EndBytes.Escape;
                    output[outputIndex++] = Convert.ToByte(InitialLength + length);
                    length = 0;
                }
                else if (length == 1)
                {
                    if (lastByte == (byte)EndBytes.Escape)
                        output[outputIndex++] = (byte)EndBytes.Escape;
                    
                    output[outputIndex++] = lastByte;
                    length = 0;
                }
                if (currentLine[j] == (byte)EndBytes.Escape)
                    output[outputIndex++] = (byte)EndBytes.Escape;
                
                output[outputIndex++] = currentLine[j];
            }
            return length;
        }

        public void Decompress(FileStream inputStream, FileStream outputStream) 
        {
            using (var gzipStream = new GZipStream(inputStream, CompressionMode.Decompress))
            using (var decompressed = new BufferedStream(gzipStream, BufferSize))
                StartDecompression(outputStream, decompressed);
        }

        private static void StartDecompression(FileStream outputStream, BufferedStream decompressed)
        {
            var buffer = new byte[BufferSize];
            var lastLine = (byte[]) null;
            var currentLine = new byte[BufferSize];
            var output = new byte[BufferSize];
            
            int bytesRead, currentLineCount = 0;
            while ((bytesRead = decompressed.Read(buffer, 0, buffer.Length)) != 0)
            {
                for (var i = 0; i < bytesRead; i++)
                {
                    var byt = buffer[i];
                    currentLine[currentLineCount++] = byt;
                    if (byt != (byte)EndBytes.NewLine && (i != bytesRead - 1 || bytesRead >= BufferSize)) continue;
                    WriteDecodedData(outputStream, ref lastLine, currentLine, currentLineCount, output);
                    currentLineCount = 0;
                }
            }
        }

        private static void WriteDecodedData(FileStream outputStream, ref byte[] lastLine, 
            byte[] currentLine, int currentLineCount, byte[] output)
        {
            if (lastLine == null)
            {
                lastLine = new byte[BufferSize];
                Array.Copy(currentLine, lastLine, currentLineCount);
                outputStream.Write(currentLine, 0, currentLineCount);
            }
            else
            {
                var escaped = false;
                int outputIndex = 0, lastLineIndex = 0;
                for (var j = 0; j < currentLineCount; j++)
                {
                    var currentByte = currentLine[j];
                    if (currentByte == (byte)EndBytes.Escape)
                    {
                        if (escaped)
                        {
                            output[outputIndex++] = currentByte;
                            lastLineIndex++;
                        }
                        escaped = !escaped;
                        continue;
                    }

                    if (currentByte > InitialLength && escaped)
                    {
                        escaped = false;
                        var length = currentByte - InitialLength;
                        for (var k = 0; k < length; k++)
                            output[outputIndex++] = lastLine[lastLineIndex + k];
                        
                        lastLineIndex += length;
                    }
                    else
                    {
                        output[outputIndex++] = currentByte;
                        lastLineIndex++;
                    }
                }
                outputStream.Write(output, 0, outputIndex);
                Array.Copy(output, lastLine, outputIndex);
            }
        }
    }
}