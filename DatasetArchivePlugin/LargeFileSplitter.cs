using PRISM;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace DatasetArchivePlugin
{
    /// <summary>
    /// Split large files into smaller chunks and arrange according to desired directory structure; split file naming scheme follows GNU split scheme
    /// </summary>
    public class LargeFileSplitter : EventNotifier
    {
        private readonly List<string> outputDirectories;
        private readonly List<string> outputPaths = new List<string>();
        private readonly List<string> recombineBatFileContents;
        private readonly FileInfo inputFileInfo;
        private readonly long splitSizeMB;
        private readonly long splitSizeGB;
        private readonly string splitBaseName;
        private readonly string recombineBatFilePath;

        public IReadOnlyList<string> OutputDirectories => outputDirectories;
        public IReadOnlyList<string> OutputSplits => outputPaths;
        public IReadOnlyList<string> RecombineBatFileContents => recombineBatFileContents;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="inputFile">file to be split, e.g. [dataset_path]\[dataset_name.d]\ser</param>
        /// <param name="outputBasePath">Base path and directory name to use for the directories containing groups of split files, e.g. [dataset_path]\[dataset_name]_ser_files_part</param>
        /// <param name="splitsPerOutputDirectory">Number of split files to include in each directory</param>
        /// <param name="splitSizeGB">Size of each split file (excluding the last one)</param>
        /// <param name="splitBaseName">First part of the name for each split file, e.g 'ser_part-'</param>
        public LargeFileSplitter(FileInfo inputFile, string outputBasePath, int splitsPerOutputDirectory, int splitSizeGB, string splitBaseName = "")
        {
            inputFileInfo = inputFile;
            this.splitSizeGB = splitSizeGB;
            splitSizeGB = Math.Max(splitSizeGB, 1);
            var fileSize = inputFile.Length;
            splitSizeMB = splitSizeGB * 1024;

            var chunkSizeBytes = (long)splitSizeGB * 1024 * 1024 * 1024;
            var splitCount = (int)Math.Ceiling((double)fileSize / chunkSizeBytes);

            if (string.IsNullOrWhiteSpace(splitBaseName))
            {
                splitBaseName = $"{inputFile.Name.Replace(".", "_")}_part-";
            }

            this.splitBaseName = splitBaseName;

            var outDirs = new List<string>(splitCount);
            for (var i = 0; i < splitCount; i++)
            {
                var splitName = $"{splitBaseName}{GetSplitSuffix(i + 1)}";
                var dirNumber = i / splitsPerOutputDirectory + 1;
                var outputDirectory = $"{outputBasePath}{dirNumber:D2}";
                outDirs.Add(outputDirectory);
                outputPaths.Add(Path.Combine(outputDirectory, splitName));
            }

            outputDirectories = outDirs.Distinct().ToList();

            recombineBatFileContents = CreateRecombineBatFileData(inputFileInfo, outputDirectories, splitBaseName);
            recombineBatFilePath = Path.Combine(inputFileInfo.Directory.FullName, $"Recombine_{inputFile.Name.Replace(".", "_")}_files.bat");
        }

        private static List<string> CreateRecombineBatFileData(FileInfo inputFileInfo, List<string> outputDirectories, string splitBaseName)
        {
            var fileContents = new List<string>();

            fileContents.Add($"rem Use the following command to combine the split {inputFileInfo.Name} files");
            fileContents.Add("");
            foreach (var entry in outputDirectories)
            {
                var relativePath = GetRelativePath(inputFileInfo.Directory.FullName, entry);
                var filter = Path.Combine(relativePath, splitBaseName) + "*";
                fileContents.Add($"move {filter} .");
            }

            fileContents.Add("");
            // We can alternatively use 'cat [splitBaseName]?? > [inputFileInfo.Name]', but that requires additional software
            fileContents.Add($"copy /b {splitBaseName}?? {inputFileInfo.Name}");
            fileContents.Add("");

            return fileContents;
        }

        /// <summary>
        /// Get the suffix for the split file
        /// </summary>
        /// <param name="splitNumber">1-based split file number</param>
        /// <returns>2-character suffix</returns>
        private static string GetSplitSuffix(int splitNumber)
        {
            var zeroBased = splitNumber - 1;
            // TODO: support longer suffixes?
            // Divide by 26 for first character
            var c1 = (char)('a' + zeroBased / 26);
            var c2 = (char)('a' + zeroBased % 26);
            return $"{c1}{c2}";
        }

        /// <summary>
        /// Split the input file, verify the split files vs. the input file, creates recombine_xxx_files.bat, and then delete the input file
        /// </summary>
        /// <param name="errorMessage"></param>
        /// <param name="doNotDelete">Set to true to not delete the original file; for testing/troubleshooting purposes</param>
        /// <returns></returns>
        public bool SplitFileAndVerifyThenDelete(out string errorMessage, bool doNotDelete = false)
        {
            if (!SplitFile(out errorMessage))
            {
                return false;
            }

            if (!VerifyFileSplits(out errorMessage))
            {
                return false;
            }

            try
            {
                using var file = new StreamWriter(new FileStream(recombineBatFilePath, FileMode.Create));
                foreach (var entry in recombineBatFileContents)
                {
                    file.WriteLine(entry);
                }
            }
            catch (Exception ex)
            {
                errorMessage = $"Failed to create recombine batch file, {recombineBatFilePath}";
                OnErrorEvent(errorMessage, ex);
                return false;
            }

            if (!doNotDelete)
            {
                try
                {
                    inputFileInfo.Delete();
                }
                catch (Exception ex)
                {
                    errorMessage = $"Failed to delete input file, {inputFileInfo.FullName}";
                    OnErrorEvent(errorMessage, ex);
                    return false;
                }
            }

            return true;
        }

        /// <summary>
        /// Perform the file split
        /// </summary>
        /// <returns>True for success, false otherwise</returns>
        public bool SplitFile(out string errorMessage)
        {
            errorMessage = "";
            // Step 1: ensure all output directories exist
            foreach (var dir in outputDirectories)
            {
                try
                {
                    Directory.CreateDirectory(dir);
                }
                catch (Exception ex)
                {
                    errorMessage = $"Error creating folder {dir}: {ex.Message}";
                    OnErrorEvent($"Error creating folder {dir}", ex);
                    return false;
                }
            }

            OnProgressUpdate("Starting file split", 0);

            // NOTES on file copy: default .NET buffer size for CopyTo is 81920 (80KB) to stay off the large object heap
            // For this, we are going to use 1 MB, but with array reuse and care to not make calls that will create single-use 1MB byte arrays
            // We do not set/change the bufferSize used by the filestream for reading, since there are optimizations to not use it if reading more data than it can hold
            using var inputFile = new FileStream(inputFileInfo.FullName, FileMode.Open, FileAccess.Read, FileShare.Read);
            var bufferSize = 1024 * 1024;
            var buffer = new byte[bufferSize]; // 1 MB buffer
            var totalBytesCopied = 0L;
            for (var i = 0; i < outputPaths.Count; i++)
            {
                var outputPath = outputPaths[i];
                var endOfFile = false;
                try
                {
                    using var outputFile = new FileStream(outputPath, FileMode.Create, FileAccess.Write, FileShare.Read, 80 * 1024);
                    var totalBytes = 0L;

                    // Copy 1 MB of data, splitSizeMB times
                    for (var j = 0; j < splitSizeMB; j++)
                    {
                        var count = inputFile.Read(buffer, 0, bufferSize);
                        totalBytes += count;
                        totalBytesCopied += count;
                        if (count > 0)
                        {
                            outputFile.Write(buffer, 0, count);
                        }

                        if (count < bufferSize)
                        {
                            // Assume end of file
                            endOfFile = true;
                            break;
                        }
                    }

                    OnProgressUpdate($"File split {i + 1} of {outputPaths.Count} complete", (float)totalBytesCopied / inputFile.Length);

                    if (outputFile.Position < outputFile.Length)
                    {
                        // TODO: Handle this in a better way...
                        // Realistically we should only hit this if previous math is bad, or the file is increasing in size even as we are splitting it.
                        OnErrorEvent($"Split file {inputFileInfo.FullName} into {OutputSplits.Count} x {splitSizeGB} GB chunks, but did not reach the end of the file! {totalBytes} bytes split from {inputFile.Length} bytes in input file.");
                        return false;
                    }

                    OnStatusEvent($"Wrote file {outputPath}, {totalBytes} bytes ({totalBytes / 1024.0 / 1024 / 1024:F2} GB)");
                }
                catch (Exception ex)
                {
                    errorMessage = $"Error splitting file {inputFileInfo.FullName}, split # {i + 1}: {ex.Message}";
                    OnErrorEvent($"Error splitting file {inputFileInfo.FullName}, split # {i + 1}", ex);
                    return false;
                }

                if (endOfFile)
                {
                    break;
                }
            }

            OnProgressUpdate("File split complete", 100);

            return true;
        }

        /// <summary>
        /// Verify the file splits, checking the start and end, and a portion at the center
        /// </summary>
        /// <param name="errorMessage"></param>
        /// <returns></returns>
        public bool VerifyFileSplits(out string errorMessage)
        {
            errorMessage = "";
            // TODO: open the input file, and then each split file in order, and do a start-middle-end byte check for each split vs. the input file
            // Another, more time-consuming option would be to perform hash checks on both the main file and each individual file
            // Hash check options:
            // 1. Calculate multiple partial hashes on the input file according to the split file size to compare to each split file hash
            // 2. Pass each split file into a single hash call, and compare that to the hash of the complete input file

            using var sourceFile = new FileStream(inputFileInfo.FullName, FileMode.Open, FileAccess.Read, FileShare.Read);

            // Read data into 80KB arrays (avoid large object heap)
            const int readSize = 80 * 1024;
            const int halfSize = readSize / 2;
            var expectedSplitSizeBytes = splitSizeMB * 1024 * 1024;
            var sourceBuffer = new byte[readSize];
            var splitBuffer = new byte[readSize];
            var defaultOffsets = new long[] { 0, expectedSplitSizeBytes / 2 - halfSize, expectedSplitSizeBytes - readSize - 1 };
            var expectedSplits = sourceFile.Length / expectedSplitSizeBytes;
            var lastSplitExpectedSize = sourceFile.Length % expectedSplitSizeBytes;
            if (lastSplitExpectedSize > 0)
            {
                expectedSplits++;
            }

            if (outputPaths.Count != expectedSplits)
            {
                errorMessage = $"Split file count does not match, expected {expectedSplits} but was {outputPaths.Count}";
                OnErrorEvent(errorMessage);
                return false;
            }

            for (var i = 0; i < outputPaths.Count; i++)
            {
                var splitPath = outputPaths[i];
                using var splitFile = new FileStream(splitPath, FileMode.Open, FileAccess.Read, FileShare.Read);

                var sourceFileBaseOffset = i * expectedSplitSizeBytes;
                var lastSplit = i == outputPaths.Count - 1;

                long[] offsetsToCheck;
                if (splitFile.Length == expectedSplitSizeBytes)
                {
                    offsetsToCheck = defaultOffsets;
                }
                else if (splitFile.Length == lastSplitExpectedSize && lastSplit)
                {
                    offsetsToCheck = new long[] { 0, Math.Max(lastSplitExpectedSize / 2 - halfSize, 0), Math.Max(lastSplitExpectedSize - readSize - 1, 0) };
                }
                else
                {
                    errorMessage = $"Split file {i + 1} length differed from expected value, was {splitFile.Length} but expected {(lastSplit ? lastSplitExpectedSize : expectedSplitSizeBytes)}";
                    OnErrorEvent(errorMessage);
                    return false;
                }

                foreach (var offset in offsetsToCheck)
                {
                    sourceFile.Seek(sourceFileBaseOffset + offset, SeekOrigin.Begin);
                    splitFile.Seek(offset, SeekOrigin.Begin);

                    var sourceRead = sourceFile.Read(sourceBuffer, 0, readSize);
                    var splitRead = splitFile.Read(splitBuffer, 0, readSize);

                    if (sourceRead != splitRead)
                    {
                        errorMessage = $"Split file {i + 1}: {splitRead} bytes read != {sourceRead} bytes read from source file";
                        OnErrorEvent(errorMessage);
                        return false;
                    }

                    if (!sourceBuffer.AsSpan(0, sourceRead).SequenceEqual(splitBuffer.AsSpan(0, splitRead)))
                    {
                        errorMessage = $"Split file {i + 1}, 80 KB compare mismatch at offset {offset} vs. source file offset {sourceFileBaseOffset + offset}";
                        OnErrorEvent(errorMessage);
                        return false;
                    }
                }
            }

            return true;
        }

        private static string GetRelativePath(string fromPath, string toPath)
        {
#if NET21_OR_GREATER
            return Path.GetRelativePath(fromPath, toPath);
#else
            // Legacy method
            // FromPath must end in a trailing slash...
            fromPath = fromPath.TrimEnd('/', '\\') + "\\";
            var fromUri = new Uri(fromPath);
            var toUri = new Uri(toPath);

            // Convert to relative URI (uses '/' and replaces spaces with %20)
            var relativeUri = fromUri.MakeRelativeUri(toUri);
            // Unescape spaces and change from '/' to the system directory separator
            return Uri.UnescapeDataString(relativeUri.ToString()).Replace('/', Path.DirectorySeparatorChar);
#endif
        }
    }
}
