using System;
using System.IO;
using DatasetArchivePlugin;
using NUnit.Framework;

namespace CaptureTaskManagerTests
{
    public class FileSplitTests
    {
        [Test]
        [TestCase(@"\\proto-9\12T_FTICR_P_Imaging\2026_4\20260930_Shabtai_RhizoMAP_AAO_T3\20260930_Shabtai_RhizoMAP_AAO_T3.d")]
        public void CheckSplitData(string inputDataset)
        {
            if (!Directory.Exists(inputDataset))
            {
                return;
            }

            var serFile = new FileInfo(Path.Combine(inputDataset, "ser"));

            if (!serFile.Exists)
            {
                return;
            }

            var datasetDir = new DirectoryInfo(inputDataset);
            var datasetBasePath = datasetDir.Parent;
            var dataset = datasetDir.Name;

            var outBasePath = Path.Combine(datasetBasePath.FullName, Path.GetFileNameWithoutExtension(inputDataset) + "_ser_files_part");

            //var s = new LargeFileSplitter(serFile, outBasePath, 3, 50, "ser_part-");
            var s = new LargeFileSplitter(serFile, outBasePath, 3, 50);

            foreach (var p in s.OutputSplits)
            {
                Console.WriteLine(p);
            }

            Console.WriteLine();

            foreach (var p in s.OutputDirectories)
            {
                Console.WriteLine(p);
            }

            Console.WriteLine();

            foreach (var p in s.RecombineBatFileContents)
            {
                Console.WriteLine(p);
            }
        }

        [Test]
        public void TestSplitData()
        {
            var inputDataset = @"D:\Including_FC2_causes_PreProcessor_to_Crash\MPX_31V_r002.d"; // Local file on Bryson's computer
            var inputDatasetInfo = new DirectoryInfo(inputDataset);
            var fileToSplit = @"AcqData\MSProfile.bin";
            var fileToSplitInfo = new FileInfo(Path.Combine(inputDataset, fileToSplit));
            var outBasePath = Path.Combine(inputDatasetInfo.Parent.FullName, Path.GetFileNameWithoutExtension(inputDataset) + "_MSProfile_bin_files_part");

            var s = new LargeFileSplitter(fileToSplitInfo, outBasePath, 3, 1);

            foreach (var p in s.OutputSplits)
            {
                Console.WriteLine(p);
            }

            Console.WriteLine();

            foreach (var p in s.OutputDirectories)
            {
                Console.WriteLine(p);
            }

            Console.WriteLine();

            foreach (var p in s.RecombineBatFileContents)
            {
                Console.WriteLine(p);
            }

            var success = s.SplitFileAndVerifyThenDelete(out var errorMessage);
            if (!success)
            {
                Console.WriteLine($"Split failed, error message: {errorMessage}");
            }
            else
            {
                Console.WriteLine("File split succeeded!");
            }
        }
    }
}
