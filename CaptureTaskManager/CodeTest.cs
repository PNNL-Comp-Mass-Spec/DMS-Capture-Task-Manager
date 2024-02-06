 using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;

namespace CaptureTaskManager
{
    internal class CodeTest
    {
        /// <summary>
        /// Test connecting to a computer on bionet
        /// </summary>
        // ReSharper disable once UnusedMember.Global
        public void TestConnection()
        {
            // Ignore Spelling: bionet, ftms, lcmsoperator

            Console.WriteLine("Code test mode");

            var credentials = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            var sharePath = @"\\15T_FTICR.bionet\ProteomicsData";

            // Make sure sharePath does not end in a backslash
            if (sharePath.EndsWith(@"\"))
            {
                sharePath = sharePath.Substring(0, sharePath.Length - 1);
            }

            credentials.Add("ftms", "PasswordHere");
            credentials.Add("lcmsoperator", "PasswordHere");

            try
            {
                foreach (var item in credentials)
                {
                    var accessCredentials = new System.Net.NetworkCredential(item.Key, item.Value, string.Empty);

                    Console.WriteLine("Credentials created for " + item.Key);

                    var bionetConnection = new NetworkConnection(sharePath, accessCredentials);

                    Console.WriteLine("Connected to share");

                    var shareDirectory = new DirectoryInfo(sharePath);

                    Console.WriteLine("Instantiated DirectoryInfo object: " + shareDirectory.FullName);

                    var iteration = 0;

                    foreach (var remoteFile in shareDirectory.GetFiles())
                    {
                        Console.WriteLine("File: " + remoteFile.Name + " size " + remoteFile.Length + " bytes");
                        ++iteration;

                        if (iteration > 20)
                        {
                            break;
                        }
                    }

                    Console.WriteLine("Files Done");

                    iteration = 0;

                    foreach (var subdirectory in shareDirectory.GetDirectories())
                    {
                        Console.WriteLine("Folder: " + subdirectory.Name + " modified " +
                                          subdirectory.LastWriteTime.ToString(CultureInfo.InvariantCulture));
                        ++iteration;

                        if (iteration > 20)
                        {
                            break;
                        }
                    }

                    Console.WriteLine("Folders Done");

                    // Disconnect network connection
                    bionetConnection.Dispose();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception: " + ex.Message);
            }
        }

        /// <summary>
        /// Test using VerifyRelativeSourcePath to update path variables
        /// </summary>
        public void TestFixSourcePath()
        {
            var searchTool = new DatasetFileSearchTool(true);

            const string sourceVol = @"\\lumos01.bionet\";
            var sourcePath = @"ProteomicsData\";
            var captureSubdirectory = @"..\ProteomicsData2";

            var originalPath = Path.Combine(sourceVol, sourcePath, captureSubdirectory);

            var pathVariablesUpdated = searchTool.VerifyRelativeSourcePath(sourceVol, ref sourcePath, ref captureSubdirectory);

            var updatedPath = Path.Combine(sourceVol, sourcePath, captureSubdirectory);

            Console.WriteLine();

            if (pathVariablesUpdated)
            {
                Console.WriteLine("Path updated");
            }
            else
            {
                Console.WriteLine("Path not updated");
            }

            Console.WriteLine();
            Console.WriteLine("Old: " + originalPath);
            Console.WriteLine("New: " + updatedPath);

            Console.WriteLine();
        }
    }
}