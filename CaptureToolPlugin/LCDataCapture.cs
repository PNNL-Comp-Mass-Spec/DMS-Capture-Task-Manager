using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using CaptureTaskManager;

namespace CaptureToolPlugin
{
    internal class LCDataCapture : LoggerBase
    {
        // Ignore Spelling: lcMethod, na

        private readonly IMgrParams mMgrParams;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mgrParams">Parameters for manager operation</param>
        public LCDataCapture(IMgrParams mgrParams)
        {
            mMgrParams = mgrParams;
        }

        /// <summary>
        /// Looks for the LCMethod file for this dataset
        /// Copies this file to the dataset directory
        /// </summary>
        /// <remarks>Returns true if the .lcmethod file is not found</remarks>
        /// <param name="datasetName"></param>
        /// <param name="datasetDirectoryPath"></param>
        /// <returns>True if file found and copied; false if an error</returns>
        public bool CaptureLCMethodFile(string datasetName, string datasetDirectoryPath)
        {
            const string DEFAULT_METHOD_FOLDER_BASE_PATH = @"\\proto-5\BionetXfer\Run_Complete_Trigger\MethodFiles";

            var success = true;
            var methodDirectoryBasePath = string.Empty;

            // Look for an LCMethod file associated with this raw spectra file
            // Note that this file is often created 45 minutes to 60 minutes after the run completes
            // and thus when capturing a dataset with an auto-created trigger file, we most likely will not find the .lcmethod file

            // The file will either be located in a directory with the dataset name, or will be in a subdirectory based on the year and quarter that the data was acquired

            try
            {
                methodDirectoryBasePath = mMgrParams.GetParam("LCMethodFilesDir", DEFAULT_METHOD_FOLDER_BASE_PATH);

                if (string.IsNullOrEmpty(methodDirectoryBasePath) ||
                    string.Equals(methodDirectoryBasePath, "na", StringComparison.OrdinalIgnoreCase))
                {
                    // LCMethodFilesDir is not defined; exit the function
                    return true;
                }

                var sourceDirectory = new DirectoryInfo(methodDirectoryBasePath);

                if (!sourceDirectory.Exists)
                {
                    LogWarning("LCMethods directory not found: " + methodDirectoryBasePath, true);

                    // Return true despite not having found the directory since this is not a fatal error for capture
                    return true;
                }

                // Construct a list of directories to search
                var directoryNamesToSearch = new List<string>
                {
                    datasetName
                };

                var year = DateTime.Now.Year;
                var quarter = GetQuarter(DateTime.Now);

                while (year >= 2011)
                {
                    directoryNamesToSearch.Add(year + "_" + quarter);

                    if (quarter > 1)
                    {
                        --quarter;
                    }
                    else
                    {
                        quarter = 4;
                        --year;
                    }

                    if (year == 2011 && quarter == 2)
                    {
                        break;
                    }
                }

                // This RegEx is used to match files with names like:
                // Cheetah_01.04.2012_08.46.17_Dataset_P28_D01_2629_192_3Jan12_Cheetah_11-09-32.lcmethod
                var methodFileMatcher = new Regex(@".+\d+\.\d+\.\d+_\d+\.\d+\.\d+_.+\.lcmethod", RegexOptions.IgnoreCase);
                var methodFiles = new List<FileInfo>();

                // Define the file match spec
                var lcMethodSearchSpec = "*_" + datasetName + ".lcmethod";

                for (var iteration = 1; iteration <= 2; iteration++)
                {
                    foreach (var directoryName in directoryNamesToSearch)
                    {
                        var sourceSubdirectory = new DirectoryInfo(Path.Combine(sourceDirectory.FullName, directoryName));

                        if (sourceSubdirectory.Exists)
                        {
                            // Look for files that match lcMethodSearchSpec
                            // There might be multiple files if the dataset was analyzed more than once
                            foreach (var methodFile in sourceSubdirectory.GetFiles(lcMethodSearchSpec))
                            {
                                if (iteration == 1)
                                {
                                    // First iteration
                                    // Check each file against the RegEx
                                    if (methodFileMatcher.IsMatch(methodFile.Name))
                                    {
                                        // Match found
                                        methodFiles.Add(methodFile);
                                    }
                                }
                                else
                                {
                                    // Second iteration; accept any match
                                    methodFiles.Add(methodFile);
                                }
                            }
                        }

                        if (methodFiles.Count > 0)
                        {
                            break;
                        }
                    }
                }

                if (methodFiles.Count == 0)
                {
                    // LCMethod file not found; exit function
                    return true;
                }

                // LCMethod file found
                // Copy to the dataset directory

                foreach (var methodFile in methodFiles)
                {
                    try
                    {
                        var targetFilePath = Path.Combine(datasetDirectoryPath, methodFile.Name);
                        methodFile.CopyTo(targetFilePath, true);
                    }
                    catch (Exception ex)
                    {
                        LogWarning("Exception copying LCMethod file " + methodFile.FullName + ": " + ex.Message);
                    }
                }

                // If the file was found in a dataset directory, rename the source directory to start with x_
                var firstFileDirectory = methodFiles[0].Directory;

                if (firstFileDirectory != null && string.Equals(firstFileDirectory.Name, datasetName, StringComparison.OrdinalIgnoreCase))
                {
                    try
                    {
                        var renamedSourceDirectory = Path.Combine(methodDirectoryBasePath, "x_" + datasetName);

                        if (Directory.Exists(renamedSourceDirectory))
                        {
                            // x_ directory already exists; move the files
                            foreach (var methodFile in methodFiles)
                            {
                                var targetFilePath = Path.Combine(renamedSourceDirectory, methodFile.Name);

                                methodFile.CopyTo(targetFilePath, true);
                                methodFile.Delete();
                            }
                            sourceDirectory.Delete(false);
                        }
                        else
                        {
                            // Rename the directory
                            sourceDirectory.MoveTo(renamedSourceDirectory);
                        }
                    }
                    catch (Exception ex)
                    {
                        // Exception renaming the directory; log this as a warning
                        LogWarning("Exception renaming source LCMethods directory for " + datasetName + ": " + ex.Message);
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Exception copying LCMethod file for " + datasetName, ex);
                success = false;
            }

            if (string.IsNullOrWhiteSpace(methodDirectoryBasePath))
            {
                return success;
            }

            var currentTime = DateTime.Now;

            if (currentTime.Hour is 18 or 19 || System.Net.Dns.GetHostName().StartsWith("WE43320", StringComparison.OrdinalIgnoreCase))
            {
                // Time is between 6 pm and 7:59 pm
                // Check for directories at METHOD_FOLDER_BASE_PATH that start with x_ and have .lcmethod files that are all at least 14 days old
                // These directories are safe to delete
                DeleteOldLCMethodDirectories(methodDirectoryBasePath);
            }

            return success;
        }

        /// <summary>
        /// Look for LCMethod directories that start with x_ and have .lcmethod files that are more than 2 weeks old
        /// Matching directories are deleted
        /// Note that in February 2012 we plan to switch to saving .lcmethod files in Year_Quarter directories (e.g. 2012_1 or 2012_2)
        /// and thus we won't need to call this function in the future
        /// </summary>
        /// <param name="lcMethodsDirectoryPath"></param>
        private void DeleteOldLCMethodDirectories(string lcMethodsDirectoryPath)
        {
            try
            {
                var lcMethodsDirectory = new DirectoryInfo(lcMethodsDirectoryPath);

                if (!lcMethodsDirectory.Exists)
                {
                    return;
                }

                foreach (var subdirectory in lcMethodsDirectory.GetDirectories("x_*"))
                {
                    var safeToDelete = true;

                    // Make sure all of the files in the directory are at least 14 days old
                    foreach (var fileOrDirectory in subdirectory.GetFileSystemInfos())
                    {
                        if (DateTime.UtcNow.Subtract(fileOrDirectory.LastWriteTimeUtc).TotalDays <= 14)
                        {
                            // File was modified within the last 2 weeks; do not delete this directory
                            safeToDelete = false;
                            break;
                        }
                    }

                    if (!safeToDelete)
                    {
                        continue;
                    }

                    try
                    {
                        subdirectory.Delete(true);

                        LogMessage("Deleted old LCMethods directory: " + subdirectory.FullName);
                    }
                    catch (Exception ex)
                    {
                        LogError("Exception deleting old LCMethods directory", ex);
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Exception looking for old LC Method directories", true);
                LogError("Stack trace", ex);
            }
        }

        /// <summary>
        /// Return the current quarter for a given date (based on the month)
        /// </summary>
        /// <param name="date"></param>
        /// <returns>Quarter of the year</returns>
        private int GetQuarter(DateTime date)
        {
            switch (date.Month)
            {
                case 1:
                case 2:
                case 3:
                    return 1;
                case 4:
                case 5:
                case 6:
                    return 2;
                case 7:
                case 8:
                case 9:
                    return 3;
                default:
                    return 4;
            }
        }
    }
}
