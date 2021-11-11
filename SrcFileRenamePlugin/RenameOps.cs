//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 11/17/2009
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using CaptureTaskManager;
using PRISM;

namespace SrcFileRenamePlugin
{
    /// <summary>
    /// Class for performing rename operations
    /// </summary>
    internal class RenameOps : EventNotifier
    {
        // ReSharper disable CommentTypo

        // Ignore Spelling: Bionet, Pwd, prepend, Username, Subfolder, secfso, fso, chromooff, flatline, LCmethod, plugsplit, slowsplit

        // ReSharper restore CommentTypo

        private string mMsg = string.Empty;
        private readonly bool mUseBioNet;
        private readonly string mUserName = string.Empty;
        private readonly string mPwd = string.Empty;
        private ShareConnector mShareConnector;
        private bool mConnected;

        private readonly DatasetFileSearchTool mDatasetFileSearchTool;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mgrParams">Parameters for manager operation</param>
        /// <param name="useBioNet">Flag to indicate if source instrument is on Bionet</param>
        public RenameOps(IMgrParams mgrParams, bool useBioNet)
        {
            // Setup for Bionet use, if applicable
            mUseBioNet = useBioNet;
            if (mUseBioNet)
            {
                mUserName = mgrParams.GetParam("BionetUser");
                mPwd = mgrParams.GetParam("BionetPwd");

                if (!mUserName.Contains(@"\"))
                {
                    // Prepend this computer's name to the username
                    mUserName = System.Net.Dns.GetHostName() + @"\" + mUserName;
                }
            }

            mDatasetFileSearchTool = new DatasetFileSearchTool(false);
            RegisterEvents(mDatasetFileSearchTool);
        }

        /// <summary>
        /// Perform a single rename operation
        /// </summary>
        /// <param name="taskParams">Enum indicating status of task</param>
        /// <param name="errorMessage">Output: error message</param>
        public EnumCloseOutType DoOperation(ITaskParams taskParams, out string errorMessage)
        {
            var datasetName = taskParams.GetParam("Dataset");

            // Example: \\exact04.bionet\
            var sourceVol = taskParams.GetParam("Source_Vol");

            // Example: ProteomicsData\
            var sourcePath = taskParams.GetParam("Source_Path");

            // Capture_Subdirectory is typically an empty string, but could be a partial path like: "CapDev" or "Smith\2014"
            var legacyCaptureSubfolder = taskParams.GetParam("Capture_Subfolder");
            var captureSubdirectory = taskParams.GetParam("Capture_Subdirectory", legacyCaptureSubfolder);

            var instrumentFileHash = taskParams.GetParam("Instrument_File_Hash");

            var pwd = CTMUtilities.DecodePassword(mPwd);

            OnDebugEvent("Started RenameOps.DoOperation()");

            // Set up paths

            mDatasetFileSearchTool.VerifyRelativeSourcePath(sourceVol, ref sourcePath, ref captureSubdirectory);

            // Determine if source dataset exists, and if it is a file or a directory
            var sourceDirectoryPath = Path.Combine(sourceVol, sourcePath);

            // Connect to Bionet if necessary
            if (mUseBioNet)
            {
                OnDebugEvent("Bionet connection required for " + sourceVol);

                mShareConnector = new ShareConnector(mUserName, pwd)
                {
                    Share = sourceDirectoryPath
                };

                if (mShareConnector.Connect())
                {
                    OnDebugEvent("Connected to Bionet");
                    mConnected = true;
                }
                else
                {
                    var msg = string.Format(
                        "Error {0} connecting to {1} as user {2} using 'secfso'",
                        mShareConnector.ErrorMessage, sourceDirectoryPath, mUserName);

                    if (mShareConnector.ErrorMessage == "1326")
                    {
                        OnErrorEvent(msg + "; you likely need to change the Capture_Method from secfso to fso");
                    }
                    else if (mShareConnector.ErrorMessage == "53")
                    {
                        OnErrorEvent(msg + "; the password may need to be reset");
                    }
                    else
                    {
                        OnErrorEvent(msg);
                    }

                    errorMessage = string.Format("Error connecting to {0} as user {1} using 'secfso'", sourceDirectoryPath, mUserName);

                    return EnumCloseOutType.CLOSEOUT_FAILED;
                }
            }
            else
            {
                OnDebugEvent("Bionet connection not required for " + sourceVol);
            }

            // If Source_Folder_Name is non-blank, use it. Otherwise use dataset name
            var sourceDirectoryName = taskParams.GetParam("Source_Folder_Name");

            if (string.IsNullOrWhiteSpace(sourceDirectoryName))
            {
                OnDebugEvent("Source_Folder_Name is empty; will use the dataset name: " + datasetName);
                sourceDirectoryName = datasetName;
            }
            else
            {
                OnDebugEvent("Source_Folder_Name: " + sourceDirectoryName);
            }

            // Now that we've had a chance to connect to the share, possibly append a subdirectory to the source path
            if (!string.IsNullOrWhiteSpace(captureSubdirectory))
            {
                OnDebugEvent("Capture_Subdirectory: " + captureSubdirectory);

                // However, if the subdirectory name matches the dataset name, this was probably an error on the operator's part
                // and we likely do not want to use the subdirectory name
                if (captureSubdirectory.EndsWith(Path.DirectorySeparatorChar + sourceDirectoryName, StringComparison.OrdinalIgnoreCase) ||
                    captureSubdirectory.Equals(sourceDirectoryName, StringComparison.OrdinalIgnoreCase))
                {
                    var candidateDirectoryPath = Path.Combine(sourceDirectoryPath, captureSubdirectory);

                    if (!Directory.Exists(candidateDirectoryPath))
                    {
                        // Leave sourceDirectoryPath unchanged
                        // Dataset Capture_Subdirectory ends with the dataset name. Gracefully ignoring because this appears to be a data entry error; directory not found:
                        OnWarningEvent("Dataset Capture_Subdirectory ends with the dataset name. Gracefully ignoring " +
                                       "because this appears to be a data entry error; directory not found: " + candidateDirectoryPath);
                    }
                    else
                    {
                        if (captureSubdirectory.Equals(sourceDirectoryName, StringComparison.OrdinalIgnoreCase))
                        {
                            OnWarningEvent("Dataset Capture_Subdirectory is the dataset name; leaving the capture path as {0} " +
                                           "so that the entire dataset directory will be copied", sourceDirectoryPath);
                        }
                        else
                        {
                            OnStatusEvent("Appending captureSubdirectory to sourceDirectoryPath, giving: " + candidateDirectoryPath);
                            sourceDirectoryPath = candidateDirectoryPath;
                        }
                    }
                }
                else
                {
                    sourceDirectoryPath = Path.Combine(sourceDirectoryPath, captureSubdirectory);
                }
            }

            OnDebugEvent("Source directory path: " + sourceDirectoryPath);
            var sourceDirectory = new DirectoryInfo(sourceDirectoryPath);
            int countRenamed;

            if (sourceDirectory.Exists)
            {
                OnDebugEvent("Find files in the source directory");

                countRenamed = FindFilesToRename(datasetName, sourceDirectory, instrumentFileHash, out errorMessage);
            }
            else
            {
                OnErrorEvent("Instrument directory not found for dataset " + datasetName + ": " + sourceDirectoryPath);

                errorMessage = "Remote directory not found: " + sourceDirectoryPath;
                return EnumCloseOutType.CLOSEOUT_FAILED;
            }

            // Close connection, if open
            if (mConnected)
            {
                DisconnectShare(ref mShareConnector);
            }

            if (countRenamed > 0)
            {
                return EnumCloseOutType.CLOSEOUT_SUCCESS;
            }

            if (string.IsNullOrEmpty(errorMessage))
            {
                errorMessage = "Data file and/or directory not found on the instrument; cannot rename";
            }

            OnErrorEvent("Dataset " + datasetName + ": " + errorMessage);

            return EnumCloseOutType.CLOSEOUT_FAILED;
        }

        /// <summary>
        /// Look for files to rename
        /// </summary>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="sourceDirectory">Directory to search</param>
        /// <param name="instrumentFileHash">SHA-1 hash of the primary instrument file (ignored for directories)</param>
        /// <param name="errorMessage">Output: error message</param>
        /// <returns>Number of renamed files</returns>
        private int FindFilesToRename(string datasetName, DirectoryInfo sourceDirectory, string instrumentFileHash, out string errorMessage)
        {
            errorMessage = string.Empty;

            // Construct a list of dataset names to check for
            // The first thing we check for is the official dataset name
            // We next check for various things that operators rename the datasets to
            var fileNamesToCheck = new List<string>
            {
                datasetName,
                datasetName + "-bad",
                datasetName + "_bad",
                datasetName + "-corrupt",
                datasetName + "_corrupt",
                datasetName + "-corrupted",
                datasetName + "_corrupted",
                // ReSharper disable StringLiteralTypo
                datasetName + "-chromooff",
                datasetName + "-flatline",
                datasetName + "-LCFroze",
                datasetName + "-mixer",
                datasetName + "-NoN2",
                datasetName + "-plugged",
                datasetName + "-pluggedSPE",
                datasetName + "-plunger",
                datasetName + "-pumpOFF",
                datasetName + "-slow",
                datasetName + "-wrongLCmethod",
                datasetName + "-air",
                datasetName + "-badQC",
                datasetName + "-corrupt",
                datasetName + "-corrupted",
                datasetName + "-plug",
                datasetName + "-plugsplit",
                datasetName + "-rotor",
                datasetName + "-slowsplit",
                // ReSharper restore StringLiteralTypo
                "x_" + datasetName,
                "x_" + datasetName + "-bad"
            };

            var loggedDatasetNotFound = false;
            var countRenamed = 0;

            foreach (var datasetNameBase in fileNamesToCheck)
            {
                if (string.IsNullOrEmpty(datasetNameBase))
                {
                    continue;
                }

                var alreadyRenamed = !datasetName.StartsWith("x_") && datasetNameBase.StartsWith("x_");

                // Get a list of files containing the dataset name
                var matchedFiles = GetMatchingFiles(sourceDirectory, datasetNameBase);

                // Get a list of directories containing the dataset name
                var matchedDirectories = GetMatchingDirectories(sourceDirectory, datasetNameBase);

                if (matchedFiles.Count + matchedDirectories.Count == 0)
                {
                    // No file or directory found
                    // Try looking for matching files with a space in the name
                    // If a match to a single file is found, and if the SHA-1 sum matches the value in DMS, rename it

                    // Uncomment to see every name checked
                    // OnDebugEvent(string.Format("Looking for {0} in {1}", datasetNameBase, sourceDirectory.FullName));

                    var matchedFiles2 = mDatasetFileSearchTool.FindDatasetFile(sourceDirectory.FullName, datasetNameBase);

                    if (matchedFiles2.FileCount == 1)
                    {
                        var candidateFile = matchedFiles2.FileList.First();
                        OnDebugEvent("Match found: " + candidateFile.FullName);

                        // Compute the SHA-1 hash
                        var sha1Hash = Pacifica.Core.Utilities.GenerateSha1Hash(candidateFile);

                        if (sha1Hash.Equals(instrumentFileHash))
                        {
                            OnStatusEvent("Hashes match for {0}: {1}", candidateFile.FullName, instrumentFileHash);
                            matchedFiles.Add(candidateFile);
                        }
                        else
                        {
                            OnWarningEvent("Hashes do not match for {0}: {1} on instrument vs. {2} on storage server",
                                candidateFile.FullName, sha1Hash, instrumentFileHash);
                        }
                    }
                    else
                    {
                        // Log a message for the first item checked in fileNamesToCheck
                        if (!loggedDatasetNotFound)
                        {
                            OnWarningEvent("Dataset {0}: data file and/or directory not found using {1}.*",
                                datasetNameBase, datasetNameBase);

                            loggedDatasetNotFound = true;
                        }

                        continue;
                    }
                }

                if (alreadyRenamed)
                {
                    OnStatusEvent("Skipping dataset {0} since data file and/or directory already renamed to {1}",
                        datasetNameBase, datasetNameBase);

                    countRenamed++;
                    break;
                }

                // Rename any files found
                foreach (var fileToRename in matchedFiles)
                {
                    if (RenameInstrumentFile(fileToRename, out errorMessage))
                    {
                        countRenamed++;
                        continue;
                    }

                    return 0;
                }

                // Rename any directories found
                foreach (var directoryToRename in matchedDirectories)
                {
                    if (RenameInstrumentDirectory(directoryToRename, out errorMessage))
                    {
                        countRenamed++;
                        continue;
                    }
                    return 0;
                }

                // Success; break out of the for loop
                break;
            }

            return countRenamed;
        }

        /// <summary>
        /// Prefixes specified file name with "x_"
        /// </summary>
        /// <param name="instrumentFile">File to be renamed</param>
        /// <param name="errorMessage">Output: error message</param>
        /// <returns>TRUE for success, FALSE for failure</returns>
        private bool RenameInstrumentFile(FileInfo instrumentFile, out string errorMessage)
        {
            // Rename dataset file on instrument
            var newPath = "??";
            errorMessage = string.Empty;

            try
            {
                if (!instrumentFile.Exists || instrumentFile.DirectoryName == null)
                {
                    return true;
                }

                newPath = Path.Combine(instrumentFile.DirectoryName, "x_" + instrumentFile.Name);
                var newInstrumentFile = new FileInfo(newPath);

                if (newInstrumentFile.Exists)
                {
                    mMsg = "Error renaming file " + instrumentFile.Name + "; new filename already exists: " + newInstrumentFile.FullName;
                    errorMessage = mMsg;
                    OnErrorEvent(mMsg);
                    return false;
                }

                instrumentFile.MoveTo(newInstrumentFile.FullName);
                mMsg = "Renamed file to " + instrumentFile.FullName;
                OnStatusEvent(mMsg);
                return true;
            }
            catch (Exception ex)
            {
                mMsg = "Error renaming file " + instrumentFile.FullName + " to " + newPath;
                OnErrorEvent(mMsg, ex);

                if (ex.Message.Contains("Access to the path") && ex.Message.Contains("is denied"))
                {
                    errorMessage = "Error renaming file: access is denied";
                }
                else
                {
                    errorMessage = "Error renaming file: " + ex.GetType();
                }

                return false;
            }
        }

        /// <summary>
        /// Prefixes specified directory name with "x_"
        /// </summary>
        /// <param name="instrumentDirectory">Directory to be renamed</param>
        /// <param name="errorMessage">Output: error message</param>
        /// <returns>TRUE for success, FALSE for failure</returns>
        private bool RenameInstrumentDirectory(DirectoryInfo instrumentDirectory, out string errorMessage)
        {
            // Rename dataset directory on instrument
            var newPath = "??";
            errorMessage = string.Empty;

            try
            {
                if (!instrumentDirectory.Exists || instrumentDirectory.Parent == null)
                {
                    return true;
                }

                newPath = Path.Combine(instrumentDirectory.Parent.FullName, "x_" + instrumentDirectory.Name);
                var newInstrumentDirectory = new DirectoryInfo(newPath);

                if (newInstrumentDirectory.Exists)
                {
                    mMsg = "Error renaming directory " + newInstrumentDirectory.Name + "; new directory already exists: " + newInstrumentDirectory.FullName;
                    errorMessage = mMsg;
                    OnErrorEvent(mMsg);
                    return false;
                }

                instrumentDirectory.MoveTo(newInstrumentDirectory.FullName);
                mMsg = "Renamed directory to " + instrumentDirectory.FullName;
                OnStatusEvent(mMsg);
                return true;
            }
            catch (Exception ex)
            {
                mMsg = "Error renaming directory " + instrumentDirectory.FullName + " to " + newPath;
                OnErrorEvent(mMsg, ex);

                if (ex.Message.Contains("Access to the path") && ex.Message.Contains("is denied"))
                {
                    errorMessage = "Error renaming directory: access is denied";
                }
                else
                {
                    errorMessage = "Error renaming directory: " + ex.GetType();
                }

                return false;
            }
        }

        /// <summary>
        /// Disconnects a Bionet shared drive
        /// </summary>
        /// <param name="myConn">Connection object for shared drive</param>
        private void DisconnectShare(ref ShareConnector myConn)
        {
            // Disconnects a shared drive
            myConn.Disconnect();
            mMsg = "Bionet disconnected";
            OnDebugEvent(mMsg);
            mConnected = false;
        }

        /// <summary>
        /// Gets a list of files containing the dataset name
        /// </summary>
        /// <param name="sourceDirectory">Directory to search</param>
        /// <param name="datasetName">Dataset name to match</param>
        /// <returns>Array of file paths</returns>
        private List<FileInfo> GetMatchingFiles(DirectoryInfo sourceDirectory, string datasetName)
        {
            return sourceDirectory.GetFiles(datasetName + ".*").ToList();
        }

        /// <summary>
        /// Gets a list of directories containing the dataset name
        /// </summary>
        /// <param name="sourceDirectory">Directory to search</param>
        /// <param name="datasetName">Dataset name to match</param>
        /// <returns>Array of folder paths</returns>
        private List<DirectoryInfo> GetMatchingDirectories(DirectoryInfo sourceDirectory, string datasetName)
        {
            return sourceDirectory.GetDirectories(datasetName + ".*").ToList();
        }
    }
}
