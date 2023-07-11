using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using CaptureTaskManager;
using PRISM;

namespace CaptureToolPlugin.DataCapture
{
    /// <summary>
    /// Class for capturing directory-based dataset "files"
    /// </summary>
    internal class CaptureDirectoryBase : CaptureBase
    {
        // Ignore Spelling: bionet, Bruker, mcf, ser

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="data">Initialization data object</param>
        public CaptureDirectoryBase(CaptureInitData data) : base(data)
        { }

        /// <summary>
        /// Look for files in the dataset directory with spaces in the name
        /// If the filename otherwise matches the dataset, rename it
        /// </summary>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="datasetDirectory">Dataset directory to search</param>
        private void AutoFixFilesWithInvalidChars(string datasetName, DirectoryInfo datasetDirectory)
        {
            var candidateFiles = new List<FileSystemInfo>();

            // Find items matching "* *" and "*%*" and "*.*"
            foreach (var item in mDatasetFileSearchTool.FilenameAutoFixes)
            {
                if (item.Key == '.')
                {
                    foreach (var candidateFile in datasetDirectory.GetFileSystemInfos("*.*", SearchOption.AllDirectories))
                    {
                        if (Path.GetFileNameWithoutExtension(candidateFile.Name).IndexOf('.') >= 0)
                        {
                            candidateFiles.Add(candidateFile);
                        }
                    }
                }
                else
                {
                    candidateFiles.AddRange(datasetDirectory.GetFileSystemInfos("*" + item.Key + "*", SearchOption.AllDirectories));
                }
            }

            var processedFiles = new SortedSet<string>();

            foreach (var datasetFileOrDirectory in candidateFiles)
            {
                if (processedFiles.Contains(datasetFileOrDirectory.FullName))
                {
                    continue;
                }

                processedFiles.Add(datasetFileOrDirectory.FullName);

                var updatedName = mDatasetFileSearchTool.AutoFixFilename(datasetName, datasetFileOrDirectory.Name);

                if (string.Equals(datasetFileOrDirectory.Name, updatedName, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                LogMessage("Renaming '" + datasetFileOrDirectory.Name + "' to '" + updatedName + "' to remove invalid characters");

                var sourceFilePath = datasetFileOrDirectory.FullName;
                string targetFilePath;

                // ReSharper disable MergeIntoPattern
                if (datasetFileOrDirectory is FileInfo datasetFile && datasetFile.Directory != null)
                {
                    targetFilePath = Path.Combine(datasetFile.Directory.FullName, updatedName);
                }
                else if (datasetFileOrDirectory is DirectoryInfo datasetSubdirectory && datasetSubdirectory.Parent != null)
                {
                    targetFilePath = Path.Combine(datasetSubdirectory.Parent.FullName, updatedName);
                }
                else
                {
                    // Fail safe code; this shouldn't typically be reached
                    LogWarning("Unable to determine the parent directory of {0} in AutoFixFilesWithInvalidChars",
                        datasetFileOrDirectory.FullName);

                    targetFilePath = Path.Combine(datasetDirectory.FullName, updatedName);
                }
                // ReSharper restore MergeIntoPattern

                try
                {
                    if (mTraceMode)
                    {
                        ToolRunnerBase.ShowTraceMessage("Moving {0} to {1}", sourceFilePath, targetFilePath);
                    }

                    File.Move(sourceFilePath, targetFilePath);
                }
                catch (Exception ex)
                {
                    LogError("Error renaming file", ex);
                    LogMessage("Source: {0}; Target:{1}", sourceFilePath, targetFilePath);
                }
            }
        }

        /// <summary>
        /// Capture a dataset directory that has an extension like .D or .Raw
        /// </summary>
        /// <param name="msg">Output: error message</param>
        /// <param name="returnData">Input/output: Return data</param>
        /// <param name="datasetInfo">Dataset info</param>
        /// <param name="sourceDirectoryPath">Source directory (on instrument); datasetInfo.FileOrDirectoryName will be appended to this</param>
        /// <param name="datasetDirectoryPath">Destination directory (on storage server); datasetInfo.FileOrDirectoryName will be appended to this</param>
        /// <param name="copyWithResume">True if using copy with resume</param>
        /// <param name="instrumentClass">Instrument class</param>
        /// <param name="instrumentName">Instrument name</param>
        /// <param name="taskParams">Task parameters</param>
        public void CaptureDirectoryExt(
            out string msg,
            ToolReturnData returnData,
            DatasetInfo datasetInfo,
            string sourceDirectoryPath,
            string datasetDirectoryPath,
            bool copyWithResume,
            InstrumentClassInfo.InstrumentClass instrumentClass,
            string instrumentName,
            ITaskParams taskParams
        )
        {
            SortedSet<string> filesToSkip = null;

            bool success;

            var sourceDirectory = new DirectoryInfo(Path.Combine(sourceDirectoryPath, datasetInfo.FileOrDirectoryName));
            var targetDirectory = new DirectoryInfo(Path.Combine(datasetDirectoryPath, datasetInfo.FileOrDirectoryName));

            // Look for a zero-byte .UIMF file or a .UIMF journal file
            // Abort the capture if either is present
            if (IsIncompleteUimfFound(sourceDirectory.FullName, out msg, returnData))
            {
                return;
            }

            if (instrumentClass == InstrumentClassInfo.InstrumentClass.Agilent_Ion_Trap)
            {
                // Confirm that a DATA.MS file exists
                if (IsIncompleteAgilentIonTrap(taskParams, sourceDirectory.FullName, out msg, returnData))
                {
                    return;
                }
            }

            var brukerDotDDirectory = false;

            if (datasetInfo.FileOrDirectoryName.EndsWith(".d", StringComparison.OrdinalIgnoreCase))
            {
                // Bruker .D directory (common for the 12T and 15T)
                // Look for journal files, which we can never copy because they are always locked

                brukerDotDDirectory = true;

                var searchSpecList = new Dictionary<string, string>()
                {
                    {"*.mcf_idx-journal", "journal file"}
                };

                if (string.Equals(instrumentName, "12T_FTICR_B", StringComparison.OrdinalIgnoreCase))
                {
                    // Add various mcf and mcf_idx files
                    // Specifically list those that have _1 or _2 etc. because we _do_ want to copy Storage.mcf_idx files
                    searchSpecList.Add("*_1.mcf", "mcf files");
                    searchSpecList.Add("*_2.mcf", "mcf files");
                    searchSpecList.Add("*_3.mcf", "mcf files");
                    searchSpecList.Add("*_4.mcf", "mcf files");
                    searchSpecList.Add("*_1.mcf_idx", "mcf_idx files");
                    searchSpecList.Add("*_2.mcf_idx", "mcf_idx files");
                    searchSpecList.Add("*_3.mcf_idx", "mcf_idx files");
                    searchSpecList.Add("*_4.mcf_idx", "mcf_idx files");
                    searchSpecList.Add("LockInfo", "lock files");
                    searchSpecList.Add("SyncHelper", "sync helper");
                    searchSpecList.Add("ProjectCreationHelper", "project creation helper");
                }

                success = FindFilesToSkip(sourceDirectory, datasetInfo, searchSpecList, returnData, out filesToSkip);
                if (!success)
                {
                    msg = "Error looking for journal files to skip";
                    returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    // Note: error has already been logged and mShareConnection.DisconnectShareIfRequired() has already been called
                    return;
                }
            }

            returnData.CloseoutMsg = string.Empty;

            if (!VerifyConstantDirectorySize(sourceDirectory, returnData))
            {
                msg = "Dataset '" + datasetInfo.DatasetName + "' not ready";
                LogWarning(msg);
                mShareConnection.DisconnectShareIfRequired();

                if (string.IsNullOrWhiteSpace(returnData.CloseoutMsg))
                {
                    returnData.CloseoutMsg = "directory size changed";
                }

                if (returnData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS)
                {
                    returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_NOT_READY;
                }

                return;
            }

            // Make a dataset directory
            try
            {
                MakeDirectoryIfMissing(datasetDirectoryPath);
            }
            catch (Exception ex)
            {
                returnData.CloseoutMsg = "Exception creating dataset directory";
                msg = returnData.CloseoutMsg + " at " + datasetDirectoryPath;
                LogError(msg, true);
                LogError("Stack trace", ex);

                mShareConnection.DisconnectShareIfRequired();

                mToolState.HandleCopyException(returnData, ex);
                return;
            }

            // Copy the source directory to the dataset directory
            try
            {
                DirectoryInfo sourceDirectoryToUse;
                string extraDirectoryToCreate;

                // Check for a subdirectory below the source directory with the same extension as the source directory
                // For example, \\Server.bionet\ProteomicsData\Dataset2_20Aug18.d\Dataset2_20Aug18.d
                // or           \\Server.bionet\ProteomicsData\SDI_42___l_a_MRM_CE10_5a.d\SDI_42  _l_a_MRM_CE10_5a.d

                var matchSpec = "*" + sourceDirectory.Extension;
                if (mTraceMode)
                {
                    ToolRunnerBase.ShowTraceMessage("Looking for directories matching {0} at {1}",
                        matchSpec, sourceDirectory.FullName);
                }

                var subdirectories = sourceDirectory.GetDirectories(matchSpec);
                if (subdirectories.Length > 1)
                {
                    LogWarning("Source directory has multiple subdirectories with extension {0}; see {1}",
                        sourceDirectory.Extension, sourceDirectory.FullName);

                    sourceDirectoryToUse = sourceDirectory;
                    extraDirectoryToCreate = string.Empty;
                }
                else if (subdirectories.Length == 1)
                {
                    // If the letters and numbers (but not symbols) in the subdirectory name match the letters and numbers
                    // in the source directory name to a tolerance of 0.75, silently use the subdirectory as the source

                    // Otherwise, use the subdirectory, but log a warning and create an empty directory on the storage server
                    // with the same name as the subdirectory

                    sourceDirectoryToUse = subdirectories.First();

                    var similarityScore = PRISM.DataUtils.StringSimilarityTool.CompareStrings(sourceDirectory.Name, sourceDirectoryToUse.Name);

                    const float SIMILARITY_SCORE_THRESHOLD = 0.75f;

                    if (similarityScore >= SIMILARITY_SCORE_THRESHOLD)
                    {
                        var logMessage = string.Format("Copying files from {0} instead of the parent directory; name similarity score: {1:F2}",
                                                       sourceDirectoryToUse.FullName, similarityScore);
                        if (mTraceMode)
                        {
                            ToolRunnerBase.ShowTraceMessage(logMessage);
                        }

                        LogDebug(logMessage);

                        extraDirectoryToCreate = string.Empty;
                    }
                    else
                    {
                        LogWarning("Copying files from {0} instead of the parent directory; name similarity score: {1:F2}. " +
                                   "Will create an empty directory named {2} on the storage server since the similarity score is less than {3}",
                            sourceDirectoryToUse.FullName, similarityScore,
                            sourceDirectoryToUse.Name, SIMILARITY_SCORE_THRESHOLD);

                        extraDirectoryToCreate = sourceDirectoryToUse.Name;
                    }
                }
                else
                {
                    sourceDirectoryToUse = sourceDirectory;
                    extraDirectoryToCreate = string.Empty;
                }

                if (mTraceMode)
                {
                    Console.WriteLine();
                    ToolRunnerBase.ShowTraceMessage("Copying from\n{0} to\n{1}", sourceDirectoryToUse.FullName, targetDirectory.FullName);

                    const int waitTimeSeconds = 5;
                    Console.WriteLine();
                    ConsoleMsgUtils.ShowDebug("Pausing for {0} seconds since TraceMode is enabled; review the directory paths", waitTimeSeconds);

                    var waitTimeEnd = DateTime.UtcNow.AddSeconds(waitTimeSeconds);

                    while (waitTimeEnd > DateTime.UtcNow)
                    {
                        AppUtils.SleepMilliseconds(1000);
                        Console.Write(".");
                    }
                }

                // Copy the dataset directory
                // Resume copying files that are already present in the target

                if (copyWithResume)
                {
                    const bool recurse = true;
                    success = CopyDirectoryWithResume(sourceDirectoryToUse.FullName, targetDirectory.FullName, recurse, returnData, filesToSkip);
                }
                else
                {
                    if (filesToSkip == null)
                    {
                        mFileTools.CopyDirectory(sourceDirectoryToUse.FullName, targetDirectory.FullName);
                    }
                    else
                    {
                        mFileTools.CopyDirectory(sourceDirectoryToUse.FullName, targetDirectory.FullName, filesToSkip.ToList());
                    }

                    success = true;
                }

                if (success)
                {
                    msg = "Copied directory " + sourceDirectoryToUse.FullName + " to " + targetDirectory.FullName + mShareConnection.GetConnectionDescription();
                    LogMessage(msg);

                    AutoFixFilesWithInvalidChars(datasetInfo.DatasetName, targetDirectory);

                    // Make sure the target directory does not have the System attribute set
                    // Agilent instruments enable the System attribute for .D directories, and this makes it harder to manage things on the storage server
                    if ((targetDirectory.Attributes & FileAttributes.System) == FileAttributes.System)
                    {
                        LogDebug("Removing the system flag from " + targetDirectory.FullName);
                        targetDirectory.Attributes &= ~FileAttributes.System;
                    }

                    if (!string.IsNullOrEmpty(extraDirectoryToCreate))
                    {
                        var extraDirectory = new DirectoryInfo(Path.Combine(targetDirectory.FullName, extraDirectoryToCreate));

                        if (mTraceMode)
                        {
                            ToolRunnerBase.ShowTraceMessage("Creating empty directory at " + extraDirectory.FullName);
                        }

                        if (!extraDirectory.Exists)
                        {
                            extraDirectory.Create();
                        }
                    }
                }
                else
                {
                    msg = "Unknown error copying the dataset directory";
                }
            }
            catch (Exception ex)
            {
                msg = "Copy exception for dataset " + datasetInfo.DatasetName + mShareConnection.GetConnectionDescription();
                LogError(msg, ex);

                mShareConnection.DisconnectShareIfRequired();

                mToolState.HandleCopyException(returnData, ex);
                return;
            }

            mShareConnection.DisconnectShareIfRequired();

            if (success)
            {
                if (brukerDotDDirectory)
                {
                    // Look for and delete certain zero-byte files
                    DeleteZeroByteBrukerFiles(targetDirectory);
                }
            }

            if (success)
            {
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
            }
            else
            {
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Look for an incomplete Agilent Ion Trap .D directory
        /// </summary>
        /// <param name="taskParams"></param>
        /// <param name="directoryPath"></param>
        /// <param name="msg"></param>
        /// <param name="returnData"></param>
        /// <returns>True if incomplete</returns>
        private bool IsIncompleteAgilentIonTrap(
            ITaskParams taskParams,
            string directoryPath,
            out string msg,
            ToolReturnData returnData)
        {
            msg = string.Empty;

            try
            {
                var sourceDirectory = new DirectoryInfo(directoryPath);

                var dataMSFile = sourceDirectory.GetFiles("DATA.MS");
                string sourceDirectoryErrorMessage = null;

                if (dataMSFile.Length == 0)
                {
                    sourceDirectoryErrorMessage = "DATA.MS file not found; incomplete dataset";
                }
                else
                {
                    if (dataMSFile[0].Length == 0)
                    {
                        sourceDirectoryErrorMessage = "Source directory has a zero-byte DATA.MS file";
                    }
                }

                var allowIncompleteDataset = taskParams.GetParam("AllowIncompleteDataset", false);

                if (!string.IsNullOrEmpty(sourceDirectoryErrorMessage))
                {
                    msg = returnData.CloseoutMsg + " at " + directoryPath;

                    if (allowIncompleteDataset)
                    {
                        returnData.EvalMsg = sourceDirectoryErrorMessage;
                        LogWarning(msg);

                        return false;
                    }

                    var job = taskParams.GetParam("Job", 0);

                    var jobParameterHint = string.Format("Exec add_update_task_parameter {0}, 'JobParameters', 'AllowIncompleteDataset', 'true'", job);

                    returnData.CloseoutMsg = sourceDirectoryErrorMessage + "; to ignore this error, use " + jobParameterHint;
                    LogError(msg);

                    mShareConnection.DisconnectShareIfRequired();
                    returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    return true;
                }
            }
            catch (Exception ex)
            {
                returnData.CloseoutMsg = "Exception checking for a DATA.MS file";
                msg = returnData.CloseoutMsg + " at " + directoryPath;
                LogError(msg, true);
                LogError("Stack trace", ex);

                mShareConnection.DisconnectShareIfRequired();

                mToolState.HandleCopyException(returnData, ex);
                return true;
            }

            return false;
        }

        /// <summary>
        /// Look for an incomplete .UIMF file, which is either 0 bytes in size or has a corresponding .uimf-journal file
        /// </summary>
        /// <param name="directoryPath"></param>
        /// <param name="msg">Output: error message</param>
        /// <param name="returnData">Input/output: Return data</param>
        /// <returns>True if an incomplete .uimf file is found</returns>
        private bool IsIncompleteUimfFound(
            string directoryPath,
            out string msg,
            ToolReturnData returnData)
        {
            msg = string.Empty;

            try
            {
                var sourceDirectory = new DirectoryInfo(directoryPath);

                var uimfJournalFiles = sourceDirectory.GetFiles("*.uimf-journal");
                string sourceDirectoryErrorMessage = null;

                if (uimfJournalFiles.Length > 0)
                {
                    sourceDirectoryErrorMessage =
                        "Source directory has SQLite journal files, indicating data acquisition is in progress";
                }
                else
                {
                    var uimfFiles = sourceDirectory.GetFiles("*.uimf");
                    if (uimfFiles.Any(uimfFile => uimfFile.Length == 0))
                    {
                        sourceDirectoryErrorMessage = "Source directory has a zero-byte UIMF file";
                    }
                }

                if (!string.IsNullOrEmpty(sourceDirectoryErrorMessage))
                {
                    returnData.CloseoutMsg = sourceDirectoryErrorMessage;
                    msg = returnData.CloseoutMsg + " at " + directoryPath;
                    LogError(msg);

                    mShareConnection.DisconnectShareIfRequired();
                    returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    return true;
                }
            }
            catch (Exception ex)
            {
                returnData.CloseoutMsg = "Exception checking for zero-byte dataset files";
                msg = returnData.CloseoutMsg + " at " + directoryPath;
                LogError(msg, true);
                LogError("Stack trace", ex);

                mShareConnection.DisconnectShareIfRequired();

                mToolState.HandleCopyException(returnData, ex);
                return true;
            }

            return false;
        }

        private void DeleteZeroByteBrukerFiles(DirectoryInfo targetDirectory)
        {
            try
            {
                var fileNamesToDelete = new List<string>
                {
                    "ProjectCreationHelper",
                    "SyncHelper",
                    "lock.file"
                };

                var fileCountDeleted = 0;
                var deletedFileList = string.Empty;

                if (!targetDirectory.Exists)
                {
                    return;
                }

                foreach (var candidateFile in targetDirectory.GetFiles("*", SearchOption.AllDirectories).ToList())
                {
                    if (candidateFile.Length > 0)
                    {
                        continue;
                    }

                    if (!fileNamesToDelete.Contains(candidateFile.Name))
                    {
                        continue;
                    }

                    // Delete this zero-byte file
                    candidateFile.Delete();
                    fileCountDeleted++;
                    if (string.IsNullOrEmpty(deletedFileList))
                    {
                        deletedFileList = candidateFile.Name;
                    }
                    else
                    {
                        deletedFileList += ", " + candidateFile.Name;
                    }
                }

                if (fileCountDeleted > 0)
                {
                    LogWarning("Deleted " + fileCountDeleted + " zero byte files in the dataset directory: " + deletedFileList);
                }
            }
            catch (Exception ex)
            {
                LogError("Error in DeleteZeroByteBrukerFiles", ex);
            }
        }

        /// <summary>
        /// Find files to skip based on filename match specs in searchSpec
        /// </summary>
        /// <param name="sourceDirectory"></param>
        /// <param name="datasetInfo"></param>
        /// <param name="searchSpecList">Dictionary where keys are file specs to pass to .GetFiles() and values are the description of each key</param>
        /// <param name="returnData"></param>
        /// <param name="filesToSkip">Output: List of file names to skip</param>
        /// <returns>True if successful, false if an error</returns>
        private bool FindFilesToSkip(
            DirectoryInfo sourceDirectory,
            DatasetInfo datasetInfo,
            Dictionary<string, string> searchSpecList,
            ToolReturnData returnData,
            out SortedSet<string> filesToSkip)
        {
            filesToSkip = new SortedSet<string>();

            try
            {
                foreach (var searchItem in searchSpecList)
                {
                    var searchSpec = searchItem.Key;

                    var foundFiles = sourceDirectory.GetFiles(searchSpec, SearchOption.AllDirectories).ToList();

                    foreach (var file in foundFiles)
                    {
                        if (!filesToSkip.Contains(file.Name))
                        {
                            filesToSkip.Add(file.Name);
                        }
                    }

                    if (foundFiles.Count == 1)
                    {
                        var firstSkippedFile = foundFiles.FirstOrDefault();
                        if (firstSkippedFile != null)
                        {
                            LogMessage("Skipping " + searchItem.Value + ": " + firstSkippedFile.Name);
                        }
                    }
                    else if (foundFiles.Count > 1)
                    {
                        var firstSkippedFile = foundFiles.FirstOrDefault();
                        var lastSkippedFile = foundFiles.LastOrDefault();

                        if (firstSkippedFile != null && lastSkippedFile != null)
                        {
                            LogMessage("Skipping " + foundFiles.Count + " " + searchItem.Value + "s: " +
                                       "(" + firstSkippedFile.Name + " through " + lastSkippedFile.Name + ")");
                        }
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                returnData.CloseoutMsg = "Exception getting list of files to skip";
                LogError(returnData.CloseoutMsg + " for dataset " + datasetInfo.DatasetName, true);
                LogError("Stack trace", ex);

                mShareConnection.DisconnectShareIfRequired();

                mToolState.HandleCopyException(returnData, ex);
                return false;
            }
        }

        /// <summary>
        /// Capture a directory with no extension on the name (the directory name is nearly always the dataset name)
        /// </summary>
        /// <param name="msg">Output: error message</param>
        /// <param name="returnData">Input/output: Return data</param>
        /// <param name="datasetInfo">Dataset info</param>
        /// <param name="sourceDirectoryPath">Source directory (on instrument); datasetInfo.FileOrDirectoryName will be appended to this</param>
        /// <param name="datasetDirectoryPath">Destination directory; datasetInfo.FileOrDirectoryName will not be appended to this (contrast with CaptureDirectoryExt)</param>
        /// <param name="copyWithResume">True if using copy with resume</param>
        /// <param name="instrumentClass">Instrument class</param>
        public void CaptureDirectoryNoExt(
            out string msg,
            ToolReturnData returnData,
            DatasetInfo datasetInfo,
            string sourceDirectoryPath,
            string datasetDirectoryPath,
            bool copyWithResume,
            InstrumentClassInfo.InstrumentClass instrumentClass)
        {
            // List of file names to skip (not full paths)
            var filesToSkip = new SortedSet<string>();

            bool success;

            var sourceDirectory = new DirectoryInfo(Path.Combine(sourceDirectoryPath, datasetInfo.FileOrDirectoryName));
            var targetDirectory = new DirectoryInfo(datasetDirectoryPath);

            // Look for a zero-byte .UIMF file or a .UIMF journal file
            // Abort the capture if either is present
            if (IsIncompleteUimfFound(sourceDirectory.FullName, out msg, returnData))
            {
                return;
            }

            // Verify the directory doesn't contain a group of ".d" directories
            var dotDDirectories = sourceDirectory.GetDirectories("*.d", SearchOption.TopDirectoryOnly);
            if (dotDDirectories.Length > 1)
            {
                var allowMultipleDirectories = false;

                if (dotDDirectories.Length == 2)
                {
                    // If one directory contains a ser file and the other directory contains an analysis.baf, we'll allow this
                    // This is sometimes the case for the 15T_FTICR_Imaging
                    var serCount = 0;
                    var bafCount = 0;
                    foreach (var directory in dotDDirectories)
                    {
                        if (directory.GetFiles("ser", SearchOption.TopDirectoryOnly).Length == 1)
                        {
                            serCount++;
                        }

                        if (directory.GetFiles("analysis.baf", SearchOption.TopDirectoryOnly).Length == 1)
                        {
                            bafCount++;
                        }
                    }

                    if (bafCount == 1 && serCount == 1)
                    {
                        allowMultipleDirectories = true;
                    }
                }

                if (!allowMultipleDirectories && instrumentClass == InstrumentClassInfo.InstrumentClass.BrukerMALDI_Imaging_V2)
                {
                    // Effective July 2016, we allow Bruker Imaging datasets to have multiple .D subdirectories
                    // They typically each have their own ser file
                    allowMultipleDirectories = true;
                }

                if (!allowMultipleDirectories)
                {
                    returnData.CloseoutMsg = "Multiple .D subdirectories found in dataset directory";
                    msg = returnData.CloseoutMsg + " " + sourceDirectory.FullName;
                    LogError(msg);
                    returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    return;
                }
            }

            // Verify the directory doesn't contain ".IMF" files
            if (sourceDirectory.GetFiles("*.imf", SearchOption.TopDirectoryOnly).Length > 0)
            {
                returnData.CloseoutMsg = "Dataset directory contains a series of .IMF files -- upload a .UIMF file instead";
                msg = returnData.CloseoutMsg + " " + sourceDirectory.FullName;
                LogError(msg);
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return;
            }

            if (instrumentClass == InstrumentClassInfo.InstrumentClass.IMS_Agilent_TOF_UIMF)
            {
                // Possibly skip the Fragmentation_Profile.txt file
                var fragProfileFile = new FileInfo(Path.Combine(sourceDirectory.FullName, "Fragmentation_Profile.txt"));

                if (fragProfileFile.Exists && FragmentationProfileFileIsDefault(fragProfileFile))
                {
                    filesToSkip.Add(fragProfileFile.Name);
                }
            }

            if (instrumentClass == InstrumentClassInfo.InstrumentClass.FT_Booster_Data)
            {
                // Skip Thermo .Raw files
                foreach (var thermoRawFile in sourceDirectory.GetFiles("*.raw", SearchOption.AllDirectories))
                {
                    filesToSkip.Add(thermoRawFile.Name);
                }

                // Skip chunk .bin files
                foreach (var thermoRawFile in sourceDirectory.GetFiles("chunk*.bin", SearchOption.AllDirectories))
                {
                    filesToSkip.Add(thermoRawFile.Name);
                }
            }

            if (instrumentClass == InstrumentClassInfo.InstrumentClass.Sciex_QTrap)
            {
                // Make sure that it doesn't have more than 2 subdirectories (it typically won't have any, but we'll allow 2)
                if (sourceDirectory.GetDirectories("*", SearchOption.TopDirectoryOnly).Length > 2)
                {
                    returnData.CloseoutMsg = "Dataset directory has more than 2 subdirectories";
                    msg = returnData.CloseoutMsg + " " + sourceDirectory.FullName;
                    LogError(msg);
                    returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    return;
                }

                // Verify that the directory has a .wiff or a .wiff.scan file
                if (sourceDirectory.GetFiles("*.wiff*", SearchOption.TopDirectoryOnly).Length == 0)
                {
                    returnData.CloseoutMsg = "Dataset directory does not contain any .wiff files";
                    msg = returnData.CloseoutMsg + " " + sourceDirectory.FullName;
                    LogError(msg);
                    returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    return;
                }
            }

            // Verify the directory size is constant (indicates acquisition is actually finished)
            if (!VerifyConstantDirectorySize(sourceDirectory, returnData))
            {
                msg = "Dataset '" + datasetInfo.DatasetName + "' not ready";
                LogWarning(msg);
                mShareConnection.DisconnectShareIfRequired();

                if (string.IsNullOrWhiteSpace(returnData.CloseoutMsg))
                {
                    returnData.CloseoutMsg = "directory size changed";
                }

                if (returnData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS)
                {
                    returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_NOT_READY;
                }

                return;
            }

            // Copy the dataset directory to the storage server
            try
            {
                if (copyWithResume)
                {
                    const bool recurse = true;
                    success = CopyDirectoryWithResume(sourceDirectory.FullName, targetDirectory.FullName, recurse, returnData, filesToSkip);
                }
                else
                {
                    mFileTools.CopyDirectory(sourceDirectory.FullName, targetDirectory.FullName, filesToSkip.ToList());
                    success = true;
                }

                if (success)
                {
                    msg = "Copied directory " + sourceDirectory.FullName + " to " + targetDirectory.FullName + mShareConnection.GetConnectionDescription();
                    LogMessage(msg);

                    AutoFixFilesWithInvalidChars(datasetInfo.DatasetName, targetDirectory);
                }
                else
                {
                    msg = "Unknown error copying the dataset directory";
                }
            }
            catch (Exception ex)
            {
                msg = "Exception copying dataset directory " + sourceDirectory.FullName + mShareConnection.GetConnectionDescription();
                LogError(msg, true);
                LogError("Stack trace", ex);

                mToolState.HandleCopyException(returnData, ex);
                return;
            }
            finally
            {
                mShareConnection.DisconnectShareIfRequired();
            }

            if (success)
            {
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
            }
            else
            {
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Capture a Bruker imaging directory
        /// </summary>
        /// <param name="msg">Output: error message</param>
        /// <param name="returnData">Input/output: Return data</param>
        /// <param name="datasetInfo">Dataset info</param>
        /// <param name="sourceDirectoryPath">Source directory (on instrument); datasetInfo.FileOrDirectoryName will be appended to this</param>
        /// <param name="datasetDirectoryPath">Destination directory; datasetInfo.FileOrDirectoryName will not be appended to this (contrast with CaptureDirectoryExt)</param>
        /// <param name="copyWithResume">True if using copy with resume</param>
        public void CaptureBrukerImaging(
            out string msg,
            ToolReturnData returnData,
            DatasetInfo datasetInfo,
            string sourceDirectoryPath,
            string datasetDirectoryPath,
            bool copyWithResume)
        {
            bool success;

            // First, verify the directory size is constant (indicates acquisition is actually finished)
            var sourceDirectory = new DirectoryInfo(Path.Combine(sourceDirectoryPath, datasetInfo.FileOrDirectoryName));
            var targetDirectory = new DirectoryInfo(datasetDirectoryPath);

            // Check to see if the directories have been zipped
            var zipFileList = Directory.GetFiles(sourceDirectory.FullName, "*.zip");
            if (zipFileList.Length < 1)
            {
                // Data files haven't been zipped, so throw error
                returnData.CloseoutMsg = "No zip files found in dataset directory";
                msg = returnData.CloseoutMsg + " at " + sourceDirectory.FullName;
                LogError(msg);
                mShareConnection.DisconnectShareIfRequired();

                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return;
            }

            if (!VerifyConstantDirectorySize(sourceDirectory, returnData))
            {
                msg = "Dataset '" + datasetInfo.DatasetName + "' not ready";
                LogWarning(msg);
                mShareConnection.DisconnectShareIfRequired();

                if (string.IsNullOrWhiteSpace(returnData.CloseoutMsg))
                {
                    returnData.CloseoutMsg = "directory size changed";
                }

                if (returnData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS)
                {
                    returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_NOT_READY;
                }

                return;
            }

            // Make a dataset directory
            try
            {
                MakeDirectoryIfMissing(targetDirectory.FullName);
            }
            catch (Exception ex)
            {
                returnData.CloseoutMsg = "Exception creating dataset directory";
                msg = returnData.CloseoutMsg + " at " + targetDirectory.FullName;
                LogError(msg, true);
                LogError("Stack trace", ex);

                mShareConnection.DisconnectShareIfRequired();

                mToolState.HandleCopyException(returnData, ex);
                return;
            }

            // Copy only the files in the dataset directory to the storage server. Do not copy directories
            try
            {
                if (copyWithResume)
                {
                    const bool recurse = false;
                    success = CopyDirectoryWithResume(sourceDirectory.FullName, targetDirectory.FullName, recurse, returnData);
                }
                else
                {
                    foreach (var fileToCopy in Directory.GetFiles(sourceDirectory.FullName))
                    {
                        var fi = new FileInfo(fileToCopy);
                        fi.CopyTo(Path.Combine(targetDirectory.FullName, fi.Name));
                    }
                    success = true;
                }

                if (success)
                {
                    msg = "Copied files in directory " + sourceDirectory.FullName + " to " + targetDirectory.FullName + mShareConnection.GetConnectionDescription();
                    LogMessage(msg);

                    AutoFixFilesWithInvalidChars(datasetInfo.DatasetName, targetDirectory);
                }
                else
                {
                    msg = "Unknown error copying the dataset files";
                }
            }
            catch (Exception ex)
            {
                returnData.CloseoutMsg = "Exception copying files from dataset directory";
                msg = returnData.CloseoutMsg + " " + sourceDirectory.FullName + mShareConnection.GetConnectionDescription();
                LogError(msg, true);
                LogError("Stack trace", ex);

                mShareConnection.DisconnectShareIfRequired();

                mToolState.HandleCopyException(returnData, ex);
                return;
            }
            finally
            {
                mShareConnection.DisconnectShareIfRequired();
            }

            if (success)
            {
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
            }
            else
            {
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Capture a directory from a Bruker_Spot instrument
        /// </summary>
        /// <param name="msg">Output: error message</param>
        /// <param name="returnData">Input/output: Return data</param>
        /// <param name="datasetInfo">Dataset info</param>
        /// <param name="sourceDirectoryPath">Source directory (on instrument); datasetInfo.FileOrDirectoryName will be appended to this</param>
        /// <param name="datasetDirectoryPath">Destination directory; datasetInfo.FileOrDirectoryName will not be appended to this (contrast with CaptureDirectoryExt)</param>
        public void CaptureBrukerSpot(
            out string msg,
            ToolReturnData returnData,
            DatasetInfo datasetInfo,
            string sourceDirectoryPath,
            string datasetDirectoryPath)
        {
            // Verify that the directory size is constant (indicates acquisition is actually finished)
            var sourceDirectory = new DirectoryInfo(Path.Combine(sourceDirectoryPath, datasetInfo.FileOrDirectoryName));
            var targetDirectory = new DirectoryInfo(datasetDirectoryPath);

            // Verify the dataset directory doesn't contain any .zip files
            var zipFiles = sourceDirectory.GetFiles("*.zip");

            if (zipFiles.Length > 0)
            {
                returnData.CloseoutMsg = "Zip files found in dataset directory";
                msg = returnData.CloseoutMsg + " " + sourceDirectory.FullName;
                LogError(msg);
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return;
            }

            // Check whether the dataset directory contains just one data directory or multiple data directories
            var dataDirectories = sourceDirectory.GetDirectories().ToList();

            if (dataDirectories.Count < 1)
            {
                returnData.CloseoutMsg = "No subdirectories were found in the dataset directory ";
                msg = returnData.CloseoutMsg + " " + sourceDirectory.FullName;
                LogError(msg);
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return;
            }

            if (dataDirectories.Count > 1)
            {
                // Make sure the subdirectories match the naming convention for MALDI spot directories
                // Example directory names:
                //  0_D4
                //  0_E10
                //  0_N4

                const string MALDI_SPOT_DIRECTORY_REGEX = @"^\d_[A-Z]\d+$";
                var maldiSpotDirectoryMatcher = new Regex(MALDI_SPOT_DIRECTORY_REGEX, RegexOptions.Compiled | RegexOptions.IgnoreCase);

                foreach (var directory in dataDirectories)
                {
                    LogDebug("Test directory " + directory + " against RegEx " + maldiSpotDirectoryMatcher);

                    if (!maldiSpotDirectoryMatcher.IsMatch(directory.Name, 0))
                    {
                        returnData.CloseoutMsg = "Dataset directory contains multiple subdirectories, but directory " + directory.Name + " does not match the expected pattern";
                        msg = returnData.CloseoutMsg + " (" + maldiSpotDirectoryMatcher + "); see " + sourceDirectory.FullName;
                        LogError(msg);
                        returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                        return;
                    }
                }
            }

            if (!VerifyConstantDirectorySize(sourceDirectory, returnData))
            {
                msg = "Dataset '" + datasetInfo.DatasetName + "' not ready";
                LogWarning(msg);
                mShareConnection.DisconnectShareIfRequired();

                if (string.IsNullOrWhiteSpace(returnData.CloseoutMsg))
                {
                    returnData.CloseoutMsg = "directory size changed";
                }

                if (returnData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS)
                {
                    returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_NOT_READY;
                }

                return;
            }

            // Copy the dataset directory (and all subdirectories) to the storage server
            try
            {
                mFileTools.CopyDirectory(sourceDirectory.FullName, targetDirectory.FullName);
                msg = "Copied directory " + sourceDirectory.FullName + " to " + targetDirectory.FullName + mShareConnection.GetConnectionDescription();
                LogMessage(msg);
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                msg = "Exception copying dataset directory " + sourceDirectory.FullName + mShareConnection.GetConnectionDescription();
                LogError(msg, true);
                LogError("Stack trace", ex);

                mToolState.HandleCopyException(returnData, ex);
            }
            finally
            {
                mShareConnection.DisconnectShareIfRequired();
            }
        }

        private bool CopyDirectoryWithResume(
            string sourceDirectoryPath,
            string targetDirectoryPath,
            bool recurse,
            ToolReturnData returnData)
        {
            return CopyDirectoryWithResume(sourceDirectoryPath, targetDirectoryPath, recurse, returnData, new SortedSet<string>());
        }

        private bool CopyDirectoryWithResume(
            string sourceDirectoryPath,
            string targetDirectoryPath,
            bool recurse,
            ToolReturnData returnData,
            SortedSet<string> filesToSkip)
        {
            const FileTools.FileOverwriteMode overwriteMode = FileTools.FileOverwriteMode.OverWriteIfDateOrLengthDiffer;
            const int MAX_RETRY_TIME_HOURS = 6;

            var success = false;
            var doCopy = true;
            var directoryCopyStartTime = DateTime.UtcNow;

            while (doCopy)
            {
                if (DateTime.UtcNow.Subtract(directoryCopyStartTime).TotalHours > MAX_RETRY_TIME_HOURS)
                {
                    success = false;
                    returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    returnData.EvalCode = EnumEvalCode.EVAL_CODE_FAILED;
                    returnData.CloseoutMsg = string.Format("Aborting CopyDirectoryWithResume since over {0} hours has elapsed", MAX_RETRY_TIME_HOURS);
                    LogError(returnData.CloseoutMsg);
                    break;
                }

                var copyStart = DateTime.UtcNow;

                try
                {
                    // Clear any previous errors
                    mToolState.ErrorMessage = string.Empty;

                    success = mFileTools.CopyDirectoryWithResume(
                        sourceDirectoryPath, targetDirectoryPath,
                        recurse, overwriteMode, filesToSkip.ToList(),
                        out var fileCountSkipped, out var fileCountResumed, out var fileCountNewlyCopied);

                    doCopy = false;

                    if (success)
                    {
                        LogDebug("  directory copy complete; CountCopied = {0}; CountSkipped = {1}; CountResumed = {2}",
                            fileCountNewlyCopied, fileCountSkipped, fileCountResumed);
                    }
                    else
                    {
                        LogError("  directory copy failed copying {0} to {1}{2}",
                            sourceDirectoryPath, targetDirectoryPath, mShareConnection.GetConnectionDescription());
                    }
                }
                catch (UnauthorizedAccessException ex)
                {
                    string msg;
                    if (string.IsNullOrWhiteSpace(mFileTools.CurrentSourceFile))
                    {
                        msg = "Access denied while copying directory: ";
                    }
                    else
                    {
                        msg = "Access denied while copying " + mFileTools.CurrentSourceFile + ": ";
                    }

                    mToolState.ErrorMessage = msg;

                    if (ex.Message.Length <= 350)
                    {
                        msg += ex.Message;
                    }
                    else
                    {
                        msg += ex.Message.Substring(0, 350);
                    }

                    LogError(msg);

                    doCopy = false;

                    mToolState.HandleCopyException(returnData, ex);
                }
                catch (Exception ex)
                {
                    string msg;
                    if (string.IsNullOrWhiteSpace(mFileTools.CurrentSourceFile))
                    {
                        msg = "Error while copying directory: ";
                    }
                    else
                    {
                        msg = "Error while copying " + mFileTools.CurrentSourceFile + ": ";
                    }

                    mToolState.ErrorMessage = msg;

                    if (ex.Message.Length <= 350)
                    {
                        msg += ex.Message;
                    }
                    else
                    {
                        msg += ex.Message.Substring(0, 350);
                    }

                    LogError(msg);

                    doCopy = false;
                    if (mFileTools.CurrentCopyStatus is FileTools.CopyStatus.BufferedCopy or FileTools.CopyStatus.BufferedCopyResume)
                    {
                        // Exception occurred during the middle of a buffered copy
                        // If at least 10 seconds have elapsed, auto-retry the copy again
                        var elapsedTime = DateTime.UtcNow.Subtract(copyStart).TotalSeconds;
                        if (elapsedTime >= 10)
                        {
                            doCopy = true;
                            msg = "  " + elapsedTime.ToString("0") + " seconds have elapsed; will attempt to resume copy";
                            LogMessage(msg);
                        }
                    }

                    mToolState.HandleCopyException(returnData, ex);
                }
            }

            if (success)
            {
                // CloseoutType may have been set to CLOSEOUT_FAILED by mToolState.HandleCopyException; reset it to CLOSEOUT_SUCCESS
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
                returnData.EvalCode = EnumEvalCode.EVAL_CODE_SUCCESS;
            }

            return success;
        }

        private bool FragmentationProfileFileIsDefault(FileSystemInfo fragProfileFile)
        {
            try
            {
                // RegEx to match lines of the form:
                // 0, 0, 0, 0, 0
                var zeroLineMatcher = new Regex("^[0, ]+$", RegexOptions.Compiled);

                using var reader = new StreamReader(new FileStream(fragProfileFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                var dataLineCount = 0;
                var lineAllZeroes = false;

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();
                    if (string.IsNullOrWhiteSpace(dataLine))
                    {
                        continue;
                    }

                    dataLineCount++;

                    lineAllZeroes = zeroLineMatcher.IsMatch(dataLine);
                }

                if (dataLineCount == 1 && lineAllZeroes)
                {
                    LogMessage("Skipping capture of default fragmentation profile file, " + fragProfileFile.FullName);
                    return true;
                }

                return false;
            }
            catch (Exception ex)
            {
                LogError("Exception examining the Fragmentation_Profile.txt file", ex);
            }
            return false;
        }
    }
}
