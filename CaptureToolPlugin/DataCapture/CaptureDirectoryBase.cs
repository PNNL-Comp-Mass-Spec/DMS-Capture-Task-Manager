using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using CaptureTaskManager;
using PRISM;

namespace CaptureToolPlugin.DataCapture
{
    /// <summary>
    /// Abstract class for capturing directory-based dataset "files"
    /// </summary>
    internal abstract class CaptureDirectoryBase : CaptureBase
    {
        // Ignore Spelling: bionet, Bruker, mcf, ser, uimf

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="data">Initialization data object</param>
        protected CaptureDirectoryBase(CaptureInitData data) : base(data)
        { }

        /// <summary>
        /// Look for files in the dataset directory with spaces in the name
        /// If the filename otherwise matches the dataset, rename it
        /// </summary>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="datasetDirectory">Dataset directory to search</param>
        protected void AutoFixFilesWithInvalidChars(string datasetName, DirectoryInfo datasetDirectory)
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
        /// Find files to skip based on filename match specs in searchSpec
        /// </summary>
        /// <param name="sourceDirectory"></param>
        /// <param name="datasetInfo"></param>
        /// <param name="searchSpecList">Dictionary where keys are file specs to pass to .GetFiles() and values are the description of each key</param>
        /// <param name="returnData"></param>
        /// <param name="filesToSkip">Output: List of file names to skip</param>
        /// <returns>True if successful, false if an error</returns>
        protected bool FindFilesToSkip(
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
        /// Look for an incomplete .UIMF file, which is either 0 bytes in size or has a corresponding .uimf-journal file
        /// </summary>
        /// <param name="directoryPath"></param>
        /// <param name="msg">Output: error message</param>
        /// <param name="returnData">Input/output: Return data</param>
        /// <returns>True if an incomplete .uimf file is found</returns>
        protected bool IsIncompleteUimfFound(
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

        protected bool CopyDirectoryWithResume(
            string sourceDirectoryPath,
            string targetDirectoryPath,
            bool recurse,
            ToolReturnData returnData)
        {
            return CopyDirectoryWithResume(sourceDirectoryPath, targetDirectoryPath, recurse, returnData, new SortedSet<string>());
        }

        protected bool CopyDirectoryWithResume(
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
                // CloseoutType may have been set to CLOSEOUT_FAILED by HandleCopyException; reset it to CLOSEOUT_SUCCESS
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
                returnData.EvalCode = EnumEvalCode.EVAL_CODE_SUCCESS;
            }

            return success;
        }
    }
}
