using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using CaptureTaskManager;
using PRISM;

namespace CaptureToolPlugin.DataCapture
{
    /// <summary>
    /// Capture implementation for "directory with extension" dataset files
    /// </summary>
    internal class DirectoryExtCapture : CaptureDirectoryBase
    {
        // Ignore Spelling: bionet, Bruker, mcf

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="data">Initialization data object</param>
        public DirectoryExtCapture(CaptureInitData data) : base(data)
        { }

        /// <inheritdoc />
        public override void Capture(
            out string msg,
            ToolReturnData returnData,
            DatasetInfo datasetInfo,
            string sourceDirectoryPath,
            string datasetDirectoryPath,
            bool copyWithResume,
            InstrumentClass instrumentClass,
            string instrumentName,
            ITaskParams taskParams
        )
        {
            CaptureDirectoryExt(out msg, returnData, datasetInfo, sourceDirectoryPath, datasetDirectoryPath, copyWithResume, instrumentClass, instrumentName, taskParams);
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
        private void CaptureDirectoryExt(
            out string msg,
            ToolReturnData returnData,
            DatasetInfo datasetInfo,
            string sourceDirectoryPath,
            string datasetDirectoryPath,
            bool copyWithResume,
            InstrumentClass instrumentClass,
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

            if (instrumentClass == InstrumentClass.Agilent_Ion_Trap)
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

                    var jobParameterHint = string.Format("Call cap.add_update_task_parameter ({0}, 'JobParameters', 'AllowIncompleteDataset', 'true');", job);

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
    }
}
