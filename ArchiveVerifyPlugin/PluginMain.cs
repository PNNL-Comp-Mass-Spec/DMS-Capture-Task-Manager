﻿using CaptureTaskManager;
using Pacifica.Core;
using Pacifica.DMSDataUpload;
using Pacifica.Json;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;

namespace ArchiveVerifyPlugin
{
    /// <summary>
    /// Archive verify plugin
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class PluginMain : ToolRunnerBase
    {
        // Ignore Spelling: dmsarch, Frodo, hashsum, keyvalue, Methow, myemsl, Pacifica, subdir, svc-dms
        private const string HASH_RESULTS_FILE_PREFIX = "results.";
        private const string DEFAULT_HASH_RESULTS_FOLDER_PATH = @"\\proto-7\MD5Results";
        private const string DEFAULT_HASH_RESULTS_BACKUP_FOLDER_PATH = @"\\proto-5\MD5ResultsBackup";
        private ToolReturnData mRetData = new();

        private int mTotalMismatchCount;
        /// <summary>
        /// Runs the Archive Verify step tool
        /// </summary>
        /// <returns>Class with completionCode, completionMessage, evaluationCode, and evaluationMessage</returns>
        public override ToolReturnData RunTool()
        {
            LogDebug("Starting ArchiveVerifyPlugin.PluginMain.RunTool");

            // Perform base class operations, if any
            mRetData = base.RunTool();

            if (mRetData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED)
            {
                return mRetData;
            }

            // Do not call StoreToolVersionInfo to store the version info in the database
            // Not required since the ArchiveVerify plugin uses components whose version was
            // already logged by the DatasetArchive plugin, and we don't need to make the
            // additional database call to set_ctm_step_task_tool_version

            var writeToLog = mDebugLevel >= 4;
            LogDebug("Verifying files in MyEMSL for dataset '" + mDataset + "'", writeToLog);

            // Set this to Success for now
            mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
            var success = false;

            mTotalMismatchCount = 0;

            int statusNum;
            byte ingestStepsCompleted;
            bool fatalError;

            try
            {
                // Examine the MyEMSL ingest status page
                success = CheckUploadStatus(out statusNum, out ingestStepsCompleted, out fatalError);
            }
            catch (Exception ex)
            {
                mRetData.CloseoutMsg = "Exception checking archive status (ArchiveVerifyPlugin): " + ex.Message;
                mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;

                LogError("Exception checking archive status for job " + mJob, ex);

                statusNum = 0;
                ingestStepsCompleted = 0;
                fatalError = true;
            }

            if (success)
            {
                // Confirm that the files are visible in the metadata search results (using metadataObject.GetDatasetFilesInMyEMSL)
                // If data is found, CreateOrUpdateHashResultsFile will also be called
                success = VisibleInMetadata(out var metadataFilePath, out var transactionID);

                UpdateIngestStepsCompletedOneTask(statusNum, ingestStepsCompleted, transactionID, fatalError);

                if (!success)
                {
                    if (string.IsNullOrWhiteSpace(mRetData.CloseoutMsg))
                    {
                        if (mTotalMismatchCount > 0)
                        {
                            mRetData.CloseoutMsg = string.Format(
                                "{0} files did not match the metadata reported by MyEMSL", mTotalMismatchCount);
                            mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                            mRetData.EvalCode = EnumEvalCode.EVAL_CODE_FAILURE_DO_NOT_RETRY;
                        }
                        else
                        {
                            mRetData.CloseoutMsg = "Not visible in metadata";
                        }
                    }
                }
                else if (!string.IsNullOrEmpty(metadataFilePath))
                {
                    DeleteMetadataFile(metadataFilePath);
                }
            }
            else if (statusNum > 0)
            {
                UpdateIngestStepsCompletedOneTask(statusNum, ingestStepsCompleted, 0, fatalError);

                if (fatalError)
                {
                    if (mRetData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS)
                    {
                        mRetData.CloseoutMsg = "UpdateIngestStepsCompletedOneTask reports a fatal error";
                        mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    }
                }
                else
                {
                    mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_NOT_READY;
                }
            }
            else
            {
                mRetData.CloseoutMsg = "StatusNum is zero";
                mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
            }

            if (success)
            {
                // Everything was good
                LogMessage("MyEMSL verification successful for job " + mJob + ", dataset " + mDataset);
                mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;

                // Note that procedure cap.set_ctm_step_task_complete will update MyEMSL State values if mRetData.EvalCode = 5
                mRetData.EvalCode = EnumEvalCode.EVAL_CODE_VERIFIED_IN_MYEMSL;
            }
            else
            {
                // There was a problem (or the data is not yet ready in MyEMSL)
                if (mRetData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS)
                {
                    LogWarning(
                        "Success is false yet CloseoutType is EnumCloseOutType.CLOSEOUT_SUCCESS; " +
                        "track down where to properly change CloseoutType " +
                        "(Job " + mJob + " on " + mMgrName + ")", true);

                    mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_NOT_READY;

                    if (string.IsNullOrWhiteSpace(mRetData.CloseoutMsg))
                    {
                        mRetData.CloseoutMsg = "Unknown reason";
                    }
                }
            }

            LogDebug("Completed PluginMain.RunTool");

            return mRetData;
        }

        /// <summary>
        /// Examine the upload status
        /// If not complete, this manager will return completionCode CLOSEOUT_NOT_READY (2)
        /// which will tell the DMS database to reset the task to state 2 and bump up the Next_Try value by 30 minutes
        /// </summary>
        /// <param name="statusNum"></param>
        /// <param name="ingestStepsCompleted"></param>
        /// <param name="fatalError">Output: true if the CloseoutType is Failed or the EvalCode is DoNotRetry</param>
        /// <returns>True if the ingest process is complete, otherwise false</returns>
        private bool CheckUploadStatus(out int statusNum, out byte ingestStepsCompleted, out bool fatalError)
        {
            var statusURI = mTaskParams.GetParam("MyEMSL_Status_URI", string.Empty);

            if (string.IsNullOrEmpty(statusURI))
            {
                const string msg = "MyEMSL_Status_URI is empty; cannot verify upload status";
                mRetData.CloseoutMsg = msg;
                mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                mRetData.EvalCode = EnumEvalCode.EVAL_CODE_FAILURE_DO_NOT_RETRY;
                LogError(msg);

                statusNum = 0;
                ingestStepsCompleted = 0;
                fatalError = true;

                return false;
            }

            var statusChecker = new MyEMSLStatusCheck();
            RegisterEvents(statusChecker);

            try
            {
                mRetData.CloseoutMsg = string.Empty;

                var ingestSuccess = GetMyEMSLIngestStatus(
                    mJob, statusChecker, statusURI,
                    mRetData, out _, out var currentTask, out var percentComplete);

                // Examine the current task and percent complete to determine the number of ingest steps completed
                ingestStepsCompleted = MyEMSLStatusCheck.DetermineIngestStepsCompleted(currentTask, percentComplete, 0);

                fatalError =
                    mRetData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED &&
                    mRetData.EvalCode == EnumEvalCode.EVAL_CODE_FAILURE_DO_NOT_RETRY;

                statusNum = MyEMSLStatusCheck.GetStatusNumFromURI(statusURI);

                return ingestSuccess;
            }
            catch (Exception ex)
            {
                if (ex.Message.Contains("timed out"))
                {
                    mRetData.CloseoutMsg = "Error checking upload status; lookup timed out";
                    LogError(mRetData.CloseoutMsg);
                }
                else if (ex.Message.Contains("401 Authorization Required"))
                {
                    mRetData.CloseoutMsg = "Error checking upload status; user authorization error";
                    LogError(mRetData.CloseoutMsg);
                }
                else
                {
                    mRetData.CloseoutMsg = "Exception checking upload status";
                    LogError(mRetData.CloseoutMsg + ": " + ex.Message, ex);
                }
            }

            statusNum = 0;
            ingestStepsCompleted = 0;
            fatalError = true;

            return false;
        }

        /// <summary>
        /// Compare the files in archivedFiles to the files in the metadata.txt file
        /// If metadata.txt file is missing, compare to files actually on disk
        /// </summary>
        /// <param name="remoteFiles"></param>
        /// <param name="metadataFilePath"></param>
        /// <param name="transactionId">The TransactionID used by the majority of the matching files</param>
        /// <returns>True if all the files match, false if a mismatch or an error</returns>
        private bool CompareArchiveFilesToExpectedFiles(
            IReadOnlyCollection<MyEMSLReader.ArchivedFileInfo> remoteFiles,
            out string metadataFilePath,
            out long transactionId)
        {
            metadataFilePath = string.Empty;
            transactionId = 0;

            try
            {
                var transferDirectoryPathBase = mTaskParams.GetParam("TransferDirectoryPath", mTaskParams.GetParam("TransferFolderPath"));

                if (!ParameterDefined("TransferDirectoryPath", transferDirectoryPathBase))
                {
                    return false;
                }

                if (string.IsNullOrEmpty(mDataset))
                {
                    mRetData.CloseoutMsg = "mDataset is empty; unable to continue";
                    LogError(mRetData.CloseoutMsg);
                    return false;
                }

                var transferDirectoryPath = Path.Combine(transferDirectoryPathBase, mDataset);

                var jobNumber = mTaskParams.GetParam("Job", string.Empty);

                if (!ParameterDefined("Job", jobNumber))
                {
                    return false;
                }

                var ignoreMyEMSLFileTrackingError = mTaskParams.GetParam("IgnoreMyEMSLFileTrackingError", false);

                var config = new Configuration();

                var metadataObject = new DMSMetadataObject(config, mMgrName, mJob, mFileTools)
                {
                    TraceMode = mTraceMode,
                    IgnoreMyEMSLFileTrackingError = ignoreMyEMSLFileTrackingError
                };

                // Attach the events
                RegisterEvents(metadataObject);

                var metadataFile = new FileInfo(Path.Combine(transferDirectoryPath, Utilities.GetMetadataFilenameForJob(jobNumber)));

                if (metadataFile.Exists)
                {
                    metadataFilePath = metadataFile.FullName;

                    CompareToMetadataFile(
                        metadataObject,
                        remoteFiles,
                        metadataFile,
                        out var matchCountToMetadata,
                        out var mismatchCountToMetadata,
                        out transactionId);

                    if (matchCountToMetadata > 0 && mismatchCountToMetadata == 0)
                    {
                        // Everything matches up
                        return true;
                    }

                    if (mismatchCountToMetadata > 0)
                    {
                        if (mTotalMismatchCount == 0)
                        {
                            LogError("MyEmsl verification errors for dataset " + mDataset + ", job " + mJob);
                        }

                        mTotalMismatchCount += mismatchCountToMetadata;

                        var matchStats = "SHA-1 mismatch between local files in metadata.txt file and MyEMSL; " +
                                         "MatchCount=" + matchCountToMetadata + ", " +
                                         "MismatchCount=" + mismatchCountToMetadata;

                        LogError(" ... " + matchStats);
                        mRetData.CloseoutMsg = matchStats;
                        mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                        mRetData.EvalCode = EnumEvalCode.EVAL_CODE_FAILURE_DO_NOT_RETRY;

                        return false;
                    }
                }

                // Metadata file was missing or empty; compare to local files on disk

                // Look for files that should have been uploaded, compute a SHA-1 hash for each, and compare those hashes to existing files in MyEMSL

                ResetTimestampForQueueWaitTimeLogging();

                var datasetFilesLocal = metadataObject.FindDatasetFilesToArchive(
                    mTaskParams.TaskDictionary,
                    mMgrParams.MgrParams,
                    out _);

                if (datasetFilesLocal.Count == 0)
                {
                    if (mTotalMismatchCount == 0)
                    {
                        LogError("MyEmsl verification errors for dataset " + mDataset + ", job " + mJob);
                    }

                    mTotalMismatchCount++;

                    mRetData.CloseoutMsg = "Local files were not found for this dataset; unable to compare SHA-1 hashes to MyEMSL values";
                    LogError(" ... " + mRetData.CloseoutMsg);
                    return false;
                }

                // Keys are relative file paths (Windows slashes); values are the SHA-1 hash values
                var filePathHashMap = new Dictionary<string, string>();

                foreach (var datasetFile in datasetFilesLocal)
                {
                    var relativeFilePathWindows = datasetFile.RelativeDestinationFullPath.Replace("/", @"\");
                    filePathHashMap.Add(relativeFilePathWindows, datasetFile.Sha1HashHex);
                }

                var transactionIdStats = new Dictionary<long, int>();

                CompareArchiveFilesToList(
                    remoteFiles,
                    out var matchCountToDisk,
                    out var mismatchCountToDisk,
                    filePathHashMap,
                    transactionIdStats);

                transactionId = GetBestTransactionId(transactionIdStats);

                if (matchCountToDisk > 0 && mismatchCountToDisk == 0)
                {
                    // Everything matches up
                    return true;
                }

                if (mismatchCountToDisk > 0)
                {
                    if (mTotalMismatchCount == 0)
                    {
                        LogError("MyEmsl verification errors for dataset " + mDataset + ", job " + mJob);
                    }

                    mTotalMismatchCount += mismatchCountToDisk;

                    mRetData.CloseoutMsg = "SHA-1 mismatch between local files on disk and MyEMSL; MatchCount=" + matchCountToDisk + ", MismatchCount=" + mismatchCountToDisk;
                    mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    mRetData.EvalCode = EnumEvalCode.EVAL_CODE_FAILURE_DO_NOT_RETRY;

                    LogError(" ... " + mRetData.CloseoutMsg);
                }

                return false;
            }
            catch (Exception ex)
            {
                LogError("Exception in CompareArchiveFilesToExpectedFiles", ex);
                return false;
            }
        }

        /// <summary>
        /// Compare local files to files in MyEMSL
        /// </summary>
        /// <param name="remoteFiles">Files in MyEMSL</param>
        /// <param name="matchCount"></param>
        /// <param name="mismatchCount"></param>
        /// <param name="filePathHashMap">Local files; keys are relative file paths (Windows slashes); values are the SHA-1 hash values</param>
        /// <param name="transactionIdStats">Keys are transaction IDs, values are the number of files for each transaction ID</param>
        private void CompareArchiveFilesToList(
            IReadOnlyCollection<MyEMSLReader.ArchivedFileInfo> remoteFiles,
            out int matchCount,
            out int mismatchCount,
            IReadOnlyDictionary<string, string> filePathHashMap,
            // ReSharper disable once SuggestBaseTypeForParameter
            Dictionary<long, int> transactionIdStats)
        {
            matchCount = 0;
            mismatchCount = 0;

            // Make sure each of the files in filePathHashMap is present in archivedFiles
            foreach (var metadataFile in filePathHashMap)
            {
                var remoteFileCandidates = (from item in remoteFiles where item.RelativePathWindows == metadataFile.Key select item).ToList();

                if (remoteFileCandidates.Count == 0)
                {
                    if (mTotalMismatchCount == 0)
                    {
                        LogError("MyEmsl verification errors for dataset " + mDataset + ", job " + mJob);
                    }

                    mTotalMismatchCount++;

                    mismatchCount++;

                    LogError(" ... file {0} not found in MyEMSL (CompareArchiveFilesToList)", metadataFile.Key);
                }
                else
                {
                    var matchingRemoteFiles = (from item in remoteFileCandidates
                                               where item.Hash == metadataFile.Value
                                               select item).ToList();

                    if (matchingRemoteFiles.Count > 0)
                    {
                        matchCount++;

                        foreach (var remoteFileVersion in matchingRemoteFiles)
                        {
                            if (transactionIdStats.TryGetValue(remoteFileVersion.TransactionID, out var fileCount))
                            {
                                transactionIdStats[remoteFileVersion.TransactionID] = fileCount + 1;
                            }
                            else
                            {
                                transactionIdStats.Add(remoteFileVersion.TransactionID, 1);
                            }
                        }
                    }
                    else
                    {
                        if (mTotalMismatchCount == 0)
                        {
                            LogError("MyEmsl verification errors for dataset " + mDataset + ", job " + mJob);
                        }

                        mTotalMismatchCount++;

                        var remoteFile = remoteFileCandidates.First();

                        LogError(" ... file mismatch for {0}; MyEMSL reports {1} but expecting {2}",
                            remoteFile.RelativePathWindows, remoteFile.Hash, metadataFile.Value);

                        mismatchCount++;
                    }
                }
            }
        }

        /// <summary>
        /// Compare the files that MyEMSL is tracking for this dataset to the files in the metadata file
        /// </summary>
        /// <param name="metadataObject"></param>
        /// <param name="remoteFiles">Files in MyEMSL</param>
        /// <param name="metadataFileInfo"></param>
        /// <param name="matchCount"></param>
        /// <param name="mismatchCount"></param>
        /// <param name="transactionId">The TransactionID used by the majority of the matching files</param>
        private void CompareToMetadataFile(
            DMSMetadataObject metadataObject,
            IReadOnlyCollection<MyEMSLReader.ArchivedFileInfo> remoteFiles,
            FileInfo metadataFileInfo,
            out int matchCount,
            out int mismatchCount,
            out long transactionId)
        {
            matchCount = 0;
            mismatchCount = 0;
            transactionId = 0;

            var transactionIdStats = new Dictionary<long, int>();

            // Parse the contents of the file
            string metadataJson;

            using (var metadataFileReader = new StreamReader(new FileStream(metadataFileInfo.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
            {
                metadataJson = metadataFileReader.ReadToEnd();
            }

            if (string.IsNullOrEmpty(metadataJson))
            {
                if (mTotalMismatchCount == 0)
                {
                    LogError("MyEmsl verification errors for dataset " + mDataset + ", job " + mJob);
                }

                mTotalMismatchCount++;

                LogError(" ... metadata file is empty: " + metadataFileInfo.FullName);
                return;
            }

            // metadataInfo is a list of Dictionaries
            var metadataInfo = JsonTools.JsonToUploadMetadata(metadataJson, metadataFileInfo.FullName, "PluginMain.CompareToMetadataFile", out _);

            // This list tracks files that were previously pushed to MyEMSL
            var metadataFiles = metadataInfo.Where(x => x.Valid && x is UploadMetadataFile).Cast<UploadMetadataFile>().ToList();

            if (metadataFiles.Count == 0)
            {
                if (mTotalMismatchCount == 0)
                {
                    LogError("MyEmsl verification errors for dataset " + mDataset + ", job " + mJob);
                }

                mTotalMismatchCount++;

                LogError(" ... metadata file JSON does not contain any entries where the DestinationTable is Files: " + metadataFileInfo.FullName);
                return;
            }

            // This dictionary tracks files on the local disk
            // Keys are relative file paths (Windows slashes); values are the SHA-1 hash values
            var filePathHashMap = new Dictionary<string, string>();

            foreach (var metadataFile in metadataFiles)
            {
                var sha1Hash = metadataFile.HashSum;
                var destinationDirectory = metadataFile.SubDir;
                var fileName = metadataFile.Name;
                var fileSizeBytes = int.TryParse(metadataFile.Size, out var size) ? size : 0;

                if (metadataObject.IgnoreFile(fileName, fileSizeBytes, false))
                {
                    continue;
                }

                // DestinationDirectory will likely be "data" or start with "data/"
                // For the reason why, see method CreatePacificaMetadataObject in Pacifica.Core.Upload

                string destDirectoryWindows;

                if (destinationDirectory.StartsWith("data/"))
                {
                    destDirectoryWindows = destinationDirectory.Substring(5).Replace('/', Path.DirectorySeparatorChar);
                }
                else if (destinationDirectory.Equals("data", StringComparison.OrdinalIgnoreCase))
                {
                    destDirectoryWindows = string.Empty;
                }
                else
                {
                    destDirectoryWindows = destinationDirectory.Replace('/', Path.DirectorySeparatorChar);
                }

                var relativeFilePathWindows = Path.Combine(destDirectoryWindows, fileName);

                filePathHashMap.Add(relativeFilePathWindows, sha1Hash);
            }

            CompareArchiveFilesToList(
                remoteFiles,
                out matchCount,
                out mismatchCount,
                filePathHashMap,
                transactionIdStats);

            transactionId = GetBestTransactionId(transactionIdStats);
        }

        private void CopyHashResultsFileToBackupFolder(string datasetInstrument, string datasetYearQuarter, FileInfo hashResultsFile)
        {
            // Copy the updated file to hashResultsFolderPathBackup
            try
            {
                var hashResultsFolderPathBackup = GetHashResultsFilePath(DEFAULT_HASH_RESULTS_BACKUP_FOLDER_PATH, mDataset, datasetInstrument, datasetYearQuarter);

                var backupHashResultsFile = new FileInfo(hashResultsFolderPathBackup);

                // Create the target folders if necessary
                if (backupHashResultsFile.Directory != null)
                {
                    if (!backupHashResultsFile.Directory.Exists)
                    {
                        backupHashResultsFile.Directory.Create();
                    }

                    // Copy the file
                    hashResultsFile.CopyTo(backupHashResultsFile.FullName, true);
                }
            }
            catch (Exception ex)
            {
                // Don't treat this as a fatal error
                LogError("Error copying SHA-1 hash results file from hash results folder to backup folder", ex);
            }
        }

        /// <summary>
        /// Create a new SHA-1 hash results file
        /// </summary>
        /// <param name="hashResultsFile"></param>
        /// <param name="archivedFiles"></param>
        /// <param name="datasetInstrument"></param>
        /// <param name="datasetYearQuarter"></param>
        /// <returns>True if successful, false if an error</returns>
        private static bool CreateHashResultsFile(
            FileInfo hashResultsFile,
            IEnumerable<MyEMSLReader.ArchivedFileInfo> archivedFiles,
            string datasetInstrument,
            string datasetYearQuarter)
        {
            try
            {
                var hashResults = new Dictionary<string, HashInfo>(StringComparer.OrdinalIgnoreCase);

                foreach (var archivedFile in archivedFiles)
                {
                    archivedFile.Instrument = datasetInstrument;
                    archivedFile.DatasetYearQuarter = datasetYearQuarter;

                    var archivedFilePath = "/myemsl/svc-dms/" + archivedFile.PathWithInstrumentAndDatasetUnix;

                    var hashInfo = new HashInfo
                    {
                        HashCode = archivedFile.Hash,
                        MyEMSLFileID = archivedFile.FileID.ToString(CultureInfo.InvariantCulture)
                    };

                    hashResults.Add(archivedFilePath, hashInfo);
                }

                if (hashResultsFile.Directory != null)
                {
                    // Create the target folders if necessary
                    if (!hashResultsFile.Directory.Exists)
                    {
                        hashResultsFile.Directory.Create();
                    }

                    return WriteHashResultsFile(hashResults, hashResultsFile.FullName, useTempFile: false);
                }

                LogError("Parent directory is null for " + hashResultsFile.FullName);
                return false;
            }
            catch (Exception ex)
            {
                LogError("Exception creating new SHA-1 hash results file in CreateHashResultsFile", ex);
                return false;
            }
        }

        /// <summary>
        /// Create or update the SHA-1 hash results file
        /// </summary>
        /// <param name="archivedFiles"></param>
        /// <returns>True if successful, false if an error</returns>
        private bool CreateOrUpdateHashResultsFile(IEnumerable<MyEMSLReader.ArchivedFileInfo> archivedFiles)
        {
            try
            {
                var datasetInstrument = mTaskParams.GetParam("Instrument_Name");

                if (!ParameterDefined("Instrument_Name", datasetInstrument))
                {
                    return false;
                }

                // Calculate the "year_quarter" code used for subdirectories within an instrument folder
                // This value is based on the date the dataset was created in DMS
                var datasetYearQuarter = DMSMetadataObject.GetDatasetYearQuarter(mTaskParams.TaskDictionary);

                var hashResultsFilePath = GetHashResultsFilePath(DEFAULT_HASH_RESULTS_FOLDER_PATH, mDataset, datasetInstrument, datasetYearQuarter);

                var hashResultsFile = new FileInfo(hashResultsFilePath);

                bool success;

                if (!hashResultsFile.Exists)
                {
                    // Target file doesn't exist; nothing to merge
                    success = CreateHashResultsFile(hashResultsFile, archivedFiles, datasetInstrument, datasetYearQuarter);
                }
                else
                {
                    // File exists; merge the new values with the existing data
                    success = UpdateHashResultsFile(hashResultsFilePath, archivedFiles, datasetInstrument, datasetYearQuarter);
                }

                if (success)
                {
                    CopyHashResultsFileToBackupFolder(datasetInstrument, datasetYearQuarter, hashResultsFile);
                }

                return success;
            }
            catch (Exception ex)
            {
                LogError("Exception in CreateHashResultsFile", ex);
                return false;
            }
        }

        private static void DeleteMetadataFile(string metadataFilePath)
        {
            try
            {
                var metadataFile = new FileInfo(metadataFilePath);
                var parentDirectory = metadataFile.Directory;

                // Delete the metadata file in the transfer folder
                if (metadataFile.Exists)
                {
                    metadataFile.Delete();
                }

                // Delete the transfer directory if it is empty
                if (parentDirectory?.GetFileSystemInfos("*", SearchOption.AllDirectories).Length == 0)
                {
                    parentDirectory.Delete();
                }
            }
            catch (Exception ex)
            {
                // Don't treat this as a fatal error
                LogError("Error deleting metadata file in transfer directory: " + ex.Message);
            }
        }

        /// <summary>
        /// Find the transactionId that has the highest value (i.e. the one used by the majority of the files)
        /// </summary>
        /// <param name="transactionIdStats">Dictionary where keys are transactionIds and values are the number of files that had the given transactionId</param>
        /// <returns>Transaction ID</returns>
        private static long GetBestTransactionId(Dictionary<long, int> transactionIdStats)
        {
            if (transactionIdStats.Count == 0)
            {
                return 0;
            }

            // Determine the best transactionId to associate with these files
            var bestTransactionId = (
                from item in transactionIdStats
                orderby item.Value descending
                select item.Key).First();

            return bestTransactionId;
        }

        private static string GetHashResultsFilePath(string hashResultsFolderPath, string datasetName, string instrumentName, string datasetYearQuarter)
        {
            return GetHashResultsFilePath(Path.Combine(hashResultsFolderPath, instrumentName, datasetYearQuarter), datasetName);
        }

        private static string GetHashResultsFilePath(string parentFolderPath, string datasetName)
        {
            return Path.Combine(parentFolderPath, HASH_RESULTS_FILE_PREFIX + datasetName);
        }

        private bool ParameterDefined(string parameterName, string parameterValue)
        {
            if (string.IsNullOrEmpty(parameterValue))
            {
                mRetData.CloseoutMsg = "Job parameters do not have " + parameterName + " defined; unable to continue";
                LogError(mRetData.CloseoutMsg);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Parse out the hash and file path from dataLine
        /// Updates hashResults (or adds a new entry)
        /// </summary>
        /// <param name="dataLine">Data line</param>
        /// <param name="hashResults">Dictionary where keys are Linux file paths and values are HashInfo, tracking the Hash value and MyEMSL File ID</param>
        /// <returns>True if hashResults is updated, false if unchanged</returns>
        private static void ParseAndStoreHashInfo(string dataLine, ref Dictionary<string, HashInfo> hashResults)
        {
            // Data Line Format
            //
            // Old data not in MyEMSL:
            //    MD5Hash<SPACE>ArchiveFilePath
            //
            // New data in MyEMSL:
            //    Sha1Hash<SPACE>MyEMSLFilePath<TAB>MyEMSLID
            //
            // The Hash and ArchiveFilePath are separated by a space because that's how Ryan Wright's script reported the results
            // The FilePath and MyEMSLID are separated by a tab in case the file path contains a space

            // Examples:
            //
            // Old data not in MyEMSL:
            //    0dcf9d677ac76519ae54c11cc5e10723 /archive/dmsarch/VOrbiETD04/2013_3/QC_Shew_13_04-100ng-3_HCD_19Aug13_Frodo_13-04-15/QC_Shew_13_04-100ng-3_HCD_19Aug13_Frodo_13-04-15.raw
            //    d47aca4d13d0a771900eef1fc7ee53ce /archive/dmsarch/VOrbiETD04/2013_3/QC_Shew_13_04-100ng-3_HCD_19Aug13_Frodo_13-04-15/QC/index.html
            //
            // New data in MyEMSL:
            //    796d99bcc6f1824dfe1c36cc9a61636dd1b07625 /myemsl/svc-dms/SW_TEST_LCQ/2006_1/SWT_LCQData_300/SIC201309041722_Auto976603/Default_2008-08-22.xml 915636
            //    70976fbd7088b27a711de4ce6309fbb3739d05f9 /myemsl/svc-dms/SW_TEST_LCQ/2006_1/SWT_LCQData_300/SIC201309041722_Auto976603/SWT_LCQData_300_TIC_Scan.tic   915648

            var hashInfo = new HashInfo();

            var values = dataLine.Split(new[] { ' ' }, 2).ToList();

            if (values.Count < 2)
            {
                // Line doesn't contain two strings separated by a space
                return;
            }

            hashInfo.HashCode = values[0];

            var pathInfo = values[1].Split('\t').ToList();

            var archiveFilePath = pathInfo[0];

            if (pathInfo.Count > 1)
            {
                hashInfo.MyEMSLFileID = pathInfo[1];
            }

            // Files should only be listed once in the SHA-1 hash results file
            // But, just in case there is a duplicate, we'll check for that
            // Results files could have duplicate entries if a file was copied to the archive via FTP and was stored via MyEMSL
            if (hashResults.TryGetValue(archiveFilePath, out var hashInfoCached))
            {
                if (hashInfo.IsMatch(hashInfoCached))
                {
                    // Values match; nothing to update
                    return;
                }

                // Preferentially use the newer value, unless the older value has a MyEMSL file ID but the newer one does not
                if (string.IsNullOrEmpty(hashInfo.MyEMSLFileID) && !string.IsNullOrEmpty(hashInfoCached.MyEMSLFileID))
                {
                    // Do not update the dictionary
                    return;
                }

                hashResults[archiveFilePath] = hashInfo;
                return;
            }

            // Append a new entry to the cached info
            hashResults.Add(archiveFilePath, hashInfo);
        }

        private static bool UpdateHashResultsFile(
            string hashResultsFilePath,
            IEnumerable<MyEMSLReader.ArchivedFileInfo> archivedFiles,
            string datasetInstrument,
            string datasetYearQuarter)
        {
            var hashResults = new Dictionary<string, HashInfo>(StringComparer.OrdinalIgnoreCase);

            // Read the file and cache the results in memory
            using (var resultsFileReader = new StreamReader(new FileStream(hashResultsFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
            {
                while (!resultsFileReader.EndOfStream)
                {
                    var dataLine = resultsFileReader.ReadLine();
                    ParseAndStoreHashInfo(dataLine, ref hashResults);
                }
            }

            var saveMergedFile = false;

            // Merge the results in archivedFiles with hashResults
            foreach (var archivedFile in archivedFiles)
            {
                if (string.IsNullOrEmpty(archivedFile.Instrument))
                {
                    archivedFile.Instrument = datasetInstrument;
                }

                if (string.IsNullOrEmpty(archivedFile.DatasetYearQuarter))
                {
                    archivedFile.DatasetYearQuarter = datasetYearQuarter;
                }

                var archivedFilePath = "/myemsl/svc-dms/" + archivedFile.PathWithInstrumentAndDatasetUnix;

                var hashInfo = new HashInfo
                {
                    HashCode = archivedFile.Hash,
                    MyEMSLFileID = archivedFile.FileID.ToString(CultureInfo.InvariantCulture)
                };

                if (hashResults.TryGetValue(archivedFilePath, out var hashInfoCached))
                {
                    if (!hashInfo.IsMatch(hashInfoCached))
                    {
                        hashResults[archivedFilePath] = hashInfo;
                        saveMergedFile = true;
                    }
                }
                else
                {
                    hashResults.Add(archivedFilePath, hashInfo);
                    saveMergedFile = true;
                }
            }

            if (!saveMergedFile)
            {
                return true;
            }

            return WriteHashResultsFile(hashResults, hashResultsFilePath, useTempFile: true);
        }

        private bool VisibleInMetadata(out string metadataFilePath, out long transactionId)
        {
            metadataFilePath = string.Empty;

            try
            {
                var reader = new MyEMSLReader.Reader
                {
                    IncludeAllRevisions = true,
                    ReportMetadataURLs = mTraceMode || mDebugLevel >= 5,
                    TraceMode = mTraceMode,
                    UseTestInstance = false
                };

                RegisterEvents(reader);

                if (!reader.CertificateFileExists(out var errorMessage))
                {
                    // MyEMSL certificate file not found in the current directory, at C:\DMS_Programs\client_certs, or at C:\client_certs\
                    mRetData.CloseoutMsg = errorMessage;
                    mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_NOT_READY;

                    LogError(mRetData.CloseoutMsg);
                    transactionId = 0;
                    return false;
                }

                var subDir = mTaskParams.GetParam("OutputDirectoryName", mTaskParams.GetParam("OutputFolderName"));

                // Find files tracked by MyEMSL for this dataset
                var remoteFiles = reader.FindFilesByDatasetID(mDatasetID, subDir);

                if (remoteFiles.Count == 0)
                {
                    if (mTotalMismatchCount == 0)
                    {
                        LogError("MyEmsl verification error for dataset " + mDataset + ", job " + mJob);
                    }

                    mTotalMismatchCount++;

                    mRetData.CloseoutMsg = string.Format(
                        "MyEMSL status lookup did not report any files for Dataset_ID {0}{1}",
                        mDatasetID,
                        string.IsNullOrEmpty(subDir) ? string.Empty : " and subdirectory " + subDir);

                    mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_NOT_READY;

                    LogWarning(" ... " + mRetData.CloseoutMsg);
                    transactionId = 0;
                    return false;
                }

                // When retrieving the list of files in MyEMSL we used DatasetID
                // If a dataset is renamed in DMS, multiple datasets could have the same DatasetID
                // Prior to June 2017, we made sure that the entries in archivedFiles only correspond to this dataset
                // Starting in June 2017, the results reported by files_for_keyvalue/omics.dms.dataset_id do not include dataset name
                // Thus, this validation cannot be performed.

                var compareSuccess = CompareArchiveFilesToExpectedFiles(remoteFiles, out metadataFilePath, out transactionId);

                if (!compareSuccess)
                {
                    return false;
                }

                var hashFileUpdateSuccess = CreateOrUpdateHashResultsFile(remoteFiles);

                if (!hashFileUpdateSuccess)
                {
                    mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_NOT_READY;
                }

                return hashFileUpdateSuccess;
            }
            catch (Exception ex)
            {
                LogError("Exception in VisibleInMetadata", ex);
                transactionId = 0;
                return false;
            }
        }

        private static bool WriteHashResultsFile(Dictionary<string, HashInfo> hashResults, string hashResultsFilePath, bool useTempFile)
        {
            var currentStep = "initializing";

            try
            {
                string targetFilePath;

                if (useTempFile)
                {
                    // Create a new Hash results file that we'll use to replace hashResultsFilePath
                    targetFilePath = hashResultsFilePath + ".new";
                }
                else
                {
                    targetFilePath = hashResultsFilePath;
                }

                currentStep = "creating " + targetFilePath;

                using (var hashResultsWriter = new StreamWriter(new FileStream(targetFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    foreach (var item in hashResults)
                    {
                        var dataLine = item.Value.HashCode + " " + item.Key;

                        if (!string.IsNullOrEmpty(item.Value.MyEMSLFileID))
                        {
                            dataLine += "\t" + item.Value.MyEMSLFileID;
                        }

                        hashResultsWriter.WriteLine(dataLine);
                    }
                }

                System.Threading.Thread.Sleep(50);

                if (useTempFile)
                {
                    currentStep = "overwriting master Hash results file with " + targetFilePath;
                    File.Copy(targetFilePath, hashResultsFilePath, true);
                    System.Threading.Thread.Sleep(100);

                    currentStep = "deleting " + targetFilePath;
                    File.Delete(targetFilePath);
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Exception in WriteHashResultsFile while " + currentStep, ex);
                return false;
            }
        }
    }
}
