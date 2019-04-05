using CaptureTaskManager;
using Jayrock.Json.Conversion;
using Pacifica.Core;
using Pacifica.DMS_Metadata;
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
    public class clsPluginMain : clsToolRunnerBase
    {

        #region "Constants and Enums"
        private const string HASH_RESULTS_FILE_PREFIX = "results.";
        private const string DEFAULT_HASH_RESULTS_FOLDER_PATH = @"\\proto-7\MD5Results";
        private const string DEFAULT_HASH_RESULTS_BACKUP_FOLDER_PATH = @"\\proto-5\MD5ResultsBackup";

        #endregion

        #region "Class-wide variables"
        clsToolReturnData mRetData = new clsToolReturnData();

        private int mTotalMismatchCount;

        #endregion

        #region "Methods"
        /// <summary>
        /// Runs the Archive Verify step tool
        /// </summary>
        /// <returns>Class with completionCode, completionMessage, evaluationCode, and evaluationMessage</returns>
        public override clsToolReturnData RunTool()
        {
            LogDebug("Starting ArchiveVerifyPlugin.clsPluginMain.RunTool");

            // Perform base class operations, if any
            mRetData = base.RunTool();
            if (mRetData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED)
                return mRetData;

            // Do not call StoreToolVersionInfo to store the version info in the database
            // Not required since the ArchiveVerify plugin uses components whose version was
            // already logged by the DatasetArchive plugin, and we don't need to make the
            // additional database call to SetStepTaskToolVersion

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
            }

            if (success)
            {
                // Everything was good
                LogMessage("MyEMSL verification successful for job " + mJob + ", dataset " + mDataset);
                mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;

                // Note that stored procedure SetStepTaskComplete will update MyEMSL State values if mRetData.EvalCode = 5
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

            LogDebug("Completed clsPluginMain.RunTool");

            return mRetData;

        }

        private bool CheckUploadStatus(out int statusNum, out byte ingestStepsCompleted, out bool fatalError)
        {

            // Examine the upload status
            // If not complete, this manager will return completionCode CLOSEOUT_NOT_READY=2
            // which will tell the DMS_Capture DB to reset the task to state 2 and bump up the Next_Try value by 30 minutes

            var statusURI = mTaskParams.GetParam("MyEMSL_Status_URI", string.Empty);

            var eusInstrumentID = mTaskParams.GetParam("EUS_InstrumentID", 0);
            var eusProposalID = mTaskParams.GetParam("EUS_ProposalID", string.Empty);
            var eusUploaderID = mTaskParams.GetParam("EUS_UploaderID", 0);

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

                mRetData.CloseoutMsg = "";

                var ingestSuccess = GetMyEMSLIngestStatus(
                    mJob, statusChecker, statusURI,
                    eusInstrumentID, eusProposalID, eusUploaderID,
                    mRetData, out _, out var currentTask, out var percentComplete);

                // Examine the current task and percent complete to determine the number of ingest steps completed
                ingestStepsCompleted = statusChecker.DetermineIngestStepsCompleted(currentTask, percentComplete, 0);

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
        /// <param name="archivedFiles"></param>
        /// <param name="metadataFilePath"></param>
        /// <param name="transactionId">The TransactionID used by the majority of the matching files</param>
        /// <returns>True if all of the files match, false if a mismatch or an error</returns>
        private bool CompareArchiveFilesToExpectedFiles(
            IReadOnlyCollection<MyEMSLReader.ArchivedFileInfo> archivedFiles,
            out string metadataFilePath,
            out long transactionId)
        {
            metadataFilePath = string.Empty;
            transactionId = 0;

            try
            {

                var transferFolderPathBase = mTaskParams.GetParam("TransferFolderPath", string.Empty);
                if (!ParameterDefined("TransferFolderPath", transferFolderPathBase))
                    return false;

                if (string.IsNullOrEmpty(mDataset))
                {
                    mRetData.CloseoutMsg = "mDataset is empty; unable to continue";
                    LogError(mRetData.CloseoutMsg);
                    return false;
                }

                var transferFolderPath = Path.Combine(transferFolderPathBase, mDataset);

                var jobNumber = mTaskParams.GetParam("Job", string.Empty);
                if (!ParameterDefined("Job", jobNumber))
                    return false;

                var fiMetadataFile = new FileInfo(Path.Combine(transferFolderPath, Utilities.GetMetadataFilenameForJob(jobNumber)));

                if (fiMetadataFile.Exists)
                {
                    metadataFilePath = fiMetadataFile.FullName;

                    CompareToMetadataFile(
                        archivedFiles,
                        fiMetadataFile,
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
                            LogError("MyEmsl verification errors for dataset " + mDataset + ", job " + mJob);

                        mTotalMismatchCount += mismatchCountToMetadata;

                        var matchStats = "Sha-1 mismatch between local files in metadata.txt file and MyEMSL; " +
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

                // Look for files that should have been uploaded, compute a Sha-1 hash for each, and compare those hashes to existing files in MyEMSL

                var ignoreMyEMSLFileTrackingError = mTaskParams.GetParam("IgnoreMyEMSLFileTrackingError", false);

                var config = new Configuration();

                ResetTimestampForQueueWaitTimeLogging();

                var metadataObject = new DMSMetadataObject(config, mMgrName, mJob, mFileTools)
                {
                    TraceMode = mTraceMode,
                    IgnoreMyEMSLFileTrackingError = ignoreMyEMSLFileTrackingError
                };

                // Attach the events
                RegisterEvents(metadataObject);

                var lstDatasetFilesLocal = metadataObject.FindDatasetFilesToArchive(
                    mTaskParams.TaskDictionary,
                    mMgrParams.MgrParams,
                    out _);

                if (lstDatasetFilesLocal.Count == 0)
                {
                    if (mTotalMismatchCount == 0)
                        LogError("MyEmsl verification errors for dataset " + mDataset + ", job " + mJob);

                    mTotalMismatchCount += 1;

                    mRetData.CloseoutMsg = "Local files were not found for this dataset; unable to compare Sha-1 hashes to MyEMSL values";
                    LogError(" ... " + mRetData.CloseoutMsg);
                    return false;
                }

                // Keys are relative file paths (Windows slashes); values are the Sha-1 hash values
                var dctFilePathHashMap = new Dictionary<string, string>();

                foreach (var datasetFile in lstDatasetFilesLocal)
                {
                    var relativeFilePathWindows = datasetFile.RelativeDestinationFullPath.Replace("/", @"\");
                    dctFilePathHashMap.Add(relativeFilePathWindows, datasetFile.Sha1HashHex);
                }

                var transactionIdStats = new Dictionary<long, int>();

                CompareArchiveFilesToList(
                    archivedFiles,
                    out var matchCountToDisk,
                    out var mismatchCountToDisk,
                    dctFilePathHashMap,
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
                        LogError("MyEmsl verification errors for dataset " + mDataset + ", job " + mJob);

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
        /// <param name="archivedFiles">Files in MyEMSL</param>
        /// <param name="matchCount"></param>
        /// <param name="mismatchCount"></param>
        /// <param name="dctFilePathHashMap">Local files; keys are relative file paths (Windows slashes); values are the Sha-1 hash values</param>
        /// <param name="transactionIdStats">Keys are transaction IDs, values are the number of files for each transaction ID</param>
        private void CompareArchiveFilesToList(
            IReadOnlyCollection<MyEMSLReader.ArchivedFileInfo> archivedFiles,
            out int matchCount,
            out int mismatchCount,
            IReadOnlyDictionary<string, string> dctFilePathHashMap,
            IDictionary<long, int> transactionIdStats)
        {
            matchCount = 0;
            mismatchCount = 0;

            // Make sure each of the files in dctFilePathHashMap is present in archivedFiles
            foreach (var metadataFile in dctFilePathHashMap)
            {
                var lstMatchingArchivedFiles = (from item in archivedFiles where item.RelativePathWindows == metadataFile.Key select item).ToList();

                if (lstMatchingArchivedFiles.Count == 0)
                {
                    if (mTotalMismatchCount == 0)
                        LogError("MyEmsl verification errors for dataset " + mDataset + ", job " + mJob);

                    mTotalMismatchCount += 1;

                    mismatchCount += 1;

                    var msg = " ... file " + metadataFile.Key + " not found in MyEMSL (CompareArchiveFilesToList)";
                    LogError(msg);
                }
                else
                {
                    var archiveFile = lstMatchingArchivedFiles.First();

                    if (archiveFile.Hash == metadataFile.Value)
                    {
                        matchCount++;

                        foreach (var archiveFileVersion in lstMatchingArchivedFiles)
                        {
                            if (transactionIdStats.TryGetValue(archiveFileVersion.TransactionID, out var fileCount))
                            {
                                transactionIdStats[archiveFileVersion.TransactionID] = fileCount + 1;

                            }
                            else
                            {
                                transactionIdStats.Add(archiveFileVersion.TransactionID, 1);
                            }
                        }
                    }
                    else
                    {
                        if (mTotalMismatchCount == 0)
                            LogError("MyEmsl verification errors for dataset " + mDataset + ", job " + mJob);

                        mTotalMismatchCount++;

                        var msg = " ... file mismatch for " + archiveFile.RelativePathWindows +
                                  "; MyEMSL reports " + archiveFile.Hash + " but expecting " + metadataFile.Value;

                        LogError(msg);

                        mismatchCount++;

                    }
                }
            }
        }

        /// <summary>
        /// Compare the files that MyEMSL is tracking for this dataset to the files in the metadata file
        /// </summary>
        /// <param name="archivedFiles">Files in MyEMSL</param>
        /// <param name="fiMetadataFile"></param>
        /// <param name="matchCount"></param>
        /// <param name="mismatchCount"></param>
        /// <param name="transactionId">The TransactionID used by the majority of the matching files</param>
        private void CompareToMetadataFile(
            IReadOnlyCollection<MyEMSLReader.ArchivedFileInfo> archivedFiles,
            FileSystemInfo fiMetadataFile,
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

            using (var srMetadataFile = new StreamReader(new FileStream(fiMetadataFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
            {
                metadataJson = srMetadataFile.ReadToEnd();
            }

            if (string.IsNullOrEmpty(metadataJson))
            {
                if (mTotalMismatchCount == 0)
                    LogError("MyEmsl verification errors for dataset " + mDataset + ", job " + mJob);

                mTotalMismatchCount += 1;

                LogError(" ... metadata file is empty: " + fiMetadataFile.FullName);
                return;
            }

            var jsa = (Jayrock.Json.JsonArray)JsonConvert.Import(metadataJson);

            // metadataInfo is a list of Dictionaries
            var metadataInfo = Utilities.JsonArrayToDictionaryList(jsa);

            // This dictionary tracks files that were previously pushed to MyEMSL
            var dctMetadataFiles = new List<Dictionary<string, object>>();

            foreach (var item in metadataInfo)
            {
                if (item.TryGetValue("destinationTable", out var destinationTable))
                {
                    var destinationTableName = destinationTable as string;
                    if (string.Equals(destinationTableName, "Files", StringComparison.OrdinalIgnoreCase))
                    {
                        dctMetadataFiles.Add(item);
                    }
                }
            }

            if (dctMetadataFiles.Count == 0)
            {
                if (mTotalMismatchCount == 0)
                    LogError("MyEmsl verification errors for dataset " + mDataset + ", job " + mJob);

                mTotalMismatchCount += 1;

                LogError(" ... metadata file JSON does not contain any entries where the DestinationTable is Files: " + fiMetadataFile.FullName);
                return;
            }

            // Get the list of files that we can ignore
            var filesToIgnore = DMSMetadataObject.GetFilesToIgnore();

            // This dictionary tracks files on the local disk
            // Keys are relative file paths (Windows slashes); values are the Sha-1 hash values
            var dctFilePathHashMap = new Dictionary<string, string>();

            foreach (var metadataFile in dctMetadataFiles)
            {
                var sha1Hash = Utilities.GetDictionaryValue(metadataFile, "hashsum");
                var destinationDirectory = Utilities.GetDictionaryValue(metadataFile, "subdir");
                var fileName = Utilities.GetDictionaryValue(metadataFile, "name");

                if (filesToIgnore.Contains(fileName))
                    continue;

                // DestinationDirectory will likely be "data" or start with "data/"
                // For the reason why, see method CreatePacificaMetadataObject in Pacifica.Core.Upload

                string destDirectoryWindows;

                if (destinationDirectory.StartsWith("data/"))
                    destDirectoryWindows = destinationDirectory.Substring(5).Replace('/', Path.DirectorySeparatorChar);
                else if (destinationDirectory.Equals("data", StringComparison.OrdinalIgnoreCase))
                    destDirectoryWindows = string.Empty;
                else
                    destDirectoryWindows = destinationDirectory.Replace('/', Path.DirectorySeparatorChar);

                var relativeFilePathWindows = Path.Combine(destDirectoryWindows, fileName);

                dctFilePathHashMap.Add(relativeFilePathWindows, sha1Hash);
            }

            CompareArchiveFilesToList(
                archivedFiles,
                out matchCount,
                out mismatchCount,
                dctFilePathHashMap,
                transactionIdStats);

            transactionId = GetBestTransactionId(transactionIdStats);
        }

        private void CopyHashResultsFileToBackupFolder(string datasetInstrument, string datasetYearQuarter, FileInfo hashResultsFile)
        {
            // Copy the updated file to hashResultsFolderPathBackup
            try
            {
                var hashResultsFolderPathBackup = GetHashResultsFilePath(DEFAULT_HASH_RESULTS_BACKUP_FOLDER_PATH, mDataset, datasetInstrument, datasetYearQuarter);

                var fiBackupHashResultsFile = new FileInfo(hashResultsFolderPathBackup);

                // Create the target folders if necessary
                if (fiBackupHashResultsFile.Directory != null)
                {
                    if (!fiBackupHashResultsFile.Directory.Exists)
                        fiBackupHashResultsFile.Directory.Create();

                    // Copy the file
                    hashResultsFile.CopyTo(fiBackupHashResultsFile.FullName, true);
                }
            }
            catch (Exception ex)
            {
                // Don't treat this as a fatal error
                LogError("Error copying Sha-1 hash results file from hash results folder to backup folder", ex);
            }
        }

        /// <summary>
        /// Create a new Sha-1 hash results file
        /// </summary>
        /// <param name="hashResultsFile"></param>
        /// <param name="archivedFiles"></param>
        /// <param name="datasetInstrument"></param>
        /// <param name="datasetYearQuarter"></param>
        /// <returns></returns>
        private bool CreateHashResultsFile(
            FileInfo hashResultsFile,
            IEnumerable<MyEMSLReader.ArchivedFileInfo> archivedFiles,
            string datasetInstrument,
            string datasetYearQuarter)
        {
            bool success;

            try
            {

                var lstHashResults = new Dictionary<string, clsHashInfo>(StringComparer.CurrentCultureIgnoreCase);

                foreach (var archivedFile in archivedFiles)
                {
                    archivedFile.Instrument = datasetInstrument;
                    archivedFile.DatasetYearQuarter = datasetYearQuarter;

                    var archivedFilePath = "/myemsl/svc-dms/" + archivedFile.PathWithInstrumentAndDatasetUnix;

                    var hashInfo = new clsHashInfo
                    {
                        HashCode = archivedFile.Hash,
                        MyEMSLFileID = archivedFile.FileID.ToString(CultureInfo.InvariantCulture)
                    };

                    lstHashResults.Add(archivedFilePath, hashInfo);
                }

                if (hashResultsFile.Directory != null)
                {
                    // Create the target folders if necessary
                    if (!hashResultsFile.Directory.Exists)
                        hashResultsFile.Directory.Create();

                    success = WriteHashResultsFile(lstHashResults, hashResultsFile.FullName, useTempFile: false);
                }
                else
                {
                    LogError("Parent directory is null for " + hashResultsFile.FullName);
                    success = false;
                }
            }
            catch (Exception ex)
            {
                LogError("Exception creating new Sha-1 hash results file in CreateHashResultsFile", ex);
                return false;
            }

            return success;

        }

        /// <summary>
        /// Create or update the Sha-1 hash results file
        /// </summary>
        /// <param name="archivedFiles"></param>
        /// <returns></returns>
        private bool CreateOrUpdateHashResultsFile(IEnumerable<MyEMSLReader.ArchivedFileInfo> archivedFiles)
        {

            bool success;

            try
            {

                var datasetInstrument = mTaskParams.GetParam("Instrument_Name");
                if (!ParameterDefined("Instrument_Name", datasetInstrument))
                    return false;

                // Calculate the "year_quarter" code used for subfolders within an instrument folder
                // This value is based on the date the dataset was created in DMS
                var datasetYearQuarter = DMSMetadataObject.GetDatasetYearQuarter(mTaskParams.TaskDictionary);

                var hashResultsFilePath = GetHashResultsFilePath(DEFAULT_HASH_RESULTS_FOLDER_PATH, mDataset, datasetInstrument, datasetYearQuarter);

                var hashResultsFile = new FileInfo(hashResultsFilePath);

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

            }
            catch (Exception ex)
            {
                LogError("Exception in CreateHashResultsFile", ex);
                return false;
            }

            return success;

        }

        private void DeleteMetadataFile(string metadataFilePath)
        {
            try
            {
                var fiMetadataFile = new FileInfo(metadataFilePath);
                var diParentFolder = fiMetadataFile.Directory;

                // Delete the metadata file in the transfer folder
                if (fiMetadataFile.Exists)
                    fiMetadataFile.Delete();

                // Delete the transfer folder if it is empty
                if (diParentFolder != null && diParentFolder.GetFileSystemInfos("*", SearchOption.AllDirectories).Length == 0)
                    diParentFolder.Delete();

            }
            catch (Exception ex)
            {
                // Don't treat this as a fatal error
                LogError("Error deleting metadata file in transfer folder: " + ex.Message);
            }
        }

        /// <summary>
        /// Find the transactionId that has the highest value (i.e. the one used by the majority of the files)
        /// </summary>
        /// <param name="transactionIdStats">Dictionary where keys are transactionIds and values are the number of files that had the given transactionId</param>
        /// <returns></returns>
        private long GetBestTransactionId(Dictionary<long, int> transactionIdStats)
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

        private string GetHashResultsFilePath(string hashResultsFolderPath, string datasetName, string instrumentName, string datasetYearQuarter)
        {
            return GetHashResultsFilePath(Path.Combine(hashResultsFolderPath, instrumentName, datasetYearQuarter), datasetName);
        }

        private string GetHashResultsFilePath(string parentFolderPath, string datasetName)
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
        /// Updates lstHashResults (or adds a new entry)
        /// </summary>
        /// <param name="dataLine"></param>
        /// <param name="lstHashResults">Dictionary where keys are unix file paths and values are clsHashInfo, tracking the Hash value and MyEMSL File ID</param>
        /// <returns>True if lstHashResults is updated, false if unchanged</returns>
        /// <remarks></remarks>
        private void ParseAndStoreHashInfo(string dataLine, ref Dictionary<string, clsHashInfo> lstHashResults)
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

            var hashInfo = new clsHashInfo();

            var lstValues = dataLine.Split(new[] { ' ' }, 2).ToList();

            if (lstValues.Count < 2)
            {
                // Line doesn't contain two strings separated by a space
                return;
            }

            hashInfo.HashCode = lstValues[0];

            var lstPathInfo = lstValues[1].Split('\t').ToList();

            var archiveFilePath = lstPathInfo[0];

            if (lstPathInfo.Count > 1)
                hashInfo.MyEMSLFileID = lstPathInfo[1];

            // Files should only be listed once in the Sha-1 hash results file
            // But, just in case there is a duplicate, we'll check for that
            // Results files could have duplicate entries if a file was copied to the archive via FTP and was stored via MyEMSL
            if (lstHashResults.TryGetValue(archiveFilePath, out var hashInfoCached))
            {
                if (hashInfo.IsMatch(hashInfoCached))
                {
                    // Values match; nothing to update
                    return;
                }

                // Preferentially use the newer value, unless the older value has a MyEMSL file ID but the newer one does not
                if (string.IsNullOrEmpty(hashInfo.MyEMSLFileID) && !string.IsNullOrEmpty(hashInfoCached.MyEMSLFileID))
                    // Do not update the dictionary
                    return;

                lstHashResults[archiveFilePath] = hashInfo;
                return;
            }

            // Append a new entry to the cached info
            lstHashResults.Add(archiveFilePath, hashInfo);

        }

        private bool UpdateHashResultsFile(
            string hashResultsFilePath,
            IEnumerable<MyEMSLReader.ArchivedFileInfo> archivedFiles,
            string datasetInstrument,
            string datasetYearQuarter)
        {
            bool success;

            var lstHashResults = new Dictionary<string, clsHashInfo>(StringComparer.CurrentCultureIgnoreCase);

            // Read the file and cache the results in memory
            using (var srHashResultsFile = new StreamReader(new FileStream(hashResultsFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
            {
                while (!srHashResultsFile.EndOfStream)
                {
                    var dataLine = srHashResultsFile.ReadLine();
                    ParseAndStoreHashInfo(dataLine, ref lstHashResults);
                }
            }

            var saveMergedFile = false;

            // Merge the results in archivedFiles with lstHashResults
            foreach (var archivedFile in archivedFiles)
            {
                if (string.IsNullOrEmpty(archivedFile.Instrument))
                    archivedFile.Instrument = datasetInstrument;

                if (string.IsNullOrEmpty(archivedFile.DatasetYearQuarter))
                    archivedFile.DatasetYearQuarter = datasetYearQuarter;

                var archivedFilePath = "/myemsl/svc-dms/" + archivedFile.PathWithInstrumentAndDatasetUnix;

                var hashInfo = new clsHashInfo
                {
                    HashCode = archivedFile.Hash,
                    MyEMSLFileID = archivedFile.FileID.ToString(CultureInfo.InvariantCulture)
                };

                if (lstHashResults.TryGetValue(archivedFilePath, out var hashInfoCached))
                {
                    if (!hashInfo.IsMatch(hashInfoCached))
                    {
                        lstHashResults[archivedFilePath] = hashInfo;
                        saveMergedFile = true;
                    }
                }
                else
                {
                    lstHashResults.Add(archivedFilePath, hashInfo);
                    saveMergedFile = true;
                }

            }

            if (saveMergedFile)
            {
                success = WriteHashResultsFile(lstHashResults, hashResultsFilePath, useTempFile: true);
            }
            else
            {
                success = true;
            }

            return success;
        }

        private bool VisibleInMetadata(out string metadataFilePath, out long transactionId)
        {
            metadataFilePath = string.Empty;

            try
            {
                var reader = new MyEMSLReader.Reader
                {
                    IncludeAllRevisions = false,
                    ReportMetadataURLs = mTraceMode || mDebugLevel >= 5,
                    TraceMode = mTraceMode,
                    UseTestInstance = false,
                };

                RegisterEvents(reader);

                if (!reader.CertificateFileExists(out var errorMessage))
                {
                    // MyEMSL certificate file not found in the current directory or at C:\client_certs\
                    mRetData.CloseoutMsg = errorMessage;
                    mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_NOT_READY;

                    LogError(mRetData.CloseoutMsg);
                    transactionId = 0;
                    return false;
                }

                var subDir = mTaskParams.GetParam("OutputFolderName", string.Empty);

                // Find files tracked by MyEMSL for this dataset
                var remoteFiles = reader.FindFilesByDatasetID(mDatasetID, subDir);
                if (remoteFiles.Count == 0)
                {
                    if (mTotalMismatchCount == 0)
                        LogError("MyEmsl verification error for dataset " + mDataset + ", job " + mJob);

                    mTotalMismatchCount += 1;

                    var msg = "MyEMSL status lookup did not report any files for Dataset_ID " + mDatasetID;
                    if (!string.IsNullOrEmpty(subDir))
                        msg += " and subdirectory " + subDir;

                    mRetData.CloseoutMsg = msg;
                    mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_NOT_READY;

                    LogWarning(" ... " + msg);
                    transactionId = 0;
                    return false;
                }

                // Make sure the entries in archivedFiles only correspond to this dataset
                // We performed the search using DatasetID, but if a dataset is renamed in DMS, multiple datasets could have the same DatasetID
                // Dataset renames are rare, but do happen (e.g. Dataset ID 382287 renamed from TB_UR_07_14Jul14_Methow_13-10-13 to TB_UR_08_14Jul14_Methow_13-10-14)

                // Unfortunately, starting in June 2017, the results reported by
                // files_for_keyvalue/omics.dms.dataset_id do not include dataset name
                // Thus, this validation cannot be performed.

                var filteredFiles = new List<MyEMSLReader.ArchivedFileInfo>();

                foreach (var archiveFile in remoteFiles)
                {
                    filteredFiles.Add(archiveFile);

                    //if (string.Equals(fileVersion.Dataset, mDataset, StringComparison.OrdinalIgnoreCase))
                    //{
                    //    filteredFiles.Add(fileVersion);
                    //}
                    //else
                    //{
                    //    LogMessage(
                    //        "Query for dataset ID " + mDatasetID + " yielded match to " + fileVersion.PathWithDataset +
                    //        " - skipping since wrong dataset", true);
                    //}
                }

                var compareSuccess = CompareArchiveFilesToExpectedFiles(filteredFiles, out metadataFilePath, out transactionId);

                if (!compareSuccess)
                    return false;

                var hashFileUpdateSuccess = CreateOrUpdateHashResultsFile(filteredFiles);

                if (!hashFileUpdateSuccess)
                    mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_NOT_READY;

                return hashFileUpdateSuccess;

            }
            catch (Exception ex)
            {
                LogError("Exception in VisibleInMetadata", ex);
                transactionId = 0;
                return false;
            }

        }

        private bool WriteHashResultsFile(Dictionary<string, clsHashInfo> lstHashResults, string hashResultsFilePath, bool useTempFile)
        {
            var currentStep = "initializing";

            try
            {
                string targetFilePath;
                if (useTempFile)
                {
                    // Create a new Hash results file that we'll use to replace strHashResultsFileMaster
                    targetFilePath = hashResultsFilePath + ".new";
                }
                else
                {
                    targetFilePath = hashResultsFilePath;
                }

                currentStep = "creating " + targetFilePath;

                using (var swHashResultsFile = new StreamWriter(new FileStream(targetFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    foreach (var item in lstHashResults)
                    {
                        var dataLine = item.Value.HashCode + " " + item.Key;
                        if (!string.IsNullOrEmpty(item.Value.MyEMSLFileID))
                            dataLine += "\t" + item.Value.MyEMSLFileID;

                        swHashResultsFile.WriteLine(dataLine);
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

        #endregion

    }

}
