using System;
using System.Globalization;
using CaptureTaskManager;
using Pacifica.Core;
using Pacifica.DMS_Metadata;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.IO;

namespace ArchiveVerifyPlugin
{
    public class clsPluginMain : clsToolRunnerBase
    {
        //*********************************************************************************************************
        // Main class for plugin
        //**********************************************************************************************************

        #region "Constants and Enums"
        private const string MD5_RESULTS_FILE_PREFIX = "results.";
        private const string DEFAULT_MD5_RESULTS_FOLDER_PATH = @"\\proto-7\MD5Results";
        private const string DEFAULT_MD5_RESULTS_BACKUP_FOLDER_PATH = @"\\proto-5\MD5ResultsBackup";

        #endregion

        #region "Class-wide variables"
        clsToolReturnData mRetData = new clsToolReturnData();

        private double mPercentComplete;
        private DateTime mLastProgressUpdateTime = DateTime.UtcNow;

        private int mTotalMismatchCount;

        #endregion

        #region "Methods"
        /// <summary>
        /// Runs the Archive Verify step tool
        /// </summary>
        /// <returns>Class with completionCode, completionMessage, evaluationCode, and evaluationMessage</returns>
        public override clsToolReturnData RunTool()
        {
            var msg = "Starting ArchiveVerifyPlugin.clsPluginMain.RunTool()";
            LogDebug(msg);

            // Perform base class operations, if any
            mRetData = base.RunTool();
            if (mRetData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED)
                return mRetData;

            var writeToLog = m_DebugLevel >= 5;
            LogDebug("Verifying files in MyEMSL for dataset '" + m_Dataset + "'", writeToLog);

            // Set this to Success for now
            mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
            var success = false;

            mTotalMismatchCount = 0;

            try
            {
                // Examine the MyEMSL ingest status page
                success = CheckUploadStatus();

            }
            catch (Exception ex)
            {
                mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;       // Possibly instead use CLOSEOUT_NOT_READY
                mRetData.CloseoutMsg = "Exception checking archive status (ArchiveVerifyPlugin): " + ex.Message;
                msg = "Exception checking archive status for job " + m_Job;
                LogError(msg, ex);
            }


            if (success)
            {

                // Confirm that the files are visible in elastic search
                // If data is found, then CreateOrUpdateMD5ResultsFile will also be called
                success = VisibleInElasticSearch(out var metadataFilePath);

                if (!success)
                {
                    mRetData.CloseoutMsg = "Not visible in elastic search";
                }
                else if (!string.IsNullOrEmpty(metadataFilePath))
                {
                    DeleteMetadataFile(metadataFilePath);
                }
            }

            if (success)
            {
                // Everything was good
                LogMessage("MyEMSL verification successful for job " + m_Job + ", dataset " + m_Dataset);
                mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;

                // Note that stored procedure SetStepTaskComplete will update MyEMSL State values if mRetData.EvalCode = 5
                mRetData.EvalCode = EnumEvalCode.EVAL_CODE_VERIFIED_IN_MYEMSL;

            }
            else
            {
                // There was a problem (or the data is not yet ready in MyEMSL)
                if (mRetData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS)
                {
                    mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_NOT_READY;
                    if (string.IsNullOrWhiteSpace(mRetData.CloseoutMsg))
                    {
                        mRetData.CloseoutMsg = "Unknown reason";
                    }
                }
            }

            msg = "Completed clsPluginMain.RunTool()";
            LogDebug(msg);

            return mRetData;

        }

        private bool CheckUploadStatus()
        {

            // Examine the upload status
            // If not complete, this manager will return completionCode CLOSEOUT_NOT_READY=2
            // which will tell the DMS_Capture DB to reset the task to state 2 and bump up the Next_Try value by 30 minutes

            var statusURI = m_TaskParams.GetParam("MyEMSL_Status_URI", string.Empty);

            var eusInstrumentID = m_TaskParams.GetParam("EUS_InstrumentID", 0);
            var eusProposalID = m_TaskParams.GetParam("EUS_ProposalID", string.Empty);
            var eusUploaderID = m_TaskParams.GetParam("EUS_UploaderID", 0);

            if (string.IsNullOrEmpty(statusURI))
            {
                const string msg = "MyEMSL_Status_URI is empty; cannot verify upload status";
                mRetData.CloseoutMsg = msg;
                mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                LogError(msg);
                return false;
            }

            var statusChecker = new MyEMSLStatusCheck();
            statusChecker.ErrorEvent += statusChecker_ErrorEvent;

            try
            {

                var ingestSuccess = GetMyEMSLIngestStatus(
                    m_Job, statusChecker, statusURI,
                    eusInstrumentID, eusProposalID, eusUploaderID,
                    mRetData, out var serverResponse);

                // ToDo: var ingestStepsCompleted = statusChecker.IngestStepCompletionCount(serverResponse);
                byte ingestStepsCompleted = 0;


                var fatalError = (
                    mRetData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED &&
                    mRetData.EvalCode == EnumEvalCode.EVAL_CODE_FAILURE_DO_NOT_RETRY);

                var statusNum = MyEMSLStatusCheck.GetStatusNumFromURI(statusURI);

                var transactionId = 0;
                if (!fatalError)
                {
                    // ToDo: transactionId = statusChecker.IngestStepTransactionId(serverResponse);
                }

                UpdateIngestStepsCompletedOneTask(statusNum, ingestStepsCompleted, transactionId, fatalError);

                // ToDo:
                //var success = statusChecker.IngestStepCompleted(
                //    serverResponse,
                //    MyEMSLStatusCheck.StatusStep.Available,
                //    out var statusMessage,
                //    out var errorMessage);

                var statusMessage = "Not implemented";

                mRetData.CloseoutMsg = statusMessage;

                // Logout below, then return false
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

            return false;
        }

        /// <summary>
        /// Compare the files in archivedFiles to the files in the metadata.txt file
        /// If metadata.txt file is missing, then compare to files actually on disk
        /// </summary>
        /// <param name="archivedFiles"></param>
        /// <param name="metadataFilePath"></param>
        /// <returns></returns>
        private bool CompareArchiveFilesToExpectedFiles(IReadOnlyCollection<MyEMSLFileInfo> archivedFiles, out string metadataFilePath)
        {
            metadataFilePath = string.Empty;

            try
            {

                var transferFolderPathBase = m_TaskParams.GetParam("TransferFolderPath", string.Empty);
                if (!ParameterDefined("TransferFolderPath", transferFolderPathBase))
                    return false;

                if (string.IsNullOrEmpty(m_Dataset))
                {
                    mRetData.CloseoutMsg = "m_Dataset is empty; unable to continue";
                    LogError(mRetData.CloseoutMsg);
                    return false;
                }

                var transferFolderPath = Path.Combine(transferFolderPathBase, m_Dataset);

                var jobNumber = m_TaskParams.GetParam("Job", string.Empty);
                if (!ParameterDefined("Job", jobNumber))
                    return false;

                var fiMetadataFile = new FileInfo(Path.Combine(transferFolderPath, Utilities.GetMetadataFilenameForJob(jobNumber)));

                if (fiMetadataFile.Exists)
                {
                    metadataFilePath = fiMetadataFile.FullName;

                    CompareToMetadataFile(archivedFiles, fiMetadataFile, out var matchCountToMetadata, out var mismatchCountToMetadata);

                    if (matchCountToMetadata > 0 && mismatchCountToMetadata == 0)
                    {
                        // Everything matches up
                        return true;
                    }

                    if (mismatchCountToMetadata > 0)
                    {
                        if (mTotalMismatchCount == 0)
                            LogError("MyEmsl verification errors for dataset " + m_Dataset + ", job " + m_Job);
                        mTotalMismatchCount += mismatchCountToMetadata;

                        var matchStats = "MatchCount=" + matchCountToMetadata + ", MismatchCount=" + mismatchCountToMetadata;
                        LogError(" ... sha1 mismatch between local files in metadata.txt file and MyEMSL; " + matchStats);
                        mRetData.CloseoutMsg = matchStats;
                        return false;
                    }
                }

                // Metadata file was missing or empty; compare to local files on disk

                // Look for files that should have been uploaded, compute a Sha-1 hash for each, and compare those hashes to existing files in MyEMSL

                var metadataObject = new DMSMetadataObject(m_MgrName);

                // Attach the events
                metadataObject.DebugEvent += DMSMetadataObject_DebugEvent;
                metadataObject.ErrorEvent += DMSMetadataObject_ErrorEvent;

                var lstDatasetFilesLocal = metadataObject.FindDatasetFilesToArchive(
                    m_TaskParams.TaskDictionary,
                    m_MgrParams.TaskDictionary,
                    out var uploadMetadata);

                if (lstDatasetFilesLocal.Count == 0)
                {
                    if (mTotalMismatchCount == 0)
                        LogError("MyEmsl verification errors for dataset " + m_Dataset + ", job " + m_Job);
                    mTotalMismatchCount += 1;

                    mRetData.CloseoutMsg = "Local files were not found for this dataset; unable to compare Sha-1 hashes to MyEMSL values";
                    LogError(" ... " + mRetData.CloseoutMsg);
                    return false;
                }

                // Keys are relative file paths (Windows slashes)
                // Values are the sha-1 hash values
                var dctFilePathHashMap = new Dictionary<string, string>();

                foreach (var datasetFile in lstDatasetFilesLocal)
                {
                    var relativeFilePathWindows = datasetFile.RelativeDestinationFullPath.Replace("/", @"\");
                    dctFilePathHashMap.Add(relativeFilePathWindows, datasetFile.Sha1HashHex);
                }

                CompareArchiveFilesToList(archivedFiles, out var matchCountToDisk, out var mismatchCountToDisk, dctFilePathHashMap);

                if (matchCountToDisk > 0 && mismatchCountToDisk == 0)
                {
                    // Everything matches up
                    return true;
                }

                if (mismatchCountToDisk > 0)
                {
                    if (mTotalMismatchCount == 0)
                        LogError("MyEmsl verification errors for dataset " + m_Dataset + ", job " + m_Job);
                    mTotalMismatchCount += mismatchCountToDisk;

                    mRetData.CloseoutMsg = "SHA-1 mismatch between local files on disk and MyEMSL; MatchCount=" + matchCountToDisk + ", MismatchCount=" + mismatchCountToDisk;
                    LogError(" ... " + mRetData.CloseoutMsg);

                    return false;
                }

                return false;

            }
            catch (Exception ex)
            {
                LogError("Exception in CompareArchiveFilesToExpectedFiles", ex);
                return false;
            }

        }

        private void CompareArchiveFilesToList(
            IReadOnlyCollection<MyEMSLFileInfo> archivedFiles,
            out int matchCount,
            out int mismatchCount,
            Dictionary<string, string> dctFilePathHashMap)
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
                        LogError("MyEmsl verification errors for dataset " + m_Dataset + ", job " + m_Job);
                    mTotalMismatchCount += 1;

                    var msg = " ... file " + metadataFile.Key + " not found in MyEMSL (CompareArchiveFilesToList)";
                    LogError(msg);
                }
                else
                {
                    var archiveFile = lstMatchingArchivedFiles.First();

                    if (archiveFile.HashSum == metadataFile.Value)
                        matchCount++;
                    else
                    {
                        if (mTotalMismatchCount == 0)
                            LogError("MyEmsl verification errors for dataset " + m_Dataset + ", job " + m_Job);
                        mTotalMismatchCount++;

                        var msg = " ... file mismatch for " + archiveFile.RelativePathWindows +
                            "; MyEMSL reports " + archiveFile.HashSum + " but expecting " + metadataFile.Value;

                        LogError(msg);

                        mismatchCount++;

                    }
                }
            }
        }

        private void CompareToMetadataFile(
            IReadOnlyCollection<MyEMSLFileInfo> archivedFiles,
            FileInfo fiMetadataFile,
            out int matchCount,
            out int mismatchCount)
        {
            matchCount = 0;
            mismatchCount = 0;

            // Parse the contents of the file
            string contents;

            using (var srMetadataFile = new StreamReader(new FileStream(fiMetadataFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
            {
                contents = srMetadataFile.ReadToEnd();
            }

            if (string.IsNullOrEmpty(contents))
            {
                if (mTotalMismatchCount == 0)
                    LogError("MyEmsl verification errors for dataset " + m_Dataset + ", job " + m_Job);
                mTotalMismatchCount += 1;

                LogError(" ... metadata file is empty: " + fiMetadataFile.FullName);
                return;
            }

            var dctMetadataInfo = Utilities.JsonToObject(contents);
            if (!dctMetadataInfo.ContainsKey("file"))
            {
                if (mTotalMismatchCount == 0)
                    LogError("MyEmsl verification errors for dataset " + m_Dataset + ", job " + m_Job);
                mTotalMismatchCount += 1;

                LogError(" ... metadata file JSON does not contain 'file': " + fiMetadataFile.FullName);
                return;
            }

            var dctMetadataFiles = (List<Dictionary<string, object>>)dctMetadataInfo["file"];

            // Keys are relative file paths (Windows slashes)
            // Values are the sha-1 hash values
            var dctFilePathHashMap = new Dictionary<string, string>();

            foreach (var metadataFile in dctMetadataFiles)
            {
                var sha1Hash = Utilities.GetDictionaryValue(metadataFile, "sha1Hash");
                var destinationDirectory = Utilities.GetDictionaryValue(metadataFile, "destinationDirectory");
                var fileName = Utilities.GetDictionaryValue(metadataFile, "fileName");

                var relativeFilePathWindows = Path.Combine(destinationDirectory.Replace('/', Path.DirectorySeparatorChar), fileName);

                dctFilePathHashMap.Add(relativeFilePathWindows, sha1Hash);
            }

            CompareArchiveFilesToList(archivedFiles, out matchCount, out mismatchCount, dctFilePathHashMap);

        }

        private void CopyMD5ResultsFileToBackupFolder(string datasetInstrument, string datasetYearQuarter, FileInfo fiMD5ResultsFile)
        {
            // Copy the updated file to md5ResultsFolderPathBackup
            try
            {
                var strMD5ResultsFileBackup = GetMD5ResultsFilePath(DEFAULT_MD5_RESULTS_BACKUP_FOLDER_PATH, m_Dataset, datasetInstrument, datasetYearQuarter);

                var fiBackupMD5ResultsFile = new FileInfo(strMD5ResultsFileBackup);

                // Create the target folders if necessary
                if (fiBackupMD5ResultsFile.Directory != null)
                {
                    if (!fiBackupMD5ResultsFile.Directory.Exists)
                        fiBackupMD5ResultsFile.Directory.Create();

                    // Copy the file
                    fiMD5ResultsFile.CopyTo(fiBackupMD5ResultsFile.FullName, true);
                }
            }
            catch (Exception ex)
            {
                // Don't treat this as a fatal error
                LogError("Error copying MD5 results file from MD5 results folder to backup folder", ex);
            }
        }

        private bool CreateMD5ResultsFile(FileInfo fiMD5ResultsFile, IEnumerable<MyEMSLFileInfo> archivedFiles)
        {
            // Create a new MD5 results file
            bool success;

            var datasetInstrument = m_TaskParams.GetParam("Instrument_Name");
            if (!ParameterDefined("Instrument_Name", datasetInstrument))
                return false;

            // Calculate the "year_quarter" code used for subfolders within an instrument folder
            // This value is based on the date the dataset was created in DMS
            var datasetYearQuarter = DMSMetadataObject.GetDatasetYearQuarter(m_TaskParams.TaskDictionary);

            try
            {
                // Create a new MD5 results file
                var lstMD5Results = new Dictionary<string, clsHashInfo>(StringComparer.CurrentCultureIgnoreCase);

                foreach (var archivedFile in archivedFiles)
                {
                    archivedFile.Instrument = datasetInstrument;
                    archivedFile.DatasetYearQuarter = datasetYearQuarter;

                    var archivedFilePath = "/myemsl/svc-dms/" + archivedFile.PathWithInstrumentAndDatasetUnix;

                    var hashInfo = new clsHashInfo
                    {
                        HashCode = archivedFile.HashSum,
                        MyEMSLFileID = archivedFile.FileID.ToString(CultureInfo.InvariantCulture)
                    };

                    lstMD5Results.Add(archivedFilePath, hashInfo);
                }

                if (fiMD5ResultsFile.Directory != null)
                {
                    // Create the target folders if necessary
                    if (!fiMD5ResultsFile.Directory.Exists)
                        fiMD5ResultsFile.Directory.Create();

                    success = WriteMD5ResultsFile(lstMD5Results, fiMD5ResultsFile.FullName, useTempFile: false);
                }
                else
                {
                    LogError("Parent directory is null for " + fiMD5ResultsFile.FullName);
                    success = false;
                }
            }
            catch (Exception ex)
            {
                LogError("Exception creating new MD5 results file in CreateMD5ResultsFile", ex);
                return false;
            }

            return success;

        }

        /// <summary>
        /// Create or update the MD5 results file
        /// </summary>
        /// <param name="archivedFiles"></param>
        /// <returns></returns>
        private bool CreateOrUpdateMD5ResultsFile(IEnumerable<MyEMSLFileInfo> archivedFiles)
        {

            bool success;

            try
            {

                var datasetInstrument = m_TaskParams.GetParam("Instrument_Name", string.Empty);
                if (!ParameterDefined("Instrument_Name", datasetInstrument))
                    return false;

                // Calculate the "year_quarter" code used for subfolders within an instrument folder
                // This value is based on the date the dataset was created in DMS
                var datasetYearQuarter = DMSMetadataObject.GetDatasetYearQuarter(m_TaskParams.TaskDictionary);

                var md5ResultsFilePath = GetMD5ResultsFilePath(DEFAULT_MD5_RESULTS_FOLDER_PATH, m_Dataset, datasetInstrument, datasetYearQuarter);

                var fiMD5ResultsFile = new FileInfo(md5ResultsFilePath);

                if (!fiMD5ResultsFile.Exists)
                {
                    // Target file doesn't exist; nothing to merge
                    success = CreateMD5ResultsFile(fiMD5ResultsFile, archivedFiles);
                }
                else
                {
                    // File exists; merge the new values with the existing data
                    success = UpdateMD5ResultsFile(md5ResultsFilePath, archivedFiles);
                }

                if (success)
                {
                    CopyMD5ResultsFileToBackupFolder(datasetInstrument, datasetYearQuarter, fiMD5ResultsFile);
                }

            }
            catch (Exception ex)
            {
                LogError("Exception in CreateMD5ResultsFile", ex);
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

        private string GetMD5ResultsFilePath(string strMD5ResultsFolderPath, string strDatasetName, string strInstrumentName, string strDatasetYearQuarter)
        {
            return GetMD5ResultsFilePath(Path.Combine(strMD5ResultsFolderPath, strInstrumentName, strDatasetYearQuarter), strDatasetName);
        }

        private string GetMD5ResultsFilePath(string strParentFolderPath, string strDatasetName)
        {
            return Path.Combine(strParentFolderPath, MD5_RESULTS_FILE_PREFIX + strDatasetName);
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
        /// Updates lstMD5Results (or adds a new entry)
        /// </summary>
        /// <param name="dataLine"></param>
        /// <param name="lstMD5Results">Dictionary where keys are unix file paths and values are clsHashInfo, tracking the Hash value and MyEMSL File ID</param>
        /// <returns>True if lstMD5Results is updated, false if unchanged</returns>
        /// <remarks></remarks>
        private bool ParseAndStoreHashInfo(string dataLine, ref Dictionary<string, clsHashInfo> lstMD5Results)
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
                return false;
            }

            hashInfo.HashCode = lstValues[0];

            var lstPathInfo = lstValues[1].Split('\t').ToList();

            var archiveFilePath = lstPathInfo[0];

            if (lstPathInfo.Count > 1)
                hashInfo.MyEMSLFileID = lstPathInfo[1];

            // Files should only be listed once in the MD5 results file
            // But, just in case there is a duplicate, we'll check for that
            // Results files could have duplicate entries if a file was copied to the archive via FTP and was stored via MyEMSL
            if (lstMD5Results.TryGetValue(archiveFilePath, out var hashInfoCached))
            {
                if (hashInfo.IsMatch(hashInfoCached))
                {
                    // Values match; nothing to update
                    return false;
                }

                // Preferentially use the newer value, unless the older value has a MyEMSL file ID but the newer one does not
                if (string.IsNullOrEmpty(hashInfo.MyEMSLFileID) && !string.IsNullOrEmpty(hashInfoCached.MyEMSLFileID))
                    // Do not update the dictionary
                    return false;

                lstMD5Results[archiveFilePath] = hashInfo;
                return true;
            }

            // Append a new entry to the cached info
            lstMD5Results.Add(archiveFilePath, hashInfo);
            return true;
        }

        private bool UpdateMD5ResultsFile(string md5ResultsFilePath, IEnumerable<MyEMSLFileInfo> archivedFiles)
        {
            bool success;

            var lstMD5Results = new Dictionary<string, clsHashInfo>(StringComparer.CurrentCultureIgnoreCase);

            // Read the file and cache the results in memory
            using (var srMD5ResultsFile = new StreamReader(new FileStream(md5ResultsFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
            {
                while (!srMD5ResultsFile.EndOfStream)
                {
                    var dataLine = srMD5ResultsFile.ReadLine();
                    ParseAndStoreHashInfo(dataLine, ref lstMD5Results);
                }
            }

            var saveMergedFile = false;

            // Merge the results in archivedFiles with lstMD5Results
            foreach (var archivedFile in archivedFiles)
            {
                var archivedFilePath = "/myemsl/svc-dms/" + archivedFile.PathWithInstrumentAndDatasetUnix;

                var hashInfo = new clsHashInfo
                {
                    HashCode = archivedFile.HashSum,
                    MyEMSLFileID = archivedFile.FileID.ToString(CultureInfo.InvariantCulture)
                };

                if (lstMD5Results.TryGetValue(archivedFilePath, out var hashInfoCached))
                {
                    if (!hashInfo.IsMatch(hashInfoCached))
                    {
                        lstMD5Results[archivedFilePath] = hashInfo;
                        saveMergedFile = true;
                    }
                }
                else
                {
                    lstMD5Results.Add(archivedFilePath, hashInfo);
                    saveMergedFile = true;
                }

            }

            if (saveMergedFile)
            {
                success = WriteMD5ResultsFile(lstMD5Results, md5ResultsFilePath, useTempFile: true);
            }
            else
            {
                success = true;
            }

            return success;
        }

        private bool VisibleInElasticSearch(out string metadataFilePath)
        {
            bool success;

            metadataFilePath = string.Empty;

            try
            {
                var metadataObject = new DMSMetadataObject(m_MgrName);

                // Attach the events
                metadataObject.DebugEvent += DMSMetadataObject_DebugEvent;
                metadataObject.ErrorEvent += DMSMetadataObject_ErrorEvent;

                var subDir = m_TaskParams.GetParam("OutputFolderName", string.Empty);

                var archivedFiles = metadataObject.GetDatasetFilesInMyEMSL(m_DatasetID, subDir);
                if (archivedFiles.Count == 0)
                {
                    if (mTotalMismatchCount == 0)
                        LogError("MyEmsl verification error for dataset " + m_Dataset + ", job " + m_Job);

                    mTotalMismatchCount += 1;

                    var msg = "MyEMSL status lookup did not report any files for Dataset_ID " + m_DatasetID;
                    if (!string.IsNullOrEmpty(subDir))
                        msg += " and subdirectory " + subDir;

                    mRetData.CloseoutMsg = msg;
                    LogError(" ... " + msg);
                    return false;
                }

                // Make sure the entries in archivedFiles only correspond to this dataset
                // We performed the search using DatasetID, but if a dataset is renamed in DMS, then multiple datasets could have the same DatasetID
                // Dataset renames are rare, but do happen (e.g. Dataset ID 382287 renamed from TB_UR_07_14Jul14_Methow_13-10-13 to TB_UR_08_14Jul14_Methow_13-10-14)
                var filteredFiles = new List<MyEMSLFileInfo>();

                foreach (var archiveFile in archivedFiles)
                {
                    foreach (var fileVersion in archiveFile.Value)
                    {
                        if (string.IsNullOrWhiteSpace(fileVersion.Dataset))
                        {
                            LogWarning("Dataset name not defined for MyEMSL file " + fileVersion);
                            filteredFiles.Add(fileVersion);
                        }
                        else if (string.Equals(fileVersion.Dataset, m_Dataset, StringComparison.OrdinalIgnoreCase))
                        {
                            filteredFiles.Add(fileVersion);
                        }
                        else
                        {
                            LogMessage(
                                "Query for dataset ID " + m_DatasetID + " yielded match to " + fileVersion.PathWithDataset +
                                " - skipping since wrong dataset", true);
                        }
                    }
                }

                success = CompareArchiveFilesToExpectedFiles(filteredFiles, out metadataFilePath);

                if (success)
                {
                    success = CreateOrUpdateMD5ResultsFile(filteredFiles);
                }

            }
            catch (Exception ex)
            {
                LogError("Exception in VisibleInElasticSearch", ex);
                return false;
            }

            return success;

        }

        private bool WriteMD5ResultsFile(Dictionary<string, clsHashInfo> lstMD5Results, string md5ResultsFilePath, bool useTempFile)
        {
            var currentStep = "initializing";

            try
            {
                string targetFilePath;
                if (useTempFile)
                {
                    // Create a new MD5 results file that we'll use to replace strMD5ResultsFileMaster
                    targetFilePath = md5ResultsFilePath + ".new";
                }
                else
                {
                    targetFilePath = md5ResultsFilePath;
                }

                currentStep = "creating " + targetFilePath;

                using (var swMD5ResultsFile = new StreamWriter(new FileStream(targetFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    foreach (var item in lstMD5Results)
                    {
                        var dataLine = item.Value.HashCode + " " + item.Key;
                        if (!string.IsNullOrEmpty(item.Value.MyEMSLFileID))
                            dataLine += "\t" + item.Value.MyEMSLFileID;

                        swMD5ResultsFile.WriteLine(dataLine);
                    }
                }

                System.Threading.Thread.Sleep(50);

                if (useTempFile)
                {
                    currentStep = "overwriting master MD5 results file with " + targetFilePath;
                    File.Copy(targetFilePath, md5ResultsFilePath, true);
                    System.Threading.Thread.Sleep(100);

                    currentStep = "deleting " + targetFilePath;
                    File.Delete(targetFilePath);
                }

                return true;

            }
            catch (Exception ex)
            {
                LogError("Exception in WriteMD5ResultsFile while " + currentStep, ex);
                return false;
            }

        }

        #endregion

        #region "Event Handlers"

        private void DMSMetadataObject_DebugEvent(object sender, MessageEventArgs e)
        {
            LogMessage(e.Message);
        }

        private void DMSMetadataObject_ErrorEvent(object sender, MessageEventArgs e)
        {
            LogError("MyEMSLReader: " + e.Message);
        }

        // ToDo: Deprecate
        void reader_ErrorEvent(string message, Exception ex)
        {
            LogError("MyEMSLReader: " + message, ex);
        }

        // ToDo: Deprecate
        void reader_MessageEvent(string message)
        {
            LogMessage(message);
        }

        // ToDo: Deprecate
        void reader_ProgressEvent(string progressMessage, float percentComplete)
        {
            var msg = "Percent complete: " + percentComplete.ToString("0.0") + "%";

            if (percentComplete > mPercentComplete || DateTime.UtcNow.Subtract(mLastProgressUpdateTime).TotalSeconds >= 30)
            {
                if (DateTime.UtcNow.Subtract(mLastProgressUpdateTime).TotalSeconds >= 1)
                {
                    LogDebug(msg);
                    mPercentComplete = percentComplete;
                    mLastProgressUpdateTime = DateTime.UtcNow;
                }
            }
        }

        void statusChecker_ErrorEvent(object sender, MessageEventArgs e)
        {
            LogError(e.Message);
        }

        #endregion

    }

}
