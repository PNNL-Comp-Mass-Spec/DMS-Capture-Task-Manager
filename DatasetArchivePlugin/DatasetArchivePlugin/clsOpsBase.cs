//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 10/20/2009
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using CaptureTaskManager;
using Pacifica.Core;
using Pacifica.DMS_Metadata;
using PRISM;
using System.IO;
using PRISM.Logging;

namespace DatasetArchivePlugin
{
    /// <summary>
    /// Base class for archive and archive update operations classes.
    /// </summary>
    abstract class clsOpsBase : clsEventNotifier
    {

        #region "Constants"

        private const string ARCHIVE = "Archive ";

        private const string UPDATE = "Archive update ";

        private const int LARGE_DATASET_THRESHOLD_GB_NO_RETRY = 15;

        private const string LARGE_DATASET_UPLOAD_ERROR = "Failure uploading a large amount of data; manual reset required";

        private const string SP_NAME_MAKE_NEW_ARCHIVE_UPDATE_JOB = "MakeNewArchiveUpdateJob";

        #endregion

        #region "Class variables"

        private readonly IMgrParams m_MgrParams;
        protected readonly ITaskParams m_TaskParams;
        private readonly IStatusFile m_StatusTools;
        private readonly clsFileTools m_FileTools;

        protected string m_ErrMsg = string.Empty;
        private string m_WarningMsg = string.Empty;
        private string m_DSNamePath;

        private bool m_MyEmslUploadSuccess;

        private readonly int m_DebugLevel;

        private readonly string m_ArchiveOrUpdate;

        private readonly clsExecuteDatabaseSP m_CaptureDBProcedureExecutor;

        protected string m_DatasetName = string.Empty;

        protected DateTime mLastStatusUpdateTime = DateTime.UtcNow;
        private DateTime mLastProgressUpdateTime = DateTime.UtcNow;

        private string mMostRecentLogMessage = string.Empty;
        protected DateTime mMostRecentLogTime = DateTime.UtcNow;

        #endregion

        #region "Properties"

        /// <summary>
        /// Implements IArchiveOps.ErrMsg
        /// </summary>
        public string ErrMsg => m_ErrMsg;

        /// <summary>
        /// True if the status indicates a critical error and there is no point in retrying
        /// </summary>
        public bool FailureDoNotRetry { get; set; }

        /// <summary>
        /// True to include additional log messages
        /// </summary>
        public bool TraceMode { get; }

        /// <summary>
        /// Warning message
        /// </summary>
        public string WarningMsg => m_WarningMsg;

        #endregion

        #region "Constructor"

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mgrParams"></param>
        /// <param name="taskParams"></param>
        /// <param name="statusTools"></param>
        /// <param name="fileTools"></param>
        protected clsOpsBase(IMgrParams mgrParams, ITaskParams taskParams, IStatusFile statusTools, clsFileTools fileTools)
        {
            m_MgrParams = mgrParams;
            m_TaskParams = taskParams;
            m_StatusTools = statusTools;
            m_FileTools = fileTools;

            // DebugLevel of 4 means Info level (normal) logging; 5 for Debug level (verbose) logging
            m_DebugLevel = m_MgrParams.GetParam("DebugLevel", 4);

            if (m_TaskParams.GetParam("StepTool") == "DatasetArchive")
            {
                m_ArchiveOrUpdate = ARCHIVE;
            }
            else
            {
                m_ArchiveOrUpdate = UPDATE;
            }

            // This connection string points to the DMS_Capture database
            var connectionString = m_MgrParams.GetParam("ConnectionString");
            m_CaptureDBProcedureExecutor = new clsExecuteDatabaseSP(connectionString);

            RegisterEvents(m_CaptureDBProcedureExecutor);

            TraceMode = m_MgrParams.GetParam("TraceMode", false);
        }

        #endregion

        #region "Methods"

        /// <summary>
        /// Sets up to perform an archive or update task (Implements IArchiveOps.PerformTask)
        /// Must be overridden in derived class
        /// </summary>
        /// <returns>TRUE for success, FALSE for failure</returns>
        public virtual bool PerformTask()
        {
            m_DatasetName = m_TaskParams.GetParam("Dataset");

            // Set client/server perspective & setup paths
            string baseStoragePath;
            if (m_MgrParams.GetParam("perspective").ToLower() == "client")
            {
                baseStoragePath = m_TaskParams.GetParam("Storage_Vol_External");
            }
            else
            {
                baseStoragePath = m_TaskParams.GetParam("Storage_Vol");
            }

            // Path to dataset on storage server
            m_DSNamePath = Path.Combine(Path.Combine(baseStoragePath, m_TaskParams.GetParam("Storage_Path")), m_TaskParams.GetParam("Folder"));

            // Verify dataset is in specified location
            if (!VerifyDSPresent(m_DSNamePath))
            {
                var errorMessage = "Dataset folder " + m_DSNamePath + " not found";
                m_ErrMsg = string.Copy(errorMessage);
                OnErrorEvent(errorMessage);
                LogOperationFailed(m_DatasetName, string.Empty, true);
                return false;
            }

            // Got to here, everything's OK, so let let the derived class take over
            return true;

        }

        private string AppendToString(string text, string append)
        {
            if (string.IsNullOrEmpty(text))
                text = string.Empty;
            else
                text += "; ";

            return text + append;
        }

        private void CreateArchiveUpdateJobsForDataset(IReadOnlyCollection<string> subdirectoryNames)
        {
            var failureMsg = "Error creating archive update tasks for dataset " + m_DatasetName + "; " +
                             "need to create ArchiveUpdate tasks for subdirectories " +
                             string.Join(", ", subdirectoryNames);

            try
            {

                // Setup for execution of the stored procedure
                var spCmd = new SqlCommand(SP_NAME_MAKE_NEW_ARCHIVE_UPDATE_JOB)
                {
                    CommandType = CommandType.StoredProcedure
                };

                spCmd.Parameters.Add("@Return", SqlDbType.Int).Direction = ParameterDirection.ReturnValue;
                spCmd.Parameters.Add("@DatasetName", SqlDbType.VarChar, 128).Value = m_DatasetName;
                var resultsFolderParam = spCmd.Parameters.Add("@ResultsFolderName", SqlDbType.VarChar, 128);
                spCmd.Parameters.Add("@AllowBlankResultsFolder", SqlDbType.TinyInt).Value = 0;
                spCmd.Parameters.Add("@PushDatasetToMyEMSL", SqlDbType.TinyInt).Value = 1;
                spCmd.Parameters.Add("@PushDatasetRecursive", SqlDbType.TinyInt).Value = 1;
                spCmd.Parameters.Add("@infoOnly", SqlDbType.TinyInt).Value = 0;
                spCmd.Parameters.Add("@message", SqlDbType.VarChar, 512).Direction = ParameterDirection.Output;

                var successCount = 0;
                foreach (var subdirectoryName in subdirectoryNames)
                {
                    resultsFolderParam.Value = subdirectoryName;

                    var datasetAndDirectory = string.Format("dataset {0}, subdirectory {1}", m_DatasetName, subdirectoryName);

                    LogTools.LogMessage("Creating archive update job for " + datasetAndDirectory);

                    // Execute the SP (retry the call up to 4 times)
                    var resCode = m_CaptureDBProcedureExecutor.ExecuteSP(spCmd, 4);

                    if (resCode == 0)
                    {
                        LogTools.LogDebug("Job successfully created");
                        successCount += 1;
                    }
                    else
                    {
                        LogTools.LogWarning(string.Format(
                                                "Unable to create archive update job for {0}; stored procedure returned resultCode {1}",
                                                datasetAndDirectory, resCode));
                    }
                }

                if (successCount < subdirectoryNames.Count)
                {
                    LogTools.LogError(failureMsg, null, true);
                }
            }
            catch (Exception ex)
            {
                LogTools.LogError(failureMsg, ex, true);
            }

        }

        /// <summary>
        /// Use MyEMSLUploader to upload the data to MyEMSL, trying up to maxAttempts times
        /// </summary>
        /// <param name="maxAttempts">Maximum upload attempts</param>
        /// <param name="recurse">True to find files in all subdirectories</param>
        /// <param name="debugMode">Debug mode options</param>
        /// <param name="useTestInstance">True to use the test instance</param>
        /// <param name="allowRetry">True if the calling procedure should retry a failed upload</param>
        /// <param name="criticalErrorMessage">Output: critical error message</param>
        /// <returns>True if success, false if an error</returns>
        protected bool UploadToMyEMSLWithRetry(
            int maxAttempts,
            bool recurse,
            EasyHttp.eDebugMode debugMode,
            bool useTestInstance,
            out bool allowRetry,
            out string criticalErrorMessage)
        {
            var success = false;
            var attempts = 0;

            allowRetry = true;
            m_MyEmslUploadSuccess = false;
            criticalErrorMessage = string.Empty;

            if (maxAttempts < 1)
                maxAttempts = 1;

            while (!success && attempts < maxAttempts)
            {
                attempts += 1;

                Console.WriteLine("Uploading files for " + m_DatasetName + " to MyEMSL; attempt=" + attempts);

                success = UploadToMyEMSL(recurse, debugMode, useTestInstance, out allowRetry, out criticalErrorMessage);

                if (!allowRetry)
                    break;

                if (debugMode != EasyHttp.eDebugMode.DebugDisabled)
                    break;

                if (!success && attempts < maxAttempts)
                {
                    // Wait 5 seconds, then retry
                    Thread.Sleep(5000);

                    mLastStatusUpdateTime = DateTime.UtcNow;
                }
            }

            if (!success)
            {
                if (debugMode != EasyHttp.eDebugMode.DebugDisabled)
                    m_WarningMsg = "Debug mode was enabled; thus, .tar file was created locally and not uploaded to MyEMSL";
                else
                    m_WarningMsg = AppendToString(m_WarningMsg, "UploadToMyEMSL reports False");
            }

            if (success && !m_MyEmslUploadSuccess)
                m_WarningMsg = AppendToString(m_WarningMsg, "UploadToMyEMSL reports True but m_MyEmslUploadSuccess is False");

            return success && m_MyEmslUploadSuccess;
        }

        /// <summary>
        /// Use MyEMSLUploader to upload the data to MyEMSL, trying up to maxAttempts times
        /// </summary>
        /// <param name="recurse">True to find files in all subdirectories</param>
        /// <param name="debugMode">
        /// Set to eDebugMode.CreateTarLocal to authenticate with MyEMSL, then create a .tar file locally instead of actually uploading it
        /// Set to eDebugMode.MyEMSLOfflineMode to create the .tar file locally without contacting MyEMSL
        /// </param>
        /// <param name="useTestInstance">True to use the test instance</param>
        /// <param name="allowRetry">Output: whether the upload should be retried if it failed</param>
        /// <param name="criticalErrorMessage">Output: critical error message</param>
        /// <returns>True if success, false if an error</returns>
        private bool UploadToMyEMSL(bool recurse, EasyHttp.eDebugMode debugMode, bool useTestInstance, out bool allowRetry, out string criticalErrorMessage)
        {
            var startTime = DateTime.UtcNow;

            MyEMSLUploader myEMSLUploader = null;
            var operatorUsername = "??";

            allowRetry = true;
            criticalErrorMessage = string.Empty;

            try
            {
                m_ErrMsg = string.Empty;

                var statusMessage = "Bundling changes to dataset " + m_DatasetName + " for transmission to MyEMSL";
                OnStatusEvent(statusMessage);

                if (!recurse)
                {
                    OnStatusEvent("Recursion is disabled since job param MyEMSLRecurse is false");
                }

                var config = new Configuration();

                myEMSLUploader = new MyEMSLUploader(config, m_MgrParams.ParamDictionary, m_TaskParams.TaskDictionary, m_FileTools)
                {
                    TraceMode = TraceMode
                };

                // Attach the events
                RegisterEvents(myEMSLUploader);

                myEMSLUploader.StatusUpdate += myEMSLUploader_StatusUpdate;
                myEMSLUploader.UploadCompleted += myEMSLUploader_UploadCompleted;

                myEMSLUploader.MetadataDefinedEvent += myEMSLUploader_MetadataDefinedEvent;

                m_TaskParams.AddAdditionalParameter(MyEMSLUploader.RECURSIVE_UPLOAD, recurse.ToString());

                // Cache the operator name; used in the exception handler below
                operatorUsername = m_TaskParams.GetParam("Operator_PRN", "Unknown_Operator");

                config.UseTestInstance = useTestInstance;

                if (useTestInstance)
                {
                    myEMSLUploader.UseTestInstance = true;

                    statusMessage = "Sending dataset to MyEMSL test instance";
                    OnStatusEvent(statusMessage);
                }

                // Start the upload
                var success = myEMSLUploader.SetupMetadataAndUpload(config, debugMode, out var statusURL);

                criticalErrorMessage = myEMSLUploader.CriticalErrorMessage;

                if (!string.IsNullOrWhiteSpace(criticalErrorMessage) || string.Equals(statusURL, MyEMSLUploader.CRITICAL_UPLOAD_ERROR))
                {
                    allowRetry = false;
                }

                var elapsedTime = DateTime.UtcNow.Subtract(startTime);

                statusMessage = "Upload of " + m_DatasetName + " completed in " + elapsedTime.TotalSeconds.ToString("0.0") + " seconds";
                if (!success)
                {
                    statusMessage += " (success=false)";
                    if (clsUtilities.BytesToGB(myEMSLUploader.MetadataContainer.TotalFileSizeToUpload) > LARGE_DATASET_THRESHOLD_GB_NO_RETRY)
                    {
                        m_ErrMsg = LARGE_DATASET_UPLOAD_ERROR;

                        // Do not auto-retry uploads over 15 GB; force an admin to check on things
                        allowRetry = false;
                    }
                }

                statusMessage += ": " +
                                 myEMSLUploader.FileCountNew + " new files, " +
                                 myEMSLUploader.FileCountUpdated + " updated files, " +
                                 myEMSLUploader.Bytes + " bytes";

                OnStatusEvent(statusMessage);

                if (debugMode != EasyHttp.eDebugMode.DebugDisabled)
                    return false;

                statusMessage = "myEMSL statusURI => " + myEMSLUploader.StatusURI;

                if (!string.IsNullOrEmpty(statusURL) && statusURL.EndsWith("/1323420608"))
                {
                    statusMessage += "; this indicates an upload error (transactionID=-1)";
                    OnErrorEvent(statusMessage);
                    return false;
                }

                if (myEMSLUploader.FileCountNew + myEMSLUploader.FileCountUpdated > 0 || !string.IsNullOrEmpty(statusURL))
                    OnStatusEvent(statusMessage);

                // Raise an event with the stats
                // This will cause clsPluginMain to call StoreMyEMSLUploadStats to store the results in the database (Table T_MyEmsl_Uploads)
                // If an error occurs while storing to the database, the status URI will be listed in the manager's local log file
                var e = new MyEMSLUploadEventArgs(
                    myEMSLUploader.FileCountNew, myEMSLUploader.FileCountUpdated,
                    myEMSLUploader.Bytes, elapsedTime.TotalSeconds,
                    statusURL, myEMSLUploader.EUSInfo,
                    iErrorCode: 0, usedTestInstance: useTestInstance);

                OnMyEMSLUploadComplete(e);

                m_StatusTools.UpdateAndWrite(100);

                if (!success)
                    return false;

                var skippedSubdirectories = myEMSLUploader.MetadataContainer.SkippedDatasetArchiveSubdirectories;
                if (skippedSubdirectories.Count == 0)
                    return true;

                if (!m_MyEmslUploadSuccess)
                {
                    var msg = "SetupMetadataAndUpload reported true for dataset " + m_DatasetName +
                              " but m_MyEmslUploadSuccess is false; need to create ArchiveUpdate tasks for subdirectories " +
                              string.Join(", ", skippedSubdirectories);
                    LogTools.LogError(msg, null, true);

                    // Return true since the primary archive task succeeded
                    return true;
                }

                CreateArchiveUpdateJobsForDataset(myEMSLUploader.MetadataContainer.SkippedDatasetArchiveSubdirectories);

                return true;

            }
            catch (Exception ex)
            {
                const string errorMessage = "Exception uploading to MyEMSL";
                m_ErrMsg = string.Copy(errorMessage);
                OnErrorEvent(errorMessage, ex);

                if (ex.Message.Contains(DMSMetadataObject.UNDEFINED_EUS_OPERATOR_ID))
                {
                    if (ex.Message.Contains(DMSMetadataObject.UNDEFINED_EUS_OPERATOR_ID))

                        m_ErrMsg += "; operator not defined in EUS. " +
                                    "Have " + operatorUsername + " login to " + DMSMetadataObject.EUS_PORTAL_URL + " " +
                                    "then wait for T_EUS_Users to update, " +
                                    "then update job parameters using SP UpdateParametersForJob";

                    // Do not retry the upload; it will fail again due to the same error
                    allowRetry = false;
                    LogOperationFailed(m_DatasetName, ex.Message, true);
                }
                else if (ex.Message.Contains(DMSMetadataObject.SOURCE_DIRECTORY_NOT_FOUND))
                {
                    m_ErrMsg += ": " + DMSMetadataObject.SOURCE_DIRECTORY_NOT_FOUND;

                    // Do not retry the upload; it will fail again due to the same error
                    allowRetry = false;
                    LogOperationFailed(m_DatasetName, m_ErrMsg, true);
                }
                else if (ex.Message.Contains(DMSMetadataObject.TOO_MANY_FILES_TO_ARCHIVE))
                {
                    m_ErrMsg += ": " + DMSMetadataObject.TOO_MANY_FILES_TO_ARCHIVE;

                    // Do not retry the upload; it will fail again due to the same error
                    allowRetry = false;
                    LogOperationFailed(m_DatasetName, m_ErrMsg, true);
                }
                else if (clsUtilities.BytesToGB(myEMSLUploader.MetadataContainer.TotalFileSizeToUpload) > LARGE_DATASET_THRESHOLD_GB_NO_RETRY)
                {
                    m_ErrMsg += ": " + LARGE_DATASET_UPLOAD_ERROR;

                    // Do not auto-retry uploads of over 15 GB in size; force an admin to check on things
                    allowRetry = false;
                    LogOperationFailed(m_DatasetName, m_ErrMsg, true);
                }
                else
                {
                    LogOperationFailed(m_DatasetName);
                }

                // Raise an event with the stats, though only if errorCode is non-zero or FileCountNew or FileCountUpdated are positive

                var errorCode = ex.Message.GetHashCode();
                if (errorCode == 0)
                    errorCode = 1;

                var elapsedTime = DateTime.UtcNow.Subtract(startTime);

                if (myEMSLUploader == null)
                {
                    if (errorCode == 0)
                        return false;

                    var eusInfo = new Upload.EUSInfo();
                    eusInfo.Clear();

                    var emptyArgs = new MyEMSLUploadEventArgs(
                        0, 0,
                        0, elapsedTime.TotalSeconds,
                        string.Empty, eusInfo,
                        errorCode, useTestInstance);

                    OnMyEMSLUploadComplete(emptyArgs);
                    return false;
                }

                // Exit this method (skipping the call to MyEMSLUploadEventArgs) if the error code is 0 and no files were added or updated
                if (errorCode == 0 && myEMSLUploader.FileCountNew == 0 && myEMSLUploader.FileCountUpdated == 0)
                    return false;

                var uploadArgs = new MyEMSLUploadEventArgs(
                    myEMSLUploader.FileCountNew, myEMSLUploader.FileCountUpdated,
                    myEMSLUploader.Bytes, elapsedTime.TotalSeconds,
                    myEMSLUploader.StatusURI, myEMSLUploader.EUSInfo,
                    errorCode, useTestInstance);

                OnMyEMSLUploadComplete(uploadArgs);

                return false;
            }
            finally
            {
                if (myEMSLUploader != null)
                {
                    // Unsubscribe from the events
                    // This should be automatic, but we're seeing duplicate messages of the form
                    // "... uploading, 25.0% complete for 48,390 KB",
                    // implying these are not being removed

                    myEMSLUploader.DebugEvent -= OnDebugEvent;
                    myEMSLUploader.StatusEvent -= OnStatusEvent;
                    myEMSLUploader.ErrorEvent -= OnErrorEvent;
                    myEMSLUploader.WarningEvent -= OnWarningEvent;
                    myEMSLUploader.ProgressUpdate -= OnProgressUpdate;

                    myEMSLUploader.StatusUpdate -= myEMSLUploader_StatusUpdate;
                    myEMSLUploader.UploadCompleted -= myEMSLUploader_UploadCompleted;

                    myEMSLUploader.MetadataDefinedEvent -= myEMSLUploader_MetadataDefinedEvent;
                }
            }

        }

        /// <summary>
        /// Return the text after the colon in statusMessage
        /// </summary>
        /// <param name="statusMessage"></param>
        /// <returns>Text if the colon was found, otherwise an empty string</returns>
        private string GetFilenameFromStatus(string statusMessage)
        {
            var colonIndex = statusMessage.IndexOf(':');
            if (colonIndex >= 0)
                return statusMessage.Substring(colonIndex + 1).Trim();

            return string.Empty;
        }

        /// <summary>
        /// Writes a log entry for a failed archive operation
        /// </summary>
        /// <param name="dsName">Name of dataset</param>
        /// <param name="reason">Reason for failed operation</param>
        /// <param name="logToDB">True to log to the database and a local file; false for local file only</param>
        private void LogOperationFailed(string dsName, string reason = "", bool logToDB = false)
        {
            var msg = m_ArchiveOrUpdate + "failed, dataset " + dsName;
            if (!string.IsNullOrEmpty(reason))
                msg += "; " + reason;

            if (logToDB)
                LogTools.LogError(msg, null, true);
            else
                OnErrorEvent(msg);
        }

        /// <summary>
        /// Verifies that the specified dataset folder exists
        /// </summary>
        /// <param name="dsNamePath">Fully qualified path to dataset folder</param>
        /// <returns>TRUE if dataset folder is present; otherwise FALSE</returns>
        private bool VerifyDSPresent(string dsNamePath)
        {
            return Directory.Exists(dsNamePath);
        }

        #endregion

        #region "Event Delegates and Classes"

        public event MyEMSLUploadEventHandler MyEMSLUploadComplete;

        #endregion

        #region "Event Handlers"

        void LogStatusMessageSkipDuplicate(string message)
        {
            if (!string.Equals(message, mMostRecentLogMessage) || DateTime.UtcNow.Subtract(mMostRecentLogTime).TotalSeconds >= 60)
            {
                mMostRecentLogMessage = string.Copy(message);
                mMostRecentLogTime = DateTime.UtcNow;
                OnStatusEvent(message);
            }
        }

        void myEMSLUploader_MetadataDefinedEvent(object sender, MessageEventArgs e)
        {
            if (m_DebugLevel >= 5)
            {
                // e.Message contains the metadata JSON
                OnDebugEvent(e.Message);
            }
        }

        void myEMSLUploader_StatusUpdate(object sender, StatusEventArgs e)
        {

            if (DateTime.UtcNow.Subtract(mLastStatusUpdateTime).TotalSeconds >= 60 && e.PercentCompleted > 0)
            {
                mLastStatusUpdateTime = DateTime.UtcNow;
                string verb;
                string filename;

                if (e.StatusMessage.StartsWith(DMSMetadataObject.HASHING_FILES))
                {
                    verb = "hashing files";
                    filename = GetFilenameFromStatus(e.StatusMessage);
                }
                else if (e.StatusMessage.StartsWith(EasyHttp.UPLOADING_FILES))
                {
                    verb = "uploading files";
                    filename = GetFilenameFromStatus(e.StatusMessage);
                }
                else
                {
                    verb = "processing";
                    filename = "";
                }

                // Example log message:
                // ... uploading files, 97.3% complete for 35,540 KB; QC_Shew_16-01_1_20Jul17_Merry_17-05-03_dta.zip

                var msgBase = "  ... " + verb + ", " + e.PercentCompleted.ToString("0.0") + "% complete";

                string msg;

                if (e.TotalBytesToSend > 0)
                    msg = msgBase + " for " + (e.TotalBytesToSend / 1024.0).ToString("#,##0") + " KB";
                else
                    msg = msgBase;

                if (string.IsNullOrEmpty(filename))
                    LogStatusMessageSkipDuplicate(msg);
                else
                    LogStatusMessageSkipDuplicate(msg + "; " + filename);

            }

            if (DateTime.UtcNow.Subtract(mLastProgressUpdateTime).TotalSeconds >= 3 && e.PercentCompleted > 0)
            {
                mLastProgressUpdateTime = DateTime.UtcNow;
                m_StatusTools.UpdateAndWrite((float)e.PercentCompleted);
            }

        }

        void myEMSLUploader_UploadCompleted(object sender, UploadCompletedEventArgs e)
        {
            var msg = "  ... MyEmsl upload task complete";

            // Note that e.ServerResponse will simply have the StatusURL if the upload succeeded
            // If a problem occurred, e.ServerResponse will either have the full server response, or may even be blank
            if (string.IsNullOrEmpty(e.ServerResponse))
                msg += ": empty server response";
            else
                msg += ": " + e.ServerResponse;

            OnDebugEvent(msg);
            m_MyEmslUploadSuccess = true;
        }

        private void OnMyEMSLUploadComplete(MyEMSLUploadEventArgs e)
        {
            MyEMSLUploadComplete?.Invoke(this, e);
        }
        #endregion

    }

}
