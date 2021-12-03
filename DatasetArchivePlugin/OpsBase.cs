//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 10/20/2009
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Threading;
using CaptureTaskManager;
using Pacifica.Core;
using Pacifica.DMS_Metadata;
using Pacifica.Upload;
using PRISM;
using PRISM.Logging;
using PRISMDatabaseUtils;

namespace DatasetArchivePlugin
{
    /// <summary>
    /// Base class for archive and archive update operations classes.
    /// </summary>
    internal abstract class OpsBase : EventNotifier
    {
        // Ignore Spelling: MyEMSLUploader, Unsubscribe

        private const string ARCHIVE = "Archive";

        private const string UPDATE = "Archive update";

        private const int LARGE_DATASET_THRESHOLD_GB_NO_RETRY = 15;

        private const string LARGE_DATASET_UPLOAD_ERROR = "Failure uploading a large amount of data; manual reset required";

        private const string SP_NAME_MAKE_NEW_ARCHIVE_UPDATE_JOB = "MakeNewArchiveUpdateJob";

        private readonly IMgrParams mMgrParams;
        protected readonly ITaskParams mTaskParams;
        private readonly IStatusFile mStatusTools;
        private readonly FileTools mFileTools;

        protected string mErrMsg = string.Empty;
        private string mDSNamePath;

        private bool mMyEmslUploadSuccess;

        private readonly int mDebugLevel;

        private readonly string mArchiveOrUpdate;

        private readonly IDBTools mCaptureDbProcedureExecutor;

        protected string mDatasetName = string.Empty;

        protected DateTime mLastStatusUpdateTime = DateTime.UtcNow;
        private DateTime mLastProgressUpdateTime = DateTime.UtcNow;

        private string mMostRecentLogMessage = string.Empty;
        protected DateTime mMostRecentLogTime = DateTime.UtcNow;

        /// <summary>
        /// Implements IArchiveOps.ErrMsg
        /// </summary>
        public string ErrMsg => mErrMsg;

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
        public string WarningMsg { get; private set; } = string.Empty;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mgrParams"></param>
        /// <param name="taskParams"></param>
        /// <param name="statusTools"></param>
        /// <param name="fileTools"></param>
        protected OpsBase(IMgrParams mgrParams, ITaskParams taskParams, IStatusFile statusTools, FileTools fileTools)
        {
            mMgrParams = mgrParams;
            mTaskParams = taskParams;
            mStatusTools = statusTools;
            mFileTools = fileTools;

            // DebugLevel of 4 means Info level (normal) logging; 5 for Debug level (verbose) logging
            mDebugLevel = mMgrParams.GetParam("DebugLevel", 4);

            TraceMode = mMgrParams.GetParam("TraceMode", false);

            if (mTaskParams.GetParam("StepTool") == "DatasetArchive")
            {
                mArchiveOrUpdate = ARCHIVE;
            }
            else
            {
                mArchiveOrUpdate = UPDATE;
            }

            // This connection string points to the DMS_Capture database
            var connectionString = mMgrParams.GetParam("ConnectionString");

            var applicationName = string.Format("{0}_DatasetArchive", mMgrParams.ManagerName);

            var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(connectionString, applicationName);

            mCaptureDbProcedureExecutor = DbToolsFactory.GetDBTools(connectionStringToUse, debugMode: TraceMode);
            RegisterEvents(mCaptureDbProcedureExecutor);
        }

        /// <summary>
        /// Sets up to perform an archive or update task (Implements IArchiveOps.PerformTask)
        /// Must be overridden in derived class
        /// </summary>
        /// <returns>TRUE for success, FALSE for failure</returns>
        public virtual bool PerformTask()
        {
            mDatasetName = mTaskParams.GetParam("Dataset");

            // Set client/server perspective & setup paths
            string baseStoragePath;
            if (string.Equals(mMgrParams.GetParam("perspective"), "client", StringComparison.OrdinalIgnoreCase))
            {
                baseStoragePath = mTaskParams.GetParam("Storage_Vol_External");
            }
            else
            {
                baseStoragePath = mTaskParams.GetParam("Storage_Vol");
            }

            // Path to dataset on storage server
            var storagePath = mTaskParams.GetParam("Storage_Path");
            var datasetDirectory = mTaskParams.GetParam(mTaskParams.HasParam("Directory") ? "Directory" : "Folder");

            mDSNamePath = Path.Combine(Path.Combine(baseStoragePath, storagePath, datasetDirectory));

            // Verify dataset is in specified location
            if (!VerifyDSPresent(mDSNamePath))
            {
                mErrMsg = "Dataset folder " + mDSNamePath + " not found";
                OnErrorEvent(mErrMsg);
                LogOperationFailed(mDatasetName, string.Empty, true);
                return false;
            }

            // Got to here, everything's OK, so let the derived class take over
            return true;
        }

        private void CreateArchiveUpdateJobsForDataset(IReadOnlyCollection<string> subdirectoryNames)
        {
            var failureMsg = "Error creating archive update tasks for dataset " + mDatasetName + "; " +
                             "need to create ArchiveUpdate tasks for subdirectories " +
                             string.Join(", ", subdirectoryNames);

            try
            {
                // Setup for execution of the stored procedure
                var dbTools = mCaptureDbProcedureExecutor;
                var cmd = dbTools.CreateCommand(SP_NAME_MAKE_NEW_ARCHIVE_UPDATE_JOB, CommandType.StoredProcedure);

                dbTools.AddParameter(cmd, "@Return", SqlType.Int, ParameterDirection.ReturnValue);
                dbTools.AddParameter(cmd, "@datasetName", SqlType.VarChar, 128, mDatasetName);
                var resultsDirectoryParam = dbTools.AddParameter(cmd, "@resultsDirectoryName", SqlType.VarChar, 128);
                dbTools.AddTypedParameter(cmd, "@allowBlankResultsDirectory", SqlType.TinyInt, value: 0);
                dbTools.AddTypedParameter(cmd, "@pushDatasetToMyEMSL", SqlType.TinyInt, value: 1);
                dbTools.AddTypedParameter(cmd, "@pushDatasetRecursive", SqlType.TinyInt, value: 1);
                dbTools.AddTypedParameter(cmd, "@infoOnly", SqlType.TinyInt, value: 0);
                dbTools.AddParameter(cmd, "@message", SqlType.VarChar, 512, ParameterDirection.Output);

                var successCount = 0;
                foreach (var subdirectoryName in subdirectoryNames)
                {
                    resultsDirectoryParam.Value = subdirectoryName;

                    var datasetAndDirectory = string.Format("dataset {0}, subdirectory {1}", mDatasetName, subdirectoryName);

                    LogTools.LogMessage("Creating archive update job for " + datasetAndDirectory);

                    // Execute the SP (retry the call up to 4 times)
                    var resCode = mCaptureDbProcedureExecutor.ExecuteSP(cmd, 4);

                    if (resCode == 0)
                    {
                        LogTools.LogDebug("Job successfully created");
                        successCount++;
                    }
                    else
                    {
                        LogTools.LogWarning("Unable to create archive update job for {0}; stored procedure returned resultCode {1}",
                            datasetAndDirectory, resCode);
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
            TarStreamUploader.UploadDebugMode debugMode,
            bool useTestInstance,
            out bool allowRetry,
            out string criticalErrorMessage)
        {
            var success = false;
            var attempts = 0;

            allowRetry = true;
            mMyEmslUploadSuccess = false;
            criticalErrorMessage = string.Empty;

            if (maxAttempts < 1)
            {
                maxAttempts = 1;
            }

            while (!success && attempts < maxAttempts)
            {
                attempts++;

                Console.WriteLine("Uploading files for " + mDatasetName + " to MyEMSL; attempt=" + attempts);

                success = UploadToMyEMSL(recurse, debugMode, useTestInstance, out allowRetry, out criticalErrorMessage);

                if (!allowRetry)
                {
                    break;
                }

                if (debugMode != TarStreamUploader.UploadDebugMode.DebugDisabled)
                {
                    break;
                }

                if (!success && attempts < maxAttempts)
                {
                    // Wait 5 seconds, then retry
                    Thread.Sleep(5000);

                    mLastStatusUpdateTime = DateTime.UtcNow;
                }
            }

            if (!success)
            {
                if (debugMode != TarStreamUploader.UploadDebugMode.DebugDisabled)
                {
                    WarningMsg = "Debug mode was enabled; thus, .tar file was created locally and not uploaded to MyEMSL";
                }
                else
                {
                    WarningMsg = CTMUtilities.AppendToString(WarningMsg, "UploadToMyEMSL reports False");
                }
            }

            if (success && !mMyEmslUploadSuccess)
            {
                WarningMsg = CTMUtilities.AppendToString(WarningMsg, "UploadToMyEMSL reports True but mMyEmslUploadSuccess is False");
            }

            return success && mMyEmslUploadSuccess;
        }

        /// <summary>
        /// Use MyEMSLUploader to upload the data to MyEMSL, trying up to maxAttempts times
        /// </summary>
        /// <param name="recurse">True to find files in all subdirectories</param>
        /// <param name="debugMode">
        /// Set to DebugMode.CreateTarLocal to authenticate with MyEMSL, then create a .tar file locally instead of actually uploading it
        /// Set to DebugMode.MyEMSLOfflineMode to create the .tar file locally without contacting MyEMSL
        /// </param>
        /// <param name="useTestInstance">True to use the test instance</param>
        /// <param name="allowRetry">Output: whether the upload should be retried if it failed</param>
        /// <param name="criticalErrorMessage">Output: critical error message</param>
        /// <returns>True if success, false if an error</returns>
        private bool UploadToMyEMSL(
            bool recurse,
            TarStreamUploader.UploadDebugMode debugMode,
            bool useTestInstance,
            out bool allowRetry,
            out string criticalErrorMessage)
        {
            var startTime = DateTime.UtcNow;

            MyEMSLUploader myEMSLUploader = null;

            var operatorUsername = "??";

            allowRetry = true;
            criticalErrorMessage = string.Empty;

            try
            {
                mErrMsg = string.Empty;

                var statusMessage = "Bundling changes to dataset " + mDatasetName + " for transmission to MyEMSL";
                OnStatusEvent(statusMessage);

                if (!recurse)
                {
                    OnStatusEvent("Recursion is disabled since job param MyEMSLRecurse is false");
                }

                var config = new Configuration();

                myEMSLUploader = new MyEMSLUploader(config, mMgrParams.MgrParams, mTaskParams.TaskDictionary, mFileTools)
                {
                    TraceMode = TraceMode
                };

                // Attach the events
                RegisterEvents(myEMSLUploader);

                myEMSLUploader.StatusUpdate += myEMSLUploader_StatusUpdate;
                myEMSLUploader.UploadCompleted += myEMSLUploader_UploadCompleted;

                myEMSLUploader.MetadataDefinedEvent += myEMSLUploader_MetadataDefinedEvent;

                mTaskParams.AddAdditionalParameter(MyEMSLUploader.RECURSIVE_UPLOAD, recurse.ToString());

                // Cache the operator name; used in the exception handler below
                operatorUsername = mTaskParams.GetParam("Operator_PRN", "Unknown_Operator");

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

                if (!success)
                {
                    if (string.IsNullOrEmpty(criticalErrorMessage))
                        OnWarningEvent("SetupMetadata returned false (myEMSLUploader.CriticalErrorMessage is empty)");
                    else
                        OnWarningEvent("SetupMetadata returned false: " + criticalErrorMessage);
                }

                if (success || string.IsNullOrEmpty(criticalErrorMessage))
                {
                    OnStatusEvent("EUS metadata: Instrument ID {0}, Project ID {1}, Uploader ID {2}",
                        myEMSLUploader.EUSInfo.EUSInstrumentID,
                        myEMSLUploader.EUSInfo.EUSProjectID,
                        myEMSLUploader.EUSInfo.EUSUploaderID);
                }

                if (!string.IsNullOrWhiteSpace(criticalErrorMessage) || string.Equals(statusURL, MyEMSLUploader.CRITICAL_UPLOAD_ERROR))
                {
                    allowRetry = false;
                }

                var elapsedTime = DateTime.UtcNow.Subtract(startTime);

                statusMessage = "Upload of " + mDatasetName + " completed in " + elapsedTime.TotalSeconds.ToString("0.0") + " seconds";
                if (!success)
                {
                    statusMessage += " (success=false)";
                    if (CTMUtilities.BytesToGB(myEMSLUploader.MetadataContainer.TotalFileSizeToUpload) > LARGE_DATASET_THRESHOLD_GB_NO_RETRY)
                    {
                        mErrMsg = LARGE_DATASET_UPLOAD_ERROR;

                        // Do not auto-retry uploads over 15 GB; force an admin to check on things
                        allowRetry = false;
                    }
                }

                statusMessage += ": " +
                                 myEMSLUploader.FileCountNew + " new files, " +
                                 myEMSLUploader.FileCountUpdated + " updated files, " +
                                 myEMSLUploader.Bytes + " bytes";

                OnStatusEvent(statusMessage);

                if (debugMode != TarStreamUploader.UploadDebugMode.DebugDisabled)
                {
                    return false;
                }

                statusMessage = "myEMSL statusURI => " + myEMSLUploader.StatusURI;

                if (!string.IsNullOrEmpty(statusURL) && statusURL.EndsWith("/1323420608"))
                {
                    statusMessage += "; this indicates an upload error (transactionID=-1)";
                    OnErrorEvent(statusMessage);
                    return false;
                }

                if (myEMSLUploader.FileCountNew + myEMSLUploader.FileCountUpdated > 0 || !string.IsNullOrEmpty(statusURL))
                {
                    OnStatusEvent(statusMessage);
                }

                // Raise an event with the stats
                // This will cause PluginMain to call StoreMyEMSLUploadStats to store the results in the database (Table T_MyEmsl_Uploads)
                // If an error occurs while storing to the database, the status URI will be listed in the manager's local log file
                var e = new MyEMSLUploadEventArgs(
                    myEMSLUploader.FileCountNew, myEMSLUploader.FileCountUpdated,
                    myEMSLUploader.Bytes, elapsedTime.TotalSeconds,
                    statusURL, myEMSLUploader.EUSInfo,
                    errorCode: 0, usedTestInstance: useTestInstance);

                OnMyEMSLUploadComplete(e);

                mStatusTools.UpdateAndWrite(100);

                if (!success)
                {
                    return false;
                }

                var skippedSubdirectories = myEMSLUploader.MetadataContainer.SkippedDatasetArchiveSubdirectories;
                if (skippedSubdirectories.Count == 0)
                {
                    return true;
                }

                if (!mMyEmslUploadSuccess)
                {
                    LogTools.LogError(string.Format(
                        "SetupMetadataAndUpload reported true for dataset {0} but mMyEmslUploadSuccess is false; " +
                        "need to create ArchiveUpdate tasks for subdirectories {1}",
                        mDatasetName, string.Join(", ", skippedSubdirectories)),
                        null, true);

                    // Return true since the primary archive task succeeded
                    return true;
                }

                CreateArchiveUpdateJobsForDataset(myEMSLUploader.MetadataContainer.SkippedDatasetArchiveSubdirectories);

                return true;
            }
            catch (DirectoryNotFoundException ex)
            {
                if (ex.Message.Contains(DMSMetadataObject.SOURCE_DIRECTORY_NOT_FOUND))
                {
                    mErrMsg = CTMUtilities.AppendToString(mErrMsg, DMSMetadataObject.SOURCE_DIRECTORY_NOT_FOUND, ": ");
                }
                else
                {
                    mErrMsg = CTMUtilities.AppendToString(mErrMsg, ex.Message, ": ");
                }

                // Do not retry the upload; it will fail again due to the same error
                allowRetry = false;
                LogOperationFailed(mDatasetName, mErrMsg, true);

                HandleUploadException(ex, myEMSLUploader, startTime, useTestInstance);
                return false;
            }
            catch (Exception ex)
            {
                const string errorMessage = "Exception uploading to MyEMSL";
                mErrMsg = errorMessage;
                OnErrorEvent(errorMessage, ex);

                if (ex.Message.Contains(DMSMetadataObject.UNDEFINED_EUS_OPERATOR_ID))
                {
                    mErrMsg += "; operator not defined in EUS. " +
                                "Have " + operatorUsername + " login to " + DMSMetadataObject.EUS_PORTAL_URL + " " +
                                "then wait for T_EUS_Users to update, " +
                                "then update job parameters using SP UpdateParametersForJob";

                    // Do not retry the upload; it will fail again due to the same error
                    allowRetry = false;
                    LogOperationFailed(mDatasetName, ex.Message, true);
                }
                else if (ex.Message.Contains(DMSMetadataObject.TOO_MANY_FILES_TO_ARCHIVE))
                {
                    mErrMsg += ": " + DMSMetadataObject.TOO_MANY_FILES_TO_ARCHIVE;

                    var jobNumber = mTaskParams.GetParam("Job", 0);

                    mErrMsg += string.Format(
                        " (to ignore this error, use Exec AddUpdateJobParameter {0}, 'StepParameters', 'IgnoreMaxFileLimit', '1')", jobNumber);

                    // Do not retry the upload; it will fail again due to the same error
                    allowRetry = false;
                    LogOperationFailed(mDatasetName, mErrMsg, true);
                }
                else if (CTMUtilities.BytesToGB(myEMSLUploader.MetadataContainer.TotalFileSizeToUpload) > LARGE_DATASET_THRESHOLD_GB_NO_RETRY)
                {
                    mErrMsg += ": " + LARGE_DATASET_UPLOAD_ERROR;

                    // Do not auto-retry uploads of over 15 GB in size; force an admin to check on things
                    allowRetry = false;
                    LogOperationFailed(mDatasetName, mErrMsg, true);
                }
                else
                {
                    LogOperationFailed(mDatasetName);
                }

                HandleUploadException(ex, myEMSLUploader, startTime, useTestInstance);
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
            {
                return statusMessage.Substring(colonIndex + 1).Trim();
            }

            return string.Empty;
        }

        /// <summary>
        /// Perform several tasks after the MyEMSL Uploader raises an exception
        /// </summary>
        /// <param name="ex"></param>
        /// <param name="myEMSLUploader"></param>
        /// <param name="startTime"></param>
        /// <param name="useTestInstance"></param>
        private void HandleUploadException(Exception ex, MyEMSLUploader myEMSLUploader, DateTime startTime, bool useTestInstance)
        {
            // Raise an event with the stats, though only if errorCode is non-zero or FileCountNew or FileCountUpdated are positive

            var errorCode = ex.Message.GetHashCode();
            if (errorCode == 0)
            {
                errorCode = 1;
            }

            var elapsedTime = DateTime.UtcNow.Subtract(startTime);

            if (myEMSLUploader == null)
            {
                // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                if (errorCode == 0)
                {
                    return;
                }

                var eusInfo = new Upload.EUSInfo();
                eusInfo.Clear();

                var emptyArgs = new MyEMSLUploadEventArgs(
                    0, 0,
                    0, elapsedTime.TotalSeconds,
                    string.Empty, eusInfo,
                    errorCode, useTestInstance);

                OnMyEMSLUploadComplete(emptyArgs);
                return;
            }

            // Exit this method (skipping the call to OnMyEMSLUploadComplete) if the error code is 0 and no files were added or updated
            // ReSharper disable once ConditionIsAlwaysTrueOrFalse
            if (errorCode == 0 && myEMSLUploader.FileCountNew == 0 && myEMSLUploader.FileCountUpdated == 0)
            {
                return;
            }

            var uploadArgs = new MyEMSLUploadEventArgs(
                myEMSLUploader.FileCountNew, myEMSLUploader.FileCountUpdated,
                myEMSLUploader.Bytes, elapsedTime.TotalSeconds,
                myEMSLUploader.StatusURI, myEMSLUploader.EUSInfo,
                errorCode, useTestInstance);

            OnMyEMSLUploadComplete(uploadArgs);
        }

        /// <summary>
        /// Writes a log entry for a failed archive operation
        /// </summary>
        /// <param name="dsName">Name of dataset</param>
        /// <param name="reason">Reason for failed operation</param>
        /// <param name="logToDB">True to log to the database and a local file; false for local file only</param>
        private void LogOperationFailed(string dsName, string reason = "", bool logToDB = false)
        {
            var msg = string.Format("{0} failed, dataset {1}{2}",
                mArchiveOrUpdate, dsName, string.IsNullOrEmpty(reason) ? string.Empty : "; " + reason);

            if (logToDB)
            {
                LogTools.LogError(msg, null, true);
            }
            else
            {
                OnErrorEvent(msg);
            }
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

        public event EventHandler<MyEMSLUploadEventArgs> MyEMSLUploadComplete;

        private void LogStatusMessageSkipDuplicate(string message)
        {
            if (!string.Equals(message, mMostRecentLogMessage) || DateTime.UtcNow.Subtract(mMostRecentLogTime).TotalSeconds >= 60)
            {
                mMostRecentLogMessage = message;
                mMostRecentLogTime = DateTime.UtcNow;
                OnStatusEvent(message);
            }
        }

        private void myEMSLUploader_MetadataDefinedEvent(object sender, MessageEventArgs e)
        {
            if (mDebugLevel >= 5)
            {
                // e.Message contains the metadata JSON
                OnDebugEvent(e.Message);
            }
        }

        private void myEMSLUploader_StatusUpdate(object sender, StatusEventArgs e)
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
                else if (e.StatusMessage.StartsWith(TarStreamUploader.UPLOADING_FILES))
                {
                    verb = "uploading files";
                    filename = GetFilenameFromStatus(e.StatusMessage);
                }
                else
                {
                    verb = "processing";
                    filename = string.Empty;
                }

                // Example log message:
                // ... uploading files, 97.3% complete for 35,540 KB; QC_Shew_16-01_1_20Jul17_Merry_17-05-03_dta.zip

                var msgBase = string.Format(" ... {0}, {1:0.0}% complete", verb, e.PercentCompleted);

                var msg = e.TotalBytesToSend > 0
                    ? string.Format("{0} for {1:#,##0} KB", msgBase, e.TotalBytesToSend / 1024.0)
                    : msgBase;

                if (string.IsNullOrEmpty(filename))
                {
                    LogStatusMessageSkipDuplicate(msg);
                }
                else
                {
                    LogStatusMessageSkipDuplicate(msg + "; " + filename);
                }
            }

            if (DateTime.UtcNow.Subtract(mLastProgressUpdateTime).TotalSeconds >= 3 && e.PercentCompleted > 0)
            {
                mLastProgressUpdateTime = DateTime.UtcNow;
                mStatusTools.UpdateAndWrite((float)e.PercentCompleted);
            }
        }

        private void myEMSLUploader_UploadCompleted(object sender, UploadCompletedEventArgs e)
        {
            // Note that e.ServerResponse will simply have the StatusURL if the upload succeeded
            // If a problem occurred, e.ServerResponse will either have the full server response, or may even be blank

            OnDebugEvent("  ... MyEmsl upload task complete: {0}",
                string.IsNullOrEmpty(e.ServerResponse) ? "empty server response" : e.ServerResponse);

            mMyEmslUploadSuccess = true;
        }

        private void OnMyEMSLUploadComplete(MyEMSLUploadEventArgs e)
        {
            MyEMSLUploadComplete?.Invoke(this, e);
        }
    }
}
