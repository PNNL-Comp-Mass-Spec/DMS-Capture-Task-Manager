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
using System.IO.Compression;
using System.Linq;
using System.Threading;
using CaptureTaskManager;
using Pacifica.Core;
using Pacifica.DataUpload;
using Pacifica.DMSDataUpload;
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

        private const string PROCEDURE_NAME_MAKE_NEW_ARCHIVE_UPDATE_TASK = "make_new_archive_update_task";

        private readonly IMgrParams mMgrParams;
        protected readonly ITaskParams mTaskParams;
        private readonly IStatusFile mStatusTools;
        private readonly FileTools mFileTools;

        protected string mErrMsg = string.Empty;

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

            // This connection string points to the DMS database on prismdb2 (previously, DMS_Capture on Gigasax)
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
            var baseStoragePath =
                mTaskParams.GetParam(string.Equals(mMgrParams.GetParam("perspective"), "client", StringComparison.OrdinalIgnoreCase)
                ? "Storage_Vol_External"
                : "Storage_Vol");

            // Path to dataset on storage server
            var storagePath = mTaskParams.GetParam("Storage_Path");
            var datasetDirectory = mTaskParams.GetParam("Directory");

            var datasetDirectoryPath = Path.Combine(Path.Combine(baseStoragePath, storagePath, datasetDirectory));

            if (string.IsNullOrWhiteSpace(datasetDirectoryPath))
            {
                mErrMsg = "Dataset directory path in datasetDirectoryPath is empty";

                if (string.IsNullOrWhiteSpace(baseStoragePath))
                {
                    mErrMsg = CTMUtilities.AppendToString(mErrMsg, "baseStoragePath is empty");
                }

                if (string.IsNullOrWhiteSpace(storagePath))
                {
                    mErrMsg = CTMUtilities.AppendToString(mErrMsg, "storagePath is empty");
                }

                if (string.IsNullOrWhiteSpace(datasetDirectory))
                {
                    mErrMsg = CTMUtilities.AppendToString(mErrMsg, "datasetDirectory is empty");
                }

                OnErrorEvent(mErrMsg);
                LogOperationFailed(mDatasetName, string.Empty, true);
                return false;
            }

            // Verify dataset is in specified location
            if (!VerifyDSPresent(datasetDirectoryPath))
            {
                mErrMsg = "Dataset directory " + datasetDirectoryPath + " not found";
                OnErrorEvent(mErrMsg);
                LogOperationFailed(mDatasetName, string.Empty, true);
                return false;
            }

            // Look for method subdirectories at the same level with identical files
            // If found, zip the directories with duplicate files
            // This is required to avoid .tar file length validation errors from MyEMSL
            return CompressDuplicateMethodDirectories(datasetDirectoryPath);
        }

        private bool CompressDuplicateMethodDirectories(string datasetDirectoryPath)
        {
            var currentTask = "Initializing";

            try
            {
                currentTask = "Finding .m directories";

                var datasetDirectory = new DirectoryInfo(datasetDirectoryPath);

                var methodDirectories = datasetDirectory.GetDirectories("*.m", SearchOption.AllDirectories);

                if (methodDirectories.Length == 0)
                    return true;

                var workDir = mMgrParams.GetParam("WorkDir");

                var zipTools = new ZipFileTools(mDebugLevel, workDir);
                RegisterEvents(zipTools);

                var processedDirectories = new SortedSet<string>();

                currentTask = "Examining .m directories";

                foreach (var methodDirectory in methodDirectories)
                {
                    if (processedDirectories.Contains(methodDirectory.FullName))
                        continue;

                    var methodSubDirectories = methodDirectory.GetDirectories("*.m", SearchOption.TopDirectoryOnly);

                    if (methodSubDirectories.Length < 2)
                        continue;

                    var sortedDirectories = (from item in methodSubDirectories orderby item.Name select item).ToList();

                    // Keys in this directory are relative file paths, values are FileInfo instances
                    var baseSubDirectoryFiles = new Dictionary<string, FileInfo>(StringComparer.OrdinalIgnoreCase);

                    var baseSubdirectory = sortedDirectories[0];

                    foreach (var file in baseSubdirectory.GetFiles("*", SearchOption.AllDirectories))
                    {
                        baseSubDirectoryFiles.Add(GetRelativePath(baseSubdirectory, file), file);
                    }

                    currentTask = "Comparing .m directories";

                    for (var i = 1; i < sortedDirectories.Count; i++)
                    {
                        var currentSubdirectory = sortedDirectories[i];

                        var compressSubDirectory = false;

                        foreach (var file in currentSubdirectory.GetFiles("*", SearchOption.AllDirectories))
                        {
                            var relativePath = GetRelativePath(currentSubdirectory, file);

                            if (!baseSubDirectoryFiles.TryGetValue(relativePath, out var baseFileInfo))
                            {
                                continue;
                            }

                            if (file.Length != baseFileInfo.Length)
                                continue;

                            LogTools.LogMessage("Compressing directory {0} since file {1} has the same name and size as file {2}",
                                currentSubdirectory.Name,
                                relativePath,
                                GetRelativePath(methodDirectory, baseFileInfo));

                            compressSubDirectory = true;
                            break;
                        }

                        if (!compressSubDirectory)
                        {
                            continue;
                        }

                        currentTask = "Copying files from " + currentSubdirectory.Name;

                        var localSubdirectoryPath = Path.Combine(workDir, currentSubdirectory.Name);

                        // Copy the files locally to prevent error "Could not find a part of the path" due to long path lengths
                        mFileTools.CopyDirectory(currentSubdirectory.FullName, localSubdirectoryPath, true);

                        var zipFilePath = string.Format("{0}.zip", Path.Combine(workDir, currentSubdirectory.Name));

                        currentTask = "Zipping " + localSubdirectoryPath;

                        zipTools.ZipDirectory(localSubdirectoryPath, zipFilePath);

                        currentTask = "Validating " + zipFilePath;

                        // Confirm that the zip file was created and has the correct number of files
                        var zipFile = new FileInfo(zipFilePath);

                        if (!zipFile.Exists)
                        {
                            mErrMsg = string.Format("Zip file not created for directory {0}; aborting {1}", currentSubdirectory.FullName, mArchiveOrUpdate);
                            LogOperationFailed(mDatasetName, mErrMsg, true);
                            return false;
                        }

                        var validZipFile = zipTools.VerifyZipFile(zipFile.FullName);

                        if (!validZipFile)
                        {
                            mErrMsg = string.Format("Corrupt zip file created for directory {0}; aborting {1}", currentSubdirectory.FullName, mArchiveOrUpdate);
                            LogOperationFailed(mDatasetName, mErrMsg, true);
                            return false;
                        }

                        currentTask = "Comparing file counts in " + zipFilePath;

                        using (var zipArchive = ZipFile.OpenRead(zipFilePath))
                        {
                            var expectedFileCount = currentSubdirectory.GetFiles("*").Length;
                            var zippedFileCount = zipArchive.Entries.Count;

                            if (zippedFileCount < expectedFileCount)
                            {
                                mErrMsg = string.Format("Zip file created for directory {0} only has {1}, but expecting {2}; aborting {3}",
                                    currentSubdirectory.FullName, zippedFileCount, expectedFileCount, mArchiveOrUpdate);

                                LogOperationFailed(mDatasetName, mErrMsg, true);
                                return false;
                            }
                        }

                        currentTask = string.Format("Copying {0} to {1}", zipFile.Name, currentSubdirectory.FullName);

                        // Copy the zip file to the server
                        var remoteZipFile = new FileInfo(Path.Combine(currentSubdirectory.FullName, zipFile.Name));

                        mFileTools.CopyFile(zipFile.FullName, remoteZipFile.FullName, true);

                        currentTask = "Deleting files in " + currentSubdirectory.FullName;

                        // Delete the files, then copy the .zip file
                        foreach (var file in currentSubdirectory.GetFiles("*", SearchOption.AllDirectories))
                        {
                            if (file.Name.Equals(zipFile.Name))
                                continue;

                            FileTools.DeleteFile(file.FullName);
                        }

                        currentTask = "Verifying that remote zip file exists at " + remoteZipFile.FullName;

                        if (!NativeIOFileTools.Exists(remoteZipFile.FullName))
                        {
                            // The remote .zip file was deleted; this is unexpected, but we can copy it again
                            mFileTools.CopyFile(zipFile.FullName, remoteZipFile.FullName, true);
                        }

                        currentTask = "Deleting local zip file at " + zipFile.FullName;

                        zipFile.Delete();

                        currentTask = "Deleting local files in " + localSubdirectoryPath;
                        mFileTools.DeleteDirectory(localSubdirectoryPath, true);

                        processedDirectories.Add(currentSubdirectory.FullName);
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                mErrMsg = string.Format("Exception in CompressDuplicateMethodDirectories ({0}): {1}; aborting {2}", currentTask, ex.Message, mArchiveOrUpdate);
                LogOperationFailed(mDatasetName, mErrMsg, true);
                return false;
            }
        }

        private void CreateArchiveUpdateJobsForDataset(List<string> subdirectoryNames)
        {
            var failureMsg = "Error creating archive update tasks for dataset " + mDatasetName + "; " +
                             "need to create ArchiveUpdate tasks for subdirectories " +
                             string.Join(", ", subdirectoryNames);

            try
            {
                // Setup for calling the procedure
                var dbTools = mCaptureDbProcedureExecutor;

                var dbServerType = DbToolsFactory.GetServerTypeFromConnectionString(dbTools.ConnectStr);

                var cmd = dbTools.CreateCommand(PROCEDURE_NAME_MAKE_NEW_ARCHIVE_UPDATE_TASK, CommandType.StoredProcedure);

                // Define parameter for procedure's return value
                // If querying a Postgres DB, dbTools will auto-change "@return" to "_returnCode"
                var returnParam = dbTools.AddParameter(cmd, "@Return", SqlType.Int, ParameterDirection.ReturnValue);

                dbTools.AddParameter(cmd, "@datasetName", SqlType.VarChar, 128, mDatasetName);
                var resultsDirectoryParam = dbTools.AddParameter(cmd, "@resultsDirectoryName", SqlType.VarChar, 128);

                if (dbServerType == DbServerTypes.PostgreSQL)
                {
                    dbTools.AddTypedParameter(cmd, "@allowBlankResultsDirectory", SqlType.Boolean, value: false);
                    dbTools.AddTypedParameter(cmd, "@infoOnly", SqlType.Boolean, value: false);
                }
                else
                {
                    dbTools.AddTypedParameter(cmd, "@allowBlankResultsDirectory", SqlType.TinyInt, value: 0);
                    dbTools.AddTypedParameter(cmd, "@infoOnly", SqlType.TinyInt, value: 0);
                }

                var messageParam = dbTools.AddParameter(cmd, "@message", SqlType.VarChar, 512, ParameterDirection.InputOutput);

                var successCount = 0;

                foreach (var subdirectoryName in subdirectoryNames)
                {
                    resultsDirectoryParam.Value = subdirectoryName;

                    var datasetAndDirectory = string.Format("dataset {0}, subdirectory {1}", mDatasetName, subdirectoryName);

                    LogTools.LogMessage("Creating archive update job for " + datasetAndDirectory);

                    // Call the procedure (retry the call, up to 4 times)
                    mCaptureDbProcedureExecutor.ExecuteSP(cmd, 4);

                    var returnCode = DBToolsBase.GetReturnCode(returnParam);

                    if (returnCode == 0)
                    {
                        LogTools.LogDebug("Job successfully created");
                        successCount++;
                    }
                    else
                    {
                        var outputMessage = messageParam.Value.CastDBVal<string>();
                        var message = string.IsNullOrWhiteSpace(outputMessage) ? "Unknown error" : outputMessage;

                        LogTools.LogWarning(
                            "Unable to create archive update job for {0}; procedure {1} returned resultCode {2}; message: {3}",
                            datasetAndDirectory, cmd.CommandText, returnParam.Value.CastDBVal<string>(), message);
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
        /// Return the text after the colon in statusMessage
        /// </summary>
        /// <param name="statusMessage">Status message</param>
        /// <returns>Text if the colon was found, otherwise an empty string</returns>
        private static string GetFilenameFromStatus(string statusMessage)
        {
            var colonIndex = statusMessage.IndexOf(':');

            if (colonIndex >= 0)
            {
                return statusMessage.Substring(colonIndex + 1).Trim();
            }

            return string.Empty;
        }

        // ReSharper disable SuggestBaseTypeForParameter

        /// <summary>
        /// Return the relative path of the given file, with relation to the given parent directory (not including the parent directory name)
        /// </summary>
        /// <param name="parentDirectory">Parent directory</param>
        /// <param name="file">File (which should reside either in the parent directory, or in a subdirectory below the parent directory))</param>
        /// <returns>Relative file path</returns>
        private static string GetRelativePath(DirectoryInfo parentDirectory, FileInfo file)
        {
            if (!file.FullName.StartsWith(parentDirectory.FullName))
                return file.FullName;

            if (file.FullName.Equals(parentDirectory.FullName))
                return file.Name;

            return file.FullName.Substring(parentDirectory.FullName.Length + 1);
        }

        // ReSharper restore SuggestBaseTypeForParameter

        /// <summary>
        /// Perform several tasks after the MyEMSL Uploader raises an exception
        /// </summary>
        /// <param name="ex">Exception</param>
        /// <param name="myEMSLUploader">Uploader instance</param>
        /// <param name="startTime">Start time</param>
        /// <param name="useTestInstance">When true, use the test instance</param>
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

            // ReSharper disable once ConvertIfStatementToSwitchStatement
            if (!success)
            {
                WarningMsg =
                    CTMUtilities.AppendToString(WarningMsg, debugMode != TarStreamUploader.UploadDebugMode.DebugDisabled
                    ? "Debug mode was enabled; thus, .tar file was created locally and not uploaded to MyEMSL"
                    : "UploadToMyEMSL reports False");
            }

            if (success && !mMyEmslUploadSuccess)
            {
                WarningMsg = CTMUtilities.AppendToString(WarningMsg,
                    "UploadToMyEMSL reports True but mMyEmslUploadSuccess is False");
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

                myEMSLUploader.ZeroByteFileEvent += myEMSLUploader_ZeroByteFileEvent;

                mTaskParams.AddAdditionalParameter(MyEMSLUploader.RECURSIVE_UPLOAD, recurse.ToString());

                // Cache the operator name; used in the exception handler below
                operatorUsername = mTaskParams.GetParam("Operator_Username", "Unknown_Operator");

                config.UseTestInstance = useTestInstance;

                if (useTestInstance)
                {
                    myEMSLUploader.UseTestInstance = true;

                    statusMessage = "Sending dataset to MyEMSL test instance";
                    OnStatusEvent(statusMessage);
                }

                // Note: If the dataset directory has over 15 GB of data (including subdirectories), only the base directory and the QC directory will be pushed
                // Other subdirectories need to be pushed with separate ArchiveUpdate tasks

                // Start the upload: the uploader will compile a list of files to upload, then push them into MyEMSL
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

                CreateArchiveUpdateJobsForDataset(skippedSubdirectories);

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

                var jobNumber = mTaskParams.GetParam("Job", 0);

                if (ex.Message.Contains(DMSMetadataObject.UNDEFINED_EUS_OPERATOR_ID))
                {
                    mErrMsg += string.Format("; operator not defined in EUS. " +
                                             "Have {0} login to " + DMSMetadataObject.EUS_PORTAL_URL + " " +
                                             "then wait for T_EUS_Users to update, " +
                                             "then update job parameters using " +
                                             "Call cap.update_parameters_for_task(_jobList => '{1}');", operatorUsername, jobNumber);

                    // Do not retry the upload; it will fail again due to the same error
                    allowRetry = false;
                    LogOperationFailed(mDatasetName, ex.Message, true);
                }
                else if (ex.Message.Contains(DMSMetadataObject.TOO_MANY_FILES_TO_ARCHIVE))
                {
                    mErrMsg += ": " + DMSMetadataObject.TOO_MANY_FILES_TO_ARCHIVE;

                    mErrMsg += string.Format(
                        " ; to ignore this error, use Call cap.add_update_task_parameter ({0}, 'StepParameters', 'IgnoreMaxFileLimit', '1');", jobNumber);

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
                else if (ex.Message.IndexOf("Bytes to be written to the stream exceed the Content-Length bytes size specified", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    // This has been observed as an intermittent error
                    // Do not immediately retry the upload; it will fail again due to the same error and may lock up the capture task manager
                    allowRetry = false;
                    LogOperationFailed(mDatasetName, ex.Message, true);
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
                    myEMSLUploader.ZeroByteFileEvent -= myEMSLUploader_ZeroByteFileEvent;
                }
            }
        }

        /// <summary>
        /// Verifies that the specified dataset directory exists
        /// </summary>
        /// <param name="datasetDirectoryPath">Fully qualified path to dataset directory</param>
        /// <returns>True if dataset directory is present; otherwise false</returns>
        private static bool VerifyDSPresent(string datasetDirectoryPath)
        {
            return Directory.Exists(datasetDirectoryPath);
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

        private void myEMSLUploader_ZeroByteFileEvent(FileInfo dataFile, string message)
        {
            const string ZERO_BYTE_FILE_FOUND = "Zero byte file found";

            OnWarningEvent(message);

            if (WarningMsg.Contains(ZERO_BYTE_FILE_FOUND))
                return;

            // Zero byte file found and skipped
            var warningMessage = string.Format("{0} and skipped: {1}", ZERO_BYTE_FILE_FOUND, dataFile.Name);

            WarningMsg = CTMUtilities.AppendToString(WarningMsg, warningMessage);
        }

        private void OnMyEMSLUploadComplete(MyEMSLUploadEventArgs e)
        {
            MyEMSLUploadComplete?.Invoke(this, e);
        }
    }
}
