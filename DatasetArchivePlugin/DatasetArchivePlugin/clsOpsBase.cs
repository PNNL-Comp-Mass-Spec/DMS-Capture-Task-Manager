
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 10/20/2009
//
//*********************************************************************************************************

using System;
using System.Threading;
using CaptureTaskManager;
using Pacifica.Core;
using Pacifica.DMS_Metadata;
using PRISM;
using System.IO;

namespace DatasetArchivePlugin
{
    class clsOpsBase : clsEventNotifier, IArchiveOps
    {
        //*********************************************************************************************************
        // Base class for archive and archive update operations classes. This class should always be overridden.
        //**********************************************************************************************************

        #region "Constants"
        private const string ARCHIVE = "Archive ";
        private const string UPDATE = "Archive update ";

        #endregion

        #region "Class variables"

        private readonly IMgrParams m_MgrParams;
        protected ITaskParams m_TaskParams;
        private readonly IStatusFile m_StatusTools;

        protected string m_ErrMsg = string.Empty;
        private string m_WarningMsg = string.Empty;
        private string m_DSNamePath;

        private bool m_MyEmslUploadSuccess;

        private readonly int m_DebugLevel;

        private readonly string m_ArchiveOrUpdate;
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

        public clsOpsBase(IMgrParams mgrParams, ITaskParams taskParams, IStatusFile statusTools)
        {
            m_MgrParams = mgrParams;
            m_TaskParams = taskParams;
            m_StatusTools = statusTools;

            // DebugLevel of 4 means Info level (normal) logging; 5 for Debug level (verbose) logging
            m_DebugLevel = m_MgrParams.GetParam("debuglevel", 4);

            if (m_TaskParams.GetParam("StepTool") == "DatasetArchive")
            {
                m_ArchiveOrUpdate = ARCHIVE;
            }
            else
            {
                m_ArchiveOrUpdate = UPDATE;
            }

            TraceMode = m_MgrParams.GetParam("TraceMode", false);
        }

        #endregion

        #region "Methods"

        public static bool OnlyUseMyEMSL(string instrumentName)
        {
            /*
            var lstExclusionPrefix = new List<string>();

            //lstExclusionPrefix.Add("DMS_Pipeline_Data");
            //                  .Add("QExact");
            //                  .Add("QTrap");
            //                  .Add("VOrbi05");
            //                  .Add("VOrbiETD03");

            foreach (string prefix in lstExclusionPrefix)
            {
                if (instrumentName.StartsWith(prefix))
                    return false;
            }
            */

            return true;

        }

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
            var bSuccess = false;
            var iAttempts = 0;

            allowRetry = true;
            m_MyEmslUploadSuccess = false;
            criticalErrorMessage = string.Empty;

            if (maxAttempts < 1)
                maxAttempts = 1;

            while (!bSuccess && iAttempts < maxAttempts)
            {
                iAttempts += 1;

                Console.WriteLine("Uploading files for " + m_DatasetName + " to MyEMSL; attempt=" + iAttempts);

                bSuccess = UploadToMyEMSL(recurse, debugMode, useTestInstance, out allowRetry, out criticalErrorMessage);

                if (!allowRetry)
                    break;

                if (debugMode != EasyHttp.eDebugMode.DebugDisabled)
                    break;

                if (!bSuccess && iAttempts < maxAttempts)
                {
                    // Wait 5 seconds, then retry
                    Thread.Sleep(5000);

                    mLastStatusUpdateTime = DateTime.UtcNow;
                }
            }

            if (!bSuccess)
            {
                if (debugMode != EasyHttp.eDebugMode.DebugDisabled)
                    m_WarningMsg = "Debug mode was enabled; thus, .tar file was created locally and not uploaded to MyEMSL";
                else
                    m_WarningMsg = AppendToString(m_WarningMsg, "UploadToMyEMSL reports False");
            }

            if (bSuccess && !m_MyEmslUploadSuccess)
                m_WarningMsg = AppendToString(m_WarningMsg, "UploadToMyEMSL reports True but m_MyEmslUploadSuccess is False");

            return bSuccess && m_MyEmslUploadSuccess;
        }

        /// <summary>
        /// Use MyEMSLUploader to upload the data to MyEMSL, trying up to maxAttempts times
        /// </summary>
        /// <param name="recurse">True to find files in all subdirectories</param>
        /// <param name="debugMode">Debug mode options</param>
        /// <param name="useTestInstance">True to use the test instance</param>
        /// <param name="allowRetry">Output: whether the upload should be retried if it failed</param>
        /// <param name="criticalErrorMessage">Output: critical error message</param>
        /// <returns>True if success, false if an error</returns>
        private bool UploadToMyEMSL(bool recurse, EasyHttp.eDebugMode debugMode, bool useTestInstance, out bool allowRetry, out string criticalErrorMessage)
        {
            var dtStartTime = DateTime.UtcNow;

            MyEMSLUploader myEMSLUL = null;
            var operatorUsername = "??";

            allowRetry = true;
            criticalErrorMessage = string.Empty;

            try
            {
                var statusMessage = "Bundling changes to dataset " + m_DatasetName + " for transmission to MyEMSL";
                OnStatusEvent(statusMessage);

                var config = new Configuration();

                myEMSLUL = new MyEMSLUploader(config, m_MgrParams.TaskDictionary, m_TaskParams.TaskDictionary) {
                    TraceMode = TraceMode
                };

                // Attach the events
                RegisterEvents(myEMSLUL);

                myEMSLUL.StatusUpdate += myEMSLUL_StatusUpdate;
                myEMSLUL.UploadCompleted += myEMSLUL_UploadCompleted;

                myEMSLUL.MetadataDefinedEvent += myEMSLUL_MetadataDefinedEvent;

                m_TaskParams.AddAdditionalParameter(MyEMSLUploader.RECURSIVE_UPLOAD, recurse.ToString());

                // Cache the operator name; used in the exception handler below
                operatorUsername = m_TaskParams.GetParam("Operator_PRN", "Unknown_Operator");

                string statusURL;

                config.UseTestInstance = useTestInstance;

                if (useTestInstance)
                {
                    myEMSLUL.UseTestInstance = true;

                    statusMessage = "Sending dataset to MyEMSL test instance";
                    OnStatusEvent(statusMessage);
                }

                // Start the upload
                var success = myEMSLUL.SetupMetadataAndUpload(config, debugMode, out statusURL);

                criticalErrorMessage = myEMSLUL.CriticalErrorMessage;

                if (!string.IsNullOrWhiteSpace(criticalErrorMessage) || string.Equals(statusURL, MyEMSLUploader.CRITICAL_UPLOAD_ERROR))
                {
                    allowRetry = false;
                }

                var tsElapsedTime = DateTime.UtcNow.Subtract(dtStartTime);

                statusMessage = "Upload of " + m_DatasetName + " completed in " + tsElapsedTime.TotalSeconds.ToString("0.0") + " seconds";
                if (!success)
                    statusMessage += " (success=false)";

                statusMessage += ": " + myEMSLUL.FileCountNew + " new files, " + myEMSLUL.FileCountUpdated + " updated files, " + myEMSLUL.Bytes + " bytes";

                OnStatusEvent(statusMessage);

                if (debugMode != EasyHttp.eDebugMode.DebugDisabled)
                    return false;

                statusMessage = "myEMSL statusURI => " + myEMSLUL.StatusURI;

                if (!string.IsNullOrEmpty(statusURL) && statusURL.EndsWith("/1323420608"))
                {
                    statusMessage += "; this indicates an upload error (transactionID=-1)";
                    OnErrorEvent(statusMessage);
                    return false;
                }

                if (myEMSLUL.FileCountNew + myEMSLUL.FileCountUpdated > 0 || !string.IsNullOrEmpty(statusURL))
                    OnStatusEvent(statusMessage);

                // Raise an event with the stats
                // This will cause clsPluginMain to call StoreMyEMSLUploadStats to store the results in the database (Table T_MyEmsl_Uploads)
                // If an error occurs while storing to the database, the status URI will be listed in the manager's local log file
                var e = new MyEMSLUploadEventArgs(
                    myEMSLUL.FileCountNew, myEMSLUL.FileCountUpdated,
                    myEMSLUL.Bytes, tsElapsedTime.TotalSeconds,
                    statusURL, myEMSLUL.EUSInfo,
                    iErrorCode: 0, usedTestInstance: useTestInstance);

                OnMyEMSLUploadComplete(e);

                m_StatusTools.UpdateAndWrite(100);

                return success;

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
                else
                {
                    LogOperationFailed(m_DatasetName);
                }

                // Raise an event with the stats, though only if errorCode is non-zero or FileCountNew or FileCountUpdated are positive

                var errorCode = ex.Message.GetHashCode();
                if (errorCode == 0)
                    errorCode = 1;

                var tsElapsedTime = DateTime.UtcNow.Subtract(dtStartTime);

                if (myEMSLUL == null)
                {
                    if (errorCode == 0)
                        return false;

                    var eusInfo = new Upload.EUSInfo();
                    eusInfo.Clear();

                    var emptyArgs = new MyEMSLUploadEventArgs(
                        0, 0,
                        0, tsElapsedTime.TotalSeconds,
                        string.Empty, eusInfo,
                        errorCode, useTestInstance);

                    OnMyEMSLUploadComplete(emptyArgs);
                    return false;
                }

                // Exit this method (skipping the call to MyEMSLUploadEventArgs) if the error code is 0 and no files were added or updated
                if (errorCode == 0 && myEMSLUL.FileCountNew == 0 && myEMSLUL.FileCountUpdated == 0)
                    return false;

                var uploadArgs = new MyEMSLUploadEventArgs(
                    myEMSLUL.FileCountNew, myEMSLUL.FileCountUpdated,
                    myEMSLUL.Bytes, tsElapsedTime.TotalSeconds,
                    myEMSLUL.StatusURI, myEMSLUL.EUSInfo,
                    errorCode, useTestInstance);

                OnMyEMSLUploadComplete(uploadArgs);

                return false;
            }

        }

        /// <summary>
        /// Verifies specified dataset is present
        /// </summary>
        /// <param name="dsNamePath">Fully qualified path to dataset folder</param>
        /// <returns>TRUE if dataset folder is present; otherwise FALSE</returns>
        private bool VerifyDSPresent(string dsNamePath)
        {
            // Verifies specified dataset is present
            return Directory.Exists(dsNamePath);

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
                clsUtilities.LogError(msg, true);
            else
                OnErrorEvent(msg);
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

        void myEMSLUL_MetadataDefinedEvent(object sender, MessageEventArgs e)
        {
            if (m_DebugLevel >= 5)
            {
                // e.Message contains the metadata JSON
                OnDebugEvent(e.Message);
            }
        }

        void myEMSLUL_StatusUpdate(object sender, StatusEventArgs e)
        {

            if (DateTime.UtcNow.Subtract(mLastStatusUpdateTime).TotalSeconds >= 60 && e.PercentCompleted > 0)
            {
                mLastStatusUpdateTime = DateTime.UtcNow;
                var msg = "  ... uploading, " + e.PercentCompleted.ToString("0.0") + "% complete for " + (e.TotalBytesToSend / 1024.0).ToString("#,##0") + " KB";
                if (!string.IsNullOrEmpty(e.StatusMessage))
                    msg += "; " + e.StatusMessage;

                LogStatusMessageSkipDuplicate(msg);
            }

            if (DateTime.UtcNow.Subtract(mLastProgressUpdateTime).TotalSeconds >= 3 && e.PercentCompleted > 0)
            {
                mLastProgressUpdateTime = DateTime.UtcNow;
                m_StatusTools.UpdateAndWrite((float)e.PercentCompleted);
            }

        }

        void myEMSLUL_UploadCompleted(object sender, UploadCompletedEventArgs e)
        {
            var msg = "  ... MyEmsl upload task complete";

            // Note that e.ServerResponse will simply have the StatusURL if the upload succeeded
            // If a problem occurred, then e.ServerResponse will either have the full server reponse, or may even be blank
            if (string.IsNullOrEmpty(e.ServerResponse))
                msg += ": empty server reponse";
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
