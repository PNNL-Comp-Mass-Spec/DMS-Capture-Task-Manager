using System;
using System.Collections.Generic;
using System.IO;
using Pacifica.Core;
using PRISM;
using Utilities = Pacifica.Core.Utilities;

namespace Pacifica.DMS_Metadata
{
    public class MyEMSLUploader : clsEventNotifier
    {
        public const string RECURSIVE_UPLOAD = "MyEMSL_Recurse";

        public const string CRITICAL_UPLOAD_ERROR = "Critical Error";

        private DMSMetadataObject mMetadataContainer;
        private readonly Upload mUploadWorker;

        private readonly Dictionary<string, string> mMgrParams;
        private readonly Dictionary<string, string> mTaskParams;

        private readonly string mManagerName;

        private readonly Configuration mPacificaConfig;

        #region "Properties"

        /// <summary>
        /// Number of bytes uploaded
        /// </summary>
        public long Bytes
        {
            get;
            private set;
        }

        /// <summary>
        /// Critical error message, as reported by SetupMetadata in DMSMetadataObject
        /// </summary>
        public string CriticalErrorMessage { get; private set; }

        /// <summary>
        /// Error message from the MyEMSLUploader
        /// </summary>
        public string ErrorMessage
        {
            get
            {
                if (mUploadWorker == null)
                    return string.Empty;

                return mUploadWorker.ErrorMessage;
            }
        }

        /// <summary>
        /// EUS Info
        /// </summary>
        public Upload.EUSInfo EUSInfo
        {
            get;
            private set;
        }

        /// <summary>
        /// New files that were added
        /// </summary>
        public int FileCountNew
        {
            get;
            private set;
        }

        /// <summary>
        /// Existing files that were updated
        /// </summary>
        public int FileCountUpdated
        {
            get;
            private set;
        }

        /// <summary>
        /// DMS Metadata container
        /// </summary>
        public DMSMetadataObject MetadataContainer => mMetadataContainer;

        /// <summary>
        /// Status URI
        /// </summary>
        public string StatusURI
        {
            get;
            private set;
        }

        /// <summary>
        /// True to enable trace mode
        /// </summary>
        public bool TraceMode { get; set; }

        private bool mUseTestInstance;

        /// <summary>
        /// True to use the test instance
        /// </summary>
        public bool UseTestInstance
        {
            get => mUseTestInstance;
            set
            {
                mUseTestInstance = value;
                mUploadWorker.UseTestInstance = value;
                mPacificaConfig.UseTestInstance = value;
            }
        }

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="config">Pacifica configuration</param>
        /// <param name="mgrParams"></param>
        /// <param name="taskParams"></param>
        public MyEMSLUploader(Configuration config, Dictionary<string, string> mgrParams, Dictionary<string, string> taskParams)
        {
            mPacificaConfig = config;

            StatusURI = string.Empty;
            FileCountNew = 0;
            FileCountUpdated = 0;
            Bytes = 0;

            CriticalErrorMessage = string.Empty;

            EUSInfo = new Upload.EUSInfo();
            EUSInfo.Clear();

            mMgrParams = mgrParams;
            mTaskParams = taskParams;

            if (!mMgrParams.TryGetValue("MgrName", out mManagerName))
                mManagerName = "MyEMSLUploader_" + Environment.MachineName;

            var transferFolderPath = Utilities.GetDictionaryValue(mTaskParams, "TransferFolderPath", string.Empty);
            if (string.IsNullOrEmpty(transferFolderPath))
                throw new InvalidDataException("Job parameters do not have TransferFolderPath defined; unable to continue");

            var datasetName = Utilities.GetDictionaryValue(mTaskParams, "Dataset", string.Empty);
            if (string.IsNullOrEmpty(transferFolderPath))
                throw new InvalidDataException("Job parameters do not have Dataset defined; unable to continue");

            transferFolderPath = Path.Combine(transferFolderPath, datasetName);

            var jobNumber = Utilities.GetDictionaryValue(mTaskParams, "Job", string.Empty);
            if (string.IsNullOrEmpty(jobNumber))
                throw new InvalidDataException("Job parameters do not have Job defined; unable to continue");

            mUploadWorker = new Upload(config, transferFolderPath, jobNumber);
            RegisterEvents(mUploadWorker);

            // Attach the events
            mUploadWorker.MyEMSLOffline += myEmslUploadOnMyEmslOffline;
            mUploadWorker.StatusUpdate += myEMSLUpload_StatusUpdate;
            mUploadWorker.UploadCompleted += myEMSLUpload_UploadCompleted;

        }

        /// <summary>
        /// Look for files to upload, compute a Sha-1 hash for each, compare those hashes to existing files in MyEMSL,
        /// and upload new/changed files
        /// </summary>
        /// <param name="config"></param>
        /// <param name="debugMode">
        /// Set to eDebugMode.CreateTarLocal to authenticate with MyEMSL, then create a .tar file locally instead of actually uploading it
        /// Set to eDebugMode.MyEMSLOfflineMode to create the .tar file locally without contacting MyEMSL
        /// </param>
        /// <param name="statusURL">Output: status URL</param>
        /// <returns>True if success, false if an error</returns>
        public bool SetupMetadataAndUpload(Configuration config, EasyHttp.eDebugMode debugMode, out string statusURL)
        {

            var jobNumber = GetParam("Job", 0);

            var ignoreMyEMSLFileTrackingError = GetParam("IgnoreMyEMSLFileTrackingError", false);

            // Instantiate the metadata object
            mMetadataContainer = new DMSMetadataObject(config, mManagerName, jobNumber)
            {
                TraceMode = TraceMode,
                IgnoreMyEMSLFileTrackingError = ignoreMyEMSLFileTrackingError
            };

            // Attach the events
            RegisterEvents(mMetadataContainer);

            // Also process Progress Updates using _mdContainer_ProgressEvent, which triggers event StatusUpdate
            mMetadataContainer.ProgressUpdate += _mdContainer_ProgressEvent;

            mMetadataContainer.UseTestInstance = UseTestInstance;

            var certificateFilePath = EasyHttp.ResolveCertFile(mPacificaConfig, "SetupMetadataAndUpload", out var errorMessage);

            if (string.IsNullOrWhiteSpace(certificateFilePath))
            {
                throw new Exception(errorMessage);
            }

            try
            {

                // Look for files to upload, compute a Sha-1 hash for each, and compare those hashes to existing files in MyEMSL
                var success = mMetadataContainer.SetupMetadata(mTaskParams, mMgrParams, out var criticalError, out var criticalErrorMessage);

                if (!success)
                {
                    if (criticalError)
                        CriticalErrorMessage = criticalErrorMessage;

                    statusURL = criticalError ? CRITICAL_UPLOAD_ERROR : string.Empty;

                    return false;
                }

            }
            catch (Exception ex)
            {
                OnWarningEvent("Exception calling MetadataContainer.SetupMetadata: " + ex.Message);
                mMetadataContainer.DeleteLockFiles();
                throw;
            }

            // Send the metadata object to the calling procedure (in case it wants to log it)
            ReportMetadataDefined("StartUpload", mMetadataContainer.MetadataObjectJSON);

            mPacificaConfig.LocalTempDirectory = Utilities.GetDictionaryValue(mMgrParams, "workdir", string.Empty);
            FileCountUpdated = mMetadataContainer.TotalFileCountUpdated;
            FileCountNew = mMetadataContainer.TotalFileCountNew;
            Bytes = mMetadataContainer.TotalFileSizeToUpload;

            EUSInfo = mMetadataContainer.EUSInfo;

            var fileList = Utilities.GetFileListFromMetadataObject(mMetadataContainer.MetadataObject);
            if (fileList.Count == 0)
            {
                OnDebugEvent("File list is empty in StartUpload; nothing to do");
                statusURL = string.Empty;
                var e = new UploadCompletedEventArgs(string.Empty);
                UploadCompleted?.Invoke(this, e);
                return true;
            }

            mMetadataContainer.CreateLockFiles();

            bool uploadSuccess;

            try
            {
                uploadSuccess = mUploadWorker.StartUpload(mMetadataContainer.MetadataObject, debugMode, out statusURL);
            }
            catch (Exception ex)
            {
                OnWarningEvent("Exception calling UploadWorker.StartUpload: " + ex.Message);
                mMetadataContainer.DeleteLockFiles();
                throw;
            }

            mMetadataContainer.DeleteLockFiles();

            if (!string.IsNullOrEmpty(statusURL))
                StatusURI = statusURL;

            return uploadSuccess;
        }

        /// <summary>
        /// Gets a job parameter
        /// </summary>
        /// <param name="name">Parameter name</param>
        /// <param name="valueIfMissing">Value to return if the parameter does not exist</param>
        /// <returns>Parameter value if found, otherwise empty string</returns>
        private bool GetParam(string name, bool valueIfMissing)
        {
            if (mTaskParams.TryGetValue(name, out var valueText))
            {
                if (bool.TryParse(valueText, out var value))
                    return value;

                return valueIfMissing;
            }

            return valueIfMissing;
        }

        /// <summary>
        /// Gets a job parameter
        /// </summary>
        /// <param name="name">Parameter name</param>
        /// <param name="valueIfMissing">Value to return if the parameter does not exist</param>
        /// <returns>Parameter value if found, otherwise empty string</returns>
        private int GetParam(string name, int valueIfMissing)
        {
            if (mTaskParams.TryGetValue(name, out var valueText))
            {
                if (int.TryParse(valueText, out var value))
                    return value;

                return valueIfMissing;
            }

            return valueIfMissing;
        }

        #region "Events and Event Handlers"

        public event MessageEventHandler MetadataDefinedEvent;

        public event StatusUpdateEventHandler StatusUpdate;

        public event UploadCompletedEventHandler UploadCompleted;

        private void ReportMetadataDefined(string callingFunction, string metadataJSON)
        {
            var e = new MessageEventArgs(callingFunction, metadataJSON);

            MetadataDefinedEvent?.Invoke(this, e);
        }

        private void myEmslUploadOnMyEmslOffline(object sender, MessageEventArgs e)
        {
            OnWarningEvent("MyEMSL is offline; unable to retrieve data or upload files: " + e.Message);
        }

        void myEMSLUpload_StatusUpdate(object sender, StatusEventArgs e)
        {
            if (StatusUpdate != null)
            {
                // Multiplying by 0.25 because we're assuming 25% of the time is required for mMetadataContainer to compute the Sha-1 hashes of files to be uploaded while 75% of the time is required to create and upload the .tar file
                var percentCompleteOverall = 25 + e.PercentCompleted * 0.75;
                StatusUpdate(this, new StatusEventArgs(percentCompleteOverall, e.TotalBytesSent, e.TotalBytesToSend, e.StatusMessage));
            }
        }

        void myEMSLUpload_UploadCompleted(object sender, UploadCompletedEventArgs e)
        {
            UploadCompleted?.Invoke(this, e);
        }

        void _mdContainer_ProgressEvent(string progressMessage, float percentComplete)
        {
            if (StatusUpdate != null)
            {
                // Multiplying by 0.25 because we're assuming 25% of the time is required for mMetadataContainer to compute the Sha-1 hashes of files to be uploaded while 75% of the time is required to create and upload the .tar file
                var percentCompleteOverall = 0 + percentComplete * 0.25;
                StatusUpdate(this, new StatusEventArgs(percentCompleteOverall, 0, mMetadataContainer.TotalFileSizeToUpload, progressMessage));
            }

        }

        #endregion

    }
}
