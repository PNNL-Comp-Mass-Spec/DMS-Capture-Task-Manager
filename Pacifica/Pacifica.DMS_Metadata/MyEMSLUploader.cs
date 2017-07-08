using System;
using System.Collections.Generic;
using System.IO;
using Pacifica.Core;

namespace Pacifica.DMS_Metadata
{
    public class MyEMSLUploader
    {
        public const string RECURSIVE_UPLOAD = "MyEMSL_Recurse";

        public const string CRITICAL_UPLOAD_ERROR = "Critical Error";

        private DMSMetadataObject _mdContainer;
        private readonly Upload myEMSLUpload;

        private readonly Dictionary<string, string> m_MgrParams;
        private readonly Dictionary<string, string> m_TaskParams;

        private readonly string mManagerName;

        private readonly Configuration mPacificaConfig;

        /// <summary>
        /// Critical error message, as reported by SetupMetadata in DMSMetadataObject
        /// </summary>
        public string CriticalErrorMessage { get; private set; }
        public string ErrorMessage
        {
            get
            {
                if (myEMSLUpload == null)
                    return string.Empty;

                return myEMSLUpload.ErrorMessage;
            }
        }

        private bool mUseTestInstance;
        public bool UseTestInstance
        {
            get => mUseTestInstance;
            set
            {
                mUseTestInstance = value;
                myEMSLUpload.UseTestInstance = value;
                mPacificaConfig.UseTestInstance = value;
            }
        }

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
            ErrorCode = string.Empty;

            CriticalErrorMessage = string.Empty;

            EUSInfo = new Upload.EUSInfo();
            EUSInfo.Clear();

            m_MgrParams = mgrParams;
            m_TaskParams = taskParams;

            if (!m_MgrParams.TryGetValue("MgrName", out mManagerName))
                mManagerName = "MyEMSLUploader_" + Environment.MachineName;

            var transferFolderPath = Utilities.GetDictionaryValue(m_TaskParams, "TransferFolderPath", string.Empty);
            if (string.IsNullOrEmpty(transferFolderPath))
                throw new InvalidDataException("Job parameters do not have TransferFolderPath defined; unable to continue");

            var datasetName = Utilities.GetDictionaryValue(m_TaskParams, "Dataset", string.Empty);
            if (string.IsNullOrEmpty(transferFolderPath))
                throw new InvalidDataException("Job parameters do not have Dataset defined; unable to continue");

            transferFolderPath = Path.Combine(transferFolderPath, datasetName);

            var jobNumber = Utilities.GetDictionaryValue(m_TaskParams, "Job", string.Empty);
            if (string.IsNullOrEmpty(jobNumber))
                throw new InvalidDataException("Job parameters do not have Job defined; unable to continue");

            myEMSLUpload = new Upload(config, transferFolderPath, jobNumber);

            // Attach the events
            myEMSLUpload.DebugEvent += myEMSLUpload_DebugEvent;
            myEMSLUpload.ErrorEvent += myEMSLUpload_ErrorEvent;
            myEMSLUpload.StatusUpdate += myEMSLUpload_StatusUpdate;
            myEMSLUpload.UploadCompleted += myEMSLUpload_UploadCompleted;

        }

        #region "Properties"

        public string StatusURI
        {
            get;
            private set;
        }

        public int FileCountNew
        {
            get;
            private set;
        }

        public int FileCountUpdated
        {
            get;
            private set;
        }

        public long Bytes
        {
            get;
            private set;
        }

        public string ErrorCode
        {
            get;
            private set;
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
        /// True to enable trace mode
        /// </summary>
        public bool TraceMode { get; set; }

        #endregion

        public bool SetupMetadataAndUpload(Configuration config, EasyHttp.eDebugMode debugMode, out string statusURL)
        {

            var jobNumber = GetParam("Job", 0);

            var ignoreMyEMSLFileTrackingError = GetParam("IgnoreMyEMSLFileTrackingError", false);

            // Instantiate the metadata object
            _mdContainer = new DMSMetadataObject(config, mManagerName, jobNumber) {
                TraceMode = TraceMode,
                IgnoreMyEMSLFileTrackingError = ignoreMyEMSLFileTrackingError
            };

            // Attach the events
            _mdContainer.ProgressEvent += _mdContainer_ProgressEvent;
            _mdContainer.DebugEvent += myEMSLUpload_DebugEvent;
            _mdContainer.ErrorEvent += myEMSLUpload_ErrorEvent;
            _mdContainer.WarningEvent += myEMSLUpload_WarningEvent;

            _mdContainer.UseTestInstance = UseTestInstance;

            if (!File.Exists(Configuration.CLIENT_CERT_FILEPATH))
            {
                throw new Exception("Authentication failure; MyEMSL certificate file not found at " + Configuration.CLIENT_CERT_FILEPATH);
            }

            // Look for files to upload, compute a Sha-1 hash for each, and compare those hashes to existing files in MyEMSL
            var success = _mdContainer.SetupMetadata(m_TaskParams, m_MgrParams, debugMode, out var criticalError, out var criticalErrorMessage);

            if (!success)
            {
                if (criticalError)
                    CriticalErrorMessage = criticalErrorMessage;

                statusURL = criticalError ? CRITICAL_UPLOAD_ERROR : string.Empty;

                return false;
            }

            // Send the metadata object to the calling procedure (in case it wants to log it)
            ReportMetadataDefined("StartUpload", _mdContainer.MetadataObjectJSON);

            mPacificaConfig.LocalTempDirectory = Utilities.GetDictionaryValue(m_MgrParams, "workdir", string.Empty);
            FileCountUpdated = _mdContainer.TotalFileCountUpdated;
            FileCountNew = _mdContainer.TotalFileCountNew;
            Bytes = _mdContainer.TotalFileSizeToUpload;

            EUSInfo = _mdContainer.EUSInfo;

            var fileList = Utilities.GetFileListFromMetadataObject(_mdContainer.MetadataObject);
            if (fileList.Count == 0)
            {
                OnDebugEvent("StartUpload", "File list is empty; nothing to do");
                statusURL = string.Empty;
                var e = new UploadCompletedEventArgs(string.Empty);
                UploadCompleted?.Invoke(this, e);
                return true;
            }

            _mdContainer.CreateLockFiles();

            bool uploadSuccess;

            try
            {
                uploadSuccess = myEMSLUpload.StartUpload(_mdContainer.MetadataObject, debugMode, out statusURL);
            }
            catch (Exception ex)
            {
                Console.WriteLine("MyEMSL Upload exception: " + ex.Message);
                _mdContainer.DeleteLockFiles();
                throw;
            }

            _mdContainer.DeleteLockFiles();

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
            if (m_TaskParams.TryGetValue(name, out var valueText))
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
            if (m_TaskParams.TryGetValue(name, out var valueText))
            {
                if (int.TryParse(valueText, out var value))
                    return value;

                return valueIfMissing;
            }

            return valueIfMissing;
        }

        #region "Events and Event Handlers"

        public event DebugEventHandler DebugEvent;
        public event DebugEventHandler ErrorEvent;
        public event DebugEventHandler WarningEvent;

        public event MessageEventHandler MetadataDefinedEvent;

        public event StatusUpdateEventHandler StatusUpdate;
        public event UploadCompletedEventHandler UploadCompleted;

        private void OnDebugEvent(string callingFunction, string debugMessage)
        {
            var e = new MessageEventArgs(callingFunction, debugMessage);
            DebugEvent?.Invoke(this, e);
        }

        private void ReportMetadataDefined(string callingFunction, string metadataJSON)
        {
            var e = new MessageEventArgs(callingFunction, metadataJSON);

            MetadataDefinedEvent?.Invoke(this, e);
        }

        /// <summary>
        /// Handler for both _mdContainer and myEMSLUpload debug events
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        void myEMSLUpload_DebugEvent(object sender, MessageEventArgs e)
        {
            DebugEvent?.Invoke(this, e);
        }

        /// <summary>
        /// Handler for both _mdContainer and myEMSLUpload error events
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        void myEMSLUpload_ErrorEvent(object sender, MessageEventArgs e)
        {
            ErrorEvent?.Invoke(this, e);
        }

        void myEMSLUpload_WarningEvent(object sender, MessageEventArgs e)
        {
            WarningEvent?.Invoke(this, e);
        }

        void myEMSLUpload_StatusUpdate(object sender, StatusEventArgs e)
        {
            if (StatusUpdate != null)
            {
                // Multiplying by 0.25 because we're assuming 25% of the time is required for _mdContainer to compute the Sha-1 hashes of files to be uploaded while 75% of the time is required to create and upload the .tar file
                var percentCompleteOverall = 25 + e.PercentCompleted * 0.75;
                StatusUpdate(this, new StatusEventArgs(percentCompleteOverall, e.TotalBytesSent, e.TotalBytesToSend, e.StatusMessage));
            }
        }

        void myEMSLUpload_UploadCompleted(object sender, UploadCompletedEventArgs e)
        {
            UploadCompleted?.Invoke(this, e);
        }

        void _mdContainer_ProgressEvent(object sender, ProgressEventArgs e)
        {
            if (StatusUpdate != null)
            {
                // Multiplying by 0.25 because we're assuming 25% of the time is required for _mdContainer to compute the Sha-1 hashes of files to be uploaded while 75% of the time is required to create and upload the .tar file
                var percentCompleteOverall = 0 + e.PercentComplete * 0.25;
                StatusUpdate(this, new StatusEventArgs(percentCompleteOverall, 0, _mdContainer.TotalFileSizeToUpload, string.Empty));
            }

        }

        #endregion


    }
}
