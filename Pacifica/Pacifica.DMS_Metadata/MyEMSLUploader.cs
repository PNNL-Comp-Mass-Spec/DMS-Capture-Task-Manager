using System;
using System.Collections.Generic;
using System.IO;
using Pacifica.Core;

namespace Pacifica.DMS_Metadata
{
    public class MyEMSLUploader
    {
        public const string RECURSIVE_UPLOAD = "MyEMSL_Recurse";

        DMSMetadataObject _mdContainer;
        readonly Upload myEMSLUpload;

        protected Dictionary<string, string> m_MgrParams;
        protected Dictionary<string, string> m_TaskParams;

        protected string mManagerName;

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
            get
            {
                return mUseTestInstance;
            }
            set
            {
                mUseTestInstance = value;
                myEMSLUpload.UseTestInstance = value;
                Configuration.UseTestInstance = value;
            }
        }

        public MyEMSLUploader(Dictionary<string, string> mgrParams, Dictionary<string, string> taskParams)
        {
            StatusURI = string.Empty;
            FileCountNew = 0;
            FileCountUpdated = 0;
            Bytes = 0;
            ErrorCode = string.Empty;

            m_MgrParams = mgrParams;
            m_TaskParams = taskParams;

            if (!m_MgrParams.TryGetValue("MgrName", out mManagerName))
                mManagerName = "MyEMSLUploader_" + Environment.MachineName;

            var transferFolderPath = Utilities.GetDictionaryValue(m_TaskParams, "TransferFolderPath", "");
            if (string.IsNullOrEmpty(transferFolderPath))
                throw new InvalidDataException("Job parameters do not have TransferFolderPath defined; unable to continue");

            var datasetName = Utilities.GetDictionaryValue(m_TaskParams, "Dataset", "");
            if (string.IsNullOrEmpty(transferFolderPath))
                throw new InvalidDataException("Job parameters do not have Dataset defined; unable to continue");

            transferFolderPath = Path.Combine(transferFolderPath, datasetName);

            var jobNumber = Utilities.GetDictionaryValue(m_TaskParams, "Job", "");
            if (string.IsNullOrEmpty(jobNumber))
                throw new InvalidDataException("Job parameters do not have Job defined; unable to continue");

            myEMSLUpload = new Upload(transferFolderPath, jobNumber);

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

        #endregion

        public bool StartUpload(EasyHttp.eDebugMode debugMode, out string statusURL)
        {

            // Instantiate the metadata object
            _mdContainer = new DMSMetadataObject(mManagerName);

            // Attach the events			
            _mdContainer.ProgressEvent += _mdContainer_ProgressEvent;
            _mdContainer.ErrorEvent += myEMSLUpload_ErrorEvent;

            _mdContainer.UseTestInstance = this.UseTestInstance;

            // Look for files to upload, compute a Sha-1 hash for each, and compare those hashes to existing files in MyEMSL
            _mdContainer.SetupMetadata(m_TaskParams, m_MgrParams, debugMode);

            Configuration.LocalTempDirectory = Utilities.GetDictionaryValue(m_MgrParams, "workdir", "");
            FileCountUpdated = _mdContainer.TotalFileCountUpdated;
            FileCountNew = _mdContainer.TotalFileCountNew;
            Bytes = _mdContainer.TotalFileSizeToUpload;

            _mdContainer.CreateLockFiles();
            bool success;

            try
            {
                success = myEMSLUpload.StartUpload(_mdContainer.MetadataObject, debugMode, out statusURL);
            }
            catch (Exception)
            {
                _mdContainer.DeleteLockFiles();
                throw;
            }

            _mdContainer.DeleteLockFiles();

            if (!string.IsNullOrEmpty(statusURL))
                StatusURI = statusURL + "/xml";

            return success;
        }

        #region "Events and Event Handlers"

        public event DebugEventHandler DebugEvent;
        public event DebugEventHandler ErrorEvent;

        public event StatusUpdateEventHandler StatusUpdate;
        public event UploadCompletedEventHandler UploadCompleted;

        void myEMSLUpload_DebugEvent(object sender, MessageEventArgs e)
        {
            if (DebugEvent != null)
                DebugEvent(this, e);
        }

        void myEMSLUpload_ErrorEvent(object sender, MessageEventArgs e)
        {
            if (ErrorEvent != null)
                ErrorEvent(this, e);
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
            if (UploadCompleted != null)
            {
                UploadCompleted(this, e);
            }
        }

        void _mdContainer_ProgressEvent(object sender, ProgressEventArgs e)
        {
            if (StatusUpdate != null)
            {
                // Multiplying by 0.25 because we're assuming 25% of the time is required for _mdContainer to compute the Sha-1 hashes of files to be uploaded while 75% of the time is required to create and upload the .tar file
                var percentCompleteOverall = 0 + e.PercentComplete * 0.25;
                StatusUpdate(this, new StatusEventArgs(percentCompleteOverall, 0, _mdContainer.TotalFileSizeToUpload, ""));
            }

        }

        #endregion


    }
}
