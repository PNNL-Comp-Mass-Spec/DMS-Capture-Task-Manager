
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 10/20/2009
//
// Last modified 10/20/2009
//*********************************************************************************************************
using System;
using System.Threading;
using CaptureTaskManager;
using Pacifica.Core;
using Pacifica.DMS_Metadata;
using PRISM.Files;
using MD5StageFileCreator;
using System.IO;

namespace DatasetArchivePlugin
{
    class clsOpsBase : IArchiveOps
    {
        //*********************************************************************************************************
        // Base class for archive and archive update operations classes. This class should always be overridden.
        //**********************************************************************************************************

        #region "Constants"
        protected const string ARCHIVE = "Archive ";
        protected const string UPDATE = "Archive update ";

        #endregion

        #region "Class variables"
        protected IMgrParams m_MgrParams;
        protected ITaskParams m_TaskParams;
        protected IStatusFile m_StatusTools;

        protected string m_ErrMsg = string.Empty;
        protected string m_WarningMsg = string.Empty;
        protected string m_DSNamePath;

        protected bool m_MyEmslUploadSuccess;

        protected string m_User;
        protected string m_Pwd;
        protected bool m_UseTls;
        protected int m_ServerPort;
        protected int m_FtpTimeOut;
        protected bool m_FtpPassive;
        protected bool m_FtpRestart;
        protected bool m_ConnectionOpen = false;
        protected string m_ArchiveOrUpdate;
        protected string m_DatasetName = string.Empty;

        protected DateTime mLastStatusUpdateTime = DateTime.UtcNow;
        protected DateTime mLastProgressUpdateTime = DateTime.UtcNow;

        protected string mMostRecentLogMessage = string.Empty;
        protected DateTime mMostRecentLogTime = DateTime.UtcNow;

        protected clsFileTools m_FileTools;

        #endregion

        #region "Properties"
        /// <summary>
        /// Implements IArchiveOps.ErrMsg
        /// </summary>
        public string ErrMsg
        {
            get { return m_ErrMsg; }
        }

        public string WarningMsg
        {
            get { return m_WarningMsg; }
        }

        #endregion

        #region "Constructors"
        public clsOpsBase(IMgrParams MgrParams, ITaskParams TaskParams, IStatusFile StatusTools)
        {
            m_MgrParams = MgrParams;
            m_TaskParams = TaskParams;
            m_StatusTools = StatusTools;

            m_User = m_MgrParams.GetParam("username");
            m_Pwd = m_MgrParams.GetParam("userpwd");
            m_UseTls = bool.Parse(m_MgrParams.GetParam("usetls"));
            m_ServerPort = int.Parse(m_MgrParams.GetParam("serverport"));
            m_FtpTimeOut = int.Parse(m_MgrParams.GetParam("timeout"));
            m_FtpPassive = bool.Parse(m_MgrParams.GetParam("passive"));
            m_FtpRestart = bool.Parse(m_MgrParams.GetParam("restart"));

            if (m_TaskParams.GetParam("StepTool") == "DatasetArchive")
            {
                m_ArchiveOrUpdate = ARCHIVE;
            }
            else
            {
                m_ArchiveOrUpdate = UPDATE;
            }

            // Instantiate m_FileTools
            m_FileTools = new clsFileTools(m_MgrParams.GetParam("MgrName", "CaptureTaskManager"), 1);

        }	// End sub
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

            //Path to dataset on storage server
            m_DSNamePath = Path.Combine(Path.Combine(baseStoragePath, m_TaskParams.GetParam("Storage_Path")), m_TaskParams.GetParam("Folder"));

            //Verify dataset is in specified location
            if (!VerifyDSPresent(m_DSNamePath))
            {
                var errorMessage = "Dataset folder " + m_DSNamePath + " not found";
                m_ErrMsg = string.Copy(errorMessage);
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMessage);
                LogOperationFailed(m_DatasetName);
                return false;
            }

            // Got to here, everything's OK, so let let the derived class take over
            return true;

        }	// End sub

        protected string AppendToString(string text, string append)
        {
            if (string.IsNullOrEmpty(text))
                text = string.Empty;
            else
                text += "; ";

            return text + append;
        }

        protected bool UploadToMyEMSLWithRetry(int maxAttempts, bool recurse, EasyHttp.eDebugMode debugMode)
        {
            bool bSuccess = false;
            int iAttempts = 0;
            m_MyEmslUploadSuccess = false;

            if (maxAttempts < 1)
                maxAttempts = 1;

            if (Environment.UserName.ToLower() != "svc-dms")
            {
                // The current user is not svc-dms
                // Uploaded files would be associated with the wrong username and thus would not be visible to all DMS Users
                m_ErrMsg = "Files must be uploaded to MyEMSL using the svc-dms account; aborting";
                Console.WriteLine(m_ErrMsg);
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_ErrMsg);
                return false;
            }

            while (!bSuccess && iAttempts < maxAttempts)
            {
                iAttempts += 1;
                bSuccess = UploadToMyEMSL(recurse, debugMode);

                if (debugMode != EasyHttp.eDebugMode.DebugDisabled)
                    break;

                if (!bSuccess && iAttempts < maxAttempts)
                {
                    // Wait 5 seconds, then retry
                    Thread.Sleep(5000);
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
        /// Use MyEMSLUploader to upload the data to MyEMSL
        /// </summary>
        /// <returns>True if success, false if an error</returns>
        protected bool UploadToMyEMSL(bool recurse, EasyHttp.eDebugMode debugMode)
        {
            bool success;
            DateTime dtStartTime = DateTime.UtcNow;
            MyEMSLUploader myEMSLUL = null;

            try
            {
                var statusMessage = "Bundling changes to dataset " + m_DatasetName + " for transmission to MyEMSL";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, statusMessage);

                myEMSLUL = new MyEMSLUploader(m_MgrParams.TaskDictionary, m_TaskParams.TaskDictionary);

                // Attach the events

                myEMSLUL.DebugEvent += myEMSLUL_DebugEvent;
                myEMSLUL.ErrorEvent += myEMSLUL_ErrorEvent;
                myEMSLUL.StatusUpdate += myEMSLUL_StatusUpdate;
                myEMSLUL.UploadCompleted += myEMSLUL_UploadCompleted;

                m_TaskParams.AddAdditionalParameter(MyEMSLUploader.RECURSIVE_UPLOAD, recurse.ToString());

                string statusURL;

                // Start the upload
                success = myEMSLUL.StartUpload(debugMode, out statusURL);

                var tsElapsedTime = DateTime.UtcNow.Subtract(dtStartTime);

                statusMessage = "Upload of " + m_DatasetName + " completed in " + tsElapsedTime.TotalSeconds.ToString("0.0") + " seconds";
                if (!success)
                    statusMessage += " (success=false)";

                statusMessage += ": " + myEMSLUL.FileCountNew + " new files, " + myEMSLUL.FileCountUpdated + " updated files, " + myEMSLUL.Bytes + " bytes";
                statusMessage += "; " + myEMSLUL.StatusURI;

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, statusMessage);

                if (debugMode != EasyHttp.eDebugMode.DebugDisabled)
                    return false;

                var errorMessage = "myEMSL statusURI => " + myEMSLUL.StatusURI;

                if (statusURL.EndsWith("/1323420608"))
                {
                    errorMessage += "; this indicates an upload error (transactionID=-1)";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMessage);
                    return false;
                }                                            

                // Raise an event with the stats
                // This will cause clsPluginMain to call StoreMyEMSLUploadStats to store the results in the database (stored procedure StoreMyEMSLUploadStats)
                // If an error occurs while storing to the database, the status URI will be listed in the manager's local log file
                var e = new MyEMSLUploadEventArgs(myEMSLUL.FileCountNew, myEMSLUL.FileCountUpdated, myEMSLUL.Bytes, tsElapsedTime.TotalSeconds, statusURL, iErrorCode: 0);
                OnMyEMSLUploadComplete(e);

                m_StatusTools.UpdateAndWrite(100);
               
            }
            catch (Exception ex)
            {
                const string errorMessage = "Exception uploading to MyEMSL";
                m_ErrMsg = string.Copy(errorMessage);
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMessage, ex);
                LogOperationFailed(m_DatasetName);

                // Raise an event with the stats

                int errorCode = ex.Message.GetHashCode();
                if (errorCode == 0)
                    errorCode = 1;

                var tsElapsedTime = DateTime.UtcNow.Subtract(dtStartTime);

                MyEMSLUploadEventArgs e;
                if (myEMSLUL == null)
                    e = new MyEMSLUploadEventArgs(0, 0, 0, tsElapsedTime.TotalSeconds, string.Empty, errorCode);
                else
                    e = new MyEMSLUploadEventArgs(myEMSLUL.FileCountNew, myEMSLUL.FileCountUpdated, myEMSLUL.Bytes, tsElapsedTime.TotalSeconds, myEMSLUL.StatusURI, errorCode);

                OnMyEMSLUploadComplete(e);

                success = false;
            }
            finally
            {
                // Detach the event handlers
                if (myEMSLUL != null)
                {
                    myEMSLUL.DebugEvent -= myEMSLUL_DebugEvent;
                    myEMSLUL.ErrorEvent -= myEMSLUL_ErrorEvent;
                    myEMSLUL.StatusUpdate -= myEMSLUL_StatusUpdate;
                    myEMSLUL.UploadCompleted -= myEMSLUL_UploadCompleted;
                }
            }

            return success;

        }

        /// <summary>
        /// Verifies specified dataset is present
        /// </summary>
        /// <param name="dsNamePath">Fully qualified path to dataset folder</param>
        /// <returns>TRUE if dataset folder is present; otherwise FALSE</returns>
        protected bool VerifyDSPresent(string dsNamePath)
        {
            //Verifies specified dataset is present
            return Directory.Exists(dsNamePath);

        }	// End sub

        /// <summary>
        /// Writes a database log entry for a failed archive operation
        /// </summary>
        /// <param name="dsName">Name of dataset</param>
        protected void LogOperationFailed(string dsName)
        {
            string msg = m_ArchiveOrUpdate + "failed, dataset " + dsName;
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
        }	// End sub

        /// <summary>
        /// Determine the total size of all files in the specified folder (including subdirectories)
        /// </summary>
        /// <param name="sourceFolderPath"></param>
        /// <returns>Total size, in GB</returns>
        protected float ComputeFolderSizeGB(string sourceFolderPath)
        {
            var diSourceFolder = new DirectoryInfo(sourceFolderPath);

            string msg = "Determing the total size of " + sourceFolderPath;
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

            if (!diSourceFolder.Exists)
            {
                msg = "Source folder not found by ComputeFolderSizeGB: " + sourceFolderPath;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);

                return 0;
            }
            float folderSizeGB = 0;

            foreach (FileInfo fiFile in diSourceFolder.GetFiles("*", SearchOption.AllDirectories))
            {
                folderSizeGB += (float)(fiFile.Length / 1024.0 / 1024.0 / 1024.0);
            }

            msg = "  Total size: " + folderSizeGB.ToString("0.0") + " GB";
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

            return folderSizeGB;

        }

        #endregion

        #region "Event Delegates and Classes"

        public event MyEMSLUploadEventHandler MyEMSLUploadComplete;

        #endregion

        #region "Event Handlers"

        void LogStatusMessageSkipDuplicate(string message)
        {
            if (String.Equals(message, mMostRecentLogMessage) || DateTime.UtcNow.Subtract(mMostRecentLogTime).TotalSeconds >= 60)
            {
                mMostRecentLogMessage = string.Copy(message);
                mMostRecentLogTime = DateTime.UtcNow;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, message);
            }
        }

        void myEMSLUL_DebugEvent(object sender, MessageEventArgs e)
        {
            string msg = "  ... " + e.CallingFunction + ": " + e.Message;
            LogStatusMessageSkipDuplicate(msg);
        }

        void myEMSLUL_ErrorEvent(object sender, MessageEventArgs e)
        {
            string msg = "MyEmslUpload error in function " + e.CallingFunction + ": " + e.Message;
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
        }

        void myEMSLUL_StatusUpdate(object sender, StatusEventArgs e)
        {

            if (DateTime.UtcNow.Subtract(mLastStatusUpdateTime).TotalSeconds >= 60 && e.PercentCompleted > 0)
            {
                mLastStatusUpdateTime = DateTime.UtcNow;
                string msg = "  ... uploading, " + e.PercentCompleted.ToString("0.0") + "% complete for " + (e.TotalBytesToSend / 1024.0).ToString("#,##0") + " KB";
                if (!(string.IsNullOrEmpty(e.StatusMessage)))
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
            string msg = "  ... MyEmsl upload task complete";

            // Note that e.ServerResponse will simply have the StatusURL if the upload succeeded
            // If a problem occurred, then e.ServerResponse will either have the full server reponse, or may even be blank
            if (string.IsNullOrEmpty(e.ServerResponse))
                msg += ": empty server reponse";
            else
                msg += ": " + e.ServerResponse;

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
            m_MyEmslUploadSuccess = true;
        }

        public void OnMyEMSLUploadComplete(MyEMSLUploadEventArgs e)
        {
            if (MyEMSLUploadComplete != null)
                MyEMSLUploadComplete(this, e);
        }
        #endregion

    }	// End class


}	// End namespace
