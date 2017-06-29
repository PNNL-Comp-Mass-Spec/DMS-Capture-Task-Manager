
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 10/20/2009
//
//*********************************************************************************************************

using System;
using CaptureTaskManager;

namespace DatasetArchivePlugin
{
    class clsArchiveUpdate : clsOpsBase
    {
        //*********************************************************************************************************
        // Tools to perform both dataset archive and archive update operations
        //**********************************************************************************************************

        #region "Class variables"

        // Obsolete: "No longer used"
        // string m_ArchiveSharePath = string.Empty;                // The dataset folder path in the archive, for example: \\aurora.emsl.pnl.gov\dmsarch\VOrbiETD03\2013_2\QC_Shew_13_02_C_29Apr13_Cougar_13-03-25
        // string m_ResultsFolderPathArchive = string.Empty;        // The target path to copy the data to, for example:    \\aurora.emsl.pnl.gov\dmsarch\VOrbiETD03\2013_2\QC_Shew_13_02_C_29Apr13_Cougar_13-03-25\SIC201304300029_Auto938684
        // string m_ResultsFolderPathServer = string.Empty;     // The source path of the dataset folder (or dataset job results folder) to archive, for example: \\proto-7\VOrbiETD03\2013_2\QC_Shew_13_02_C_29Apr13_Cougar_13-03-25\SIC201304300029_Auto938684

        #endregion

        #region "Constructors"

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="MgrParams">Manager parameters</param>
        /// <param name="TaskParams">Task parameters</param>
        /// <param name="StatusTools"></param>
        public clsArchiveUpdate(IMgrParams MgrParams, ITaskParams TaskParams, IStatusFile StatusTools)
            : base(MgrParams, TaskParams, StatusTools)
        {
        }
        #endregion

        #region "Methods"
        /// <summary>
        /// Performs a dataset archive or archive update task
        /// </summary>
        /// <returns>TRUE for success, FALSE for failure</returns>
        public override bool PerformTask()
        {

            // Perform base class operations
            if (!base.PerformTask())
                return false;

            var statusMessage = "Updating dataset " + m_DatasetName + ", job " + m_TaskParams.GetParam("Job");
            OnDebugEvent(statusMessage);

            statusMessage = "Pushing dataset folder to MyEMSL";
            OnDebugEvent(statusMessage);

            mMostRecentLogTime = DateTime.UtcNow;
            mLastStatusUpdateTime = DateTime.UtcNow;

            const int iMaxMyEMSLUploadAttempts = 2;
            const bool recurse = true;

            // Set this to true to create the .tar file locally and thus not upload the data to MyEMSL
            var debugMode = Pacifica.Core.EasyHttp.eDebugMode.DebugDisabled;

            if (m_TaskParams.GetParam("DebugTestTar", false))
                debugMode = Pacifica.Core.EasyHttp.eDebugMode.CreateTarLocal;
            else
                if (m_TaskParams.GetParam("MyEMSLOffline", false))
                debugMode = Pacifica.Core.EasyHttp.eDebugMode.MyEMSLOfflineMode;

            if (debugMode != Pacifica.Core.EasyHttp.eDebugMode.DebugDisabled)
                OnStatusEvent("Calling UploadToMyEMSLWithRetry with debugMode=" + debugMode);

            const bool PUSH_TO_TEST_SERVER = true;

            var debugTestInstanceOnly = PUSH_TO_TEST_SERVER;

            if (!debugTestInstanceOnly)
            {
                var copySuccess = UploadToMyEMSLWithRetry(iMaxMyEMSLUploadAttempts, recurse, debugMode,
                                                          useTestInstance: false);

                if (!copySuccess)
                    return false;

                // Finished with this update task
                statusMessage = "Completed push to MyEMSL, dataset " + m_DatasetName + ", Folder " +
                                m_TaskParams.GetParam("OutputFolderName") + ", job " + m_TaskParams.GetParam("Job");
                OnDebugEvent(statusMessage);
            }

            if (!PUSH_TO_TEST_SERVER)
                return true;

            // Possibly also upload the dataset to the MyEMSL test instance
            const int PERCENT_DATA_TO_SEND_TO_TEST = 100;
            var testDateCuttoff = new DateTime(2017, 7, 1);

            if (DateTime.Now > testDateCuttoff)
            {
                // Testing has finished
                return true;
            }

            var rand = new Random();
            var randomNumber = rand.Next(0, 100);

            if (randomNumber > PERCENT_DATA_TO_SEND_TO_TEST & !debugTestInstanceOnly)
            {
                // Do not send this dataset to the test server
                return true;
            }

            // Also upload a copy of the data to the MyEMSL test server

            var testCopySuccess = UploadToMyEMSLWithRetry(iMaxMyEMSLUploadAttempts, recurse, debugMode, useTestInstance: true);
            if (!testCopySuccess)
            {
                OnErrorEvent("MyEMSL test server upload failed");
            }
            else
            {
                statusMessage = "Completed push to the MyEMSL test server, dataset " + m_DatasetName;
                OnDebugEvent(statusMessage);
            }

            return true;

        }

        /// <summary>
        /// Write an error message to the log
        /// If msg is blank, then logs the current task description followed by "empty error message"
        /// </summary>
        /// <param name="msg">Error message</param>
        /// <param name="currentTask">Current task</param>
        private void LogErrorMessage(string msg, string currentTask)
        {
            if (string.IsNullOrWhiteSpace(msg))
                msg = currentTask + ": empty error message";

            OnErrorEvent(msg);

        }

        #endregion


    }
}
