//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 10/20/2009
//*********************************************************************************************************

using CaptureTaskManager;
using PRISM;
using System;
using Uploader = Pacifica.Upload;

namespace DatasetArchivePlugin
{
    /// <summary>
    /// Used for both dataset archive and archive update
    /// </summary>
    class clsArchiveUpdate : clsOpsBase
    {
        // Ignore Spelling: dmsarch

        #region "Class wide variables"

        // Obsolete: "No longer used"
        // string mArchiveSharePath = string.Empty;             // The dataset folder path in the archive, for example: \\aurora.emsl.pnl.gov\dmsarch\VOrbiETD03\2013_2\QC_Shew_13_02_C_29Apr13_Cougar_13-03-25
        // string mResultsFolderPathArchive = string.Empty;     // The target path to copy the data to, for example:    \\aurora.emsl.pnl.gov\dmsarch\VOrbiETD03\2013_2\QC_Shew_13_02_C_29Apr13_Cougar_13-03-25\SIC201304300029_Auto938684
        // string mResultsFolderPathServer = string.Empty;      // The source path of the dataset folder (or dataset job results folder) to archive, for example: \\proto-7\VOrbiETD03\2013_2\QC_Shew_13_02_C_29Apr13_Cougar_13-03-25\SIC201304300029_Auto938684

        #endregion

        #region "Constructors"

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mgrParams">Manager parameters</param>
        /// <param name="taskParams">Task parameters</param>
        /// <param name="statusTools">Status Tools</param>
        /// <param name="fileTools">Instance of FileTools</param>
        public clsArchiveUpdate(IMgrParams mgrParams, ITaskParams taskParams, IStatusFile statusTools, FileTools fileTools)
            : base(mgrParams, taskParams, statusTools, fileTools)
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
            {
                return false;
            }

            var statusMessage = "Updating dataset " + mDatasetName + ", job " + mTaskParams.GetParam("Job");
            OnDebugEvent(statusMessage);

            statusMessage = "Pushing dataset folder to MyEMSL";
            OnDebugEvent(statusMessage);

            mMostRecentLogTime = DateTime.UtcNow;
            mLastStatusUpdateTime = DateTime.UtcNow;

            const int iMaxMyEMSLUploadAttempts = 2;

            var recurse = mTaskParams.GetParam("MyEMSLRecurse", true);

            // Set this to .CreateTarLocal to create the .tar file locally and thus not upload the data to MyEMSL
            var debugMode = Uploader.TarStreamUploader.UploadDebugMode.DebugDisabled;

            if (mTaskParams.GetParam("DebugTestTar", false))
            {
                debugMode = Uploader.TarStreamUploader.UploadDebugMode.CreateTarLocal;
            }
            else if (mTaskParams.GetParam("MyEMSLOffline", false))
            {
                debugMode = Uploader.TarStreamUploader.UploadDebugMode.MyEMSLOfflineMode;
            }

            if (debugMode != Uploader.TarStreamUploader.UploadDebugMode.DebugDisabled)
            {
                OnStatusEvent("Calling UploadToMyEMSLWithRetry with debugMode=" + debugMode);
            }

            const bool PUSH_TO_TEST_SERVER = false;

            var debugTestInstanceOnly = PUSH_TO_TEST_SERVER;

            bool allowRetry;
            string criticalErrorMessage;

            // ReSharper disable once ConditionIsAlwaysTrueOrFalse
            if (!debugTestInstanceOnly)
            {
                const bool USE_TEST_INSTANCE_FALSE = false;

                var copySuccess = UploadToMyEMSLWithRetry(iMaxMyEMSLUploadAttempts, recurse, debugMode,
                                                          USE_TEST_INSTANCE_FALSE,
                                                          out allowRetry, out criticalErrorMessage);

                if (!string.IsNullOrWhiteSpace(criticalErrorMessage))
                {
                    mErrMsg = criticalErrorMessage;
                }

                if (!allowRetry)
                {
                    FailureDoNotRetry = true;
                }

                if (!copySuccess)
                {
                    return false;
                }

                var subDirName = mTaskParams.GetParam("OutputDirectoryName", mTaskParams.GetParam("OutputFolderName"));

                // Finished with this update task
                statusMessage = string.Format("Completed push to MyEMSL, dataset {0}, directory {1}, job {2}",
                                              mDatasetName, subDirName, mTaskParams.GetParam("Job"));
                OnDebugEvent(statusMessage);
            }

            // ReSharper disable once ConditionIsAlwaysTrueOrFalse
            if (!PUSH_TO_TEST_SERVER)
            {
                return true;
            }

            // Possibly also upload the dataset to the MyEMSL test instance
#pragma warning disable 162
            const int PERCENT_DATA_TO_SEND_TO_TEST = 20;
            var testDateCutoff = new DateTime(2017, 7, 4);

            if (DateTime.Now > testDateCutoff)
            {
                // Testing has finished
                // Return true if debutTestInstanceOnly is false
                return !debugTestInstanceOnly;
            }

            var rand = new Random();
            var randomNumber = rand.Next(0, 100);

            if (randomNumber > PERCENT_DATA_TO_SEND_TO_TEST && !debugTestInstanceOnly)
            {
                // Do not send this dataset to the test server
                return true;
            }

            // Also upload a copy of the data to the MyEMSL test server

            const bool USE_TEST_INSTANCE_TRUE = true;

            var testCopySuccess = UploadToMyEMSLWithRetry(iMaxMyEMSLUploadAttempts, recurse, debugMode,
                                                          USE_TEST_INSTANCE_TRUE,
                                                          out allowRetry, out criticalErrorMessage);

            if (!string.IsNullOrWhiteSpace(criticalErrorMessage))
            {
                mErrMsg = criticalErrorMessage;
            }

            if (!testCopySuccess)
            {
                OnErrorEvent("MyEMSL test server upload failed");
            }
            else
            {
                statusMessage = "Completed push to the MyEMSL test server, dataset " + mDatasetName;
                OnDebugEvent(statusMessage);
            }

            return true;
#pragma warning restore 162

        }

        #endregion

    }
}
