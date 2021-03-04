//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 10/08/2009
//*********************************************************************************************************

using CaptureTaskManager;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using PRISMDatabaseUtils;

namespace DatasetArchivePlugin
{
    /// <summary>
    /// Dataset archive plugin
    /// </summary>
    /// <remarks>Also used for archive update</remarks>
    // ReSharper disable once UnusedMember.Global
    public class PluginMain : ToolRunnerBase
    {
        #region "Constants"
        protected const string SP_NAME_STORE_MYEMSL_STATS = "StoreMyEMSLUploadStats";
        #endregion

        #region "Class wide Variables"

        private bool mSubmittedToMyEMSL;
        private bool mMyEMSLAlreadyUpToDate;

        #endregion

        #region "Methods"
        /// <summary>
        /// Runs the archive and archive update step tools
        /// </summary>
        /// <returns>Enum indicating success or failure</returns>
        public override ToolReturnData RunTool()
        {
            string archiveOpDescription;
            mSubmittedToMyEMSL = false;
            mMyEMSLAlreadyUpToDate = false;

            var msg = "Starting DatasetArchivePlugin.PluginMain.RunTool()";
            LogDebug(msg);

            // Perform base class operations, if any
            var returnData = base.RunTool();
            if (returnData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED)
            {
                return returnData;
            }

            // Store the version info in the database
            if (!StoreToolVersionInfo())
            {
                LogError("Aborting since StoreToolVersionInfo returned false");
                returnData.CloseoutMsg = "Error determining tool version info";
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return returnData;
            }

            ResetTimestampForQueueWaitTimeLogging();

            // Always use ArchiveUpdate for both archiving new datasets and updating existing datasets
            OpsBase archOpTool = new ArchiveUpdate(mMgrParams, mTaskParams, mStatusTools, mFileTools);
            RegisterEvents(archOpTool);

            if (mTaskParams.GetParam("StepTool").Equals("DatasetArchive", StringComparison.OrdinalIgnoreCase))
            {
                archiveOpDescription = "archive";
            }
            else
            {
                archiveOpDescription = "archive update";
            }

            // Attach the MyEMSL Upload event handler
            archOpTool.MyEMSLUploadComplete += MyEMSLUploadCompleteHandler;

            msg = "Starting " + archiveOpDescription + ", job " + mJob + ", dataset " + mDataset;

            LogMessage(msg);
            if (archOpTool.PerformTask())
            {
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
            }
            else
            {
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                returnData.CloseoutMsg = archOpTool.ErrMsg;

                if (archOpTool.FailureDoNotRetry)
                {
                    returnData.EvalCode = EnumEvalCode.EVAL_CODE_FAILURE_DO_NOT_RETRY;
                }
            }

            if (!string.IsNullOrEmpty(archOpTool.WarningMsg))
            {
                returnData.EvalMsg = archOpTool.WarningMsg;
            }

            if (returnData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS)
            {
                if (mSubmittedToMyEMSL)
                {
                    // Note that stored procedure SetStepTaskComplete will update MyEMSL State values if returnData.EvalCode is 4 or 7
                    if (mMyEMSLAlreadyUpToDate)
                    {
                        returnData.EvalCode = EnumEvalCode.EVAL_CODE_MYEMSL_IS_ALREADY_UP_TO_DATE;
                    }
                    else
                    {
                        returnData.EvalCode = EnumEvalCode.EVAL_CODE_SUBMITTED_TO_MYEMSL;
                    }
                }
            }

            msg = "Completed " + archiveOpDescription + ", job " + mJob;
            LogMessage(msg);

            msg = "Completed PluginMain.RunTool()";
            LogDebug(msg);

            return returnData;
        }

        /// <summary>
        /// Initializes the dataset archive tool
        /// </summary>
        /// <param name="mgrParams">Parameters for manager operation</param>
        /// <param name="taskParams">Parameters for the assigned task</param>
        /// <param name="statusTools">Tools for status reporting</param>
        public override void Setup(IMgrParams mgrParams, ITaskParams taskParams, IStatusFile statusTools)
        {
            var msg = "Starting PluginMain.Setup()";
            LogDebug(msg);

            base.Setup(mgrParams, taskParams, statusTools);

            msg = "Completed PluginMain.Setup()";
            LogDebug(msg);
        }

        /// <summary>
        /// Communicates with database to store the MyEMSL upload stats in table T_MyEMSL_Uploads
        /// </summary>
        /// <param name="fileCountNew"></param>
        /// <param name="fileCountUpdated"></param>
        /// <param name="bytes"></param>
        /// <param name="uploadTimeSeconds"></param>
        /// <param name="statusURI"></param>
        /// <param name="eusInstrumentID">EUS Instrument ID</param>
        /// <param name="eusProjectID">EUS Project number (usually an integer but sometimes includes letters, for example 8491a)</param>
        /// <param name="eusUploaderID">EUS user ID of the instrument operator</param>
        /// <param name="errorCode"></param>
        /// <param name="usedTestInstance"></param>
        /// <returns>True for success, False for failure</returns>
        protected bool StoreMyEMSLUploadStats(
            int fileCountNew,
            int fileCountUpdated,
            long bytes,
            double uploadTimeSeconds,
            string statusURI,
            int eusInstrumentID,
            string eusProjectID,
            int eusUploaderID,
            int errorCode,
            bool usedTestInstance)
        {
            mSubmittedToMyEMSL = true;

            mMyEMSLAlreadyUpToDate = (errorCode == 0 && fileCountNew == 0 && fileCountUpdated == 0);

            try
            {
                // Setup for execution of the stored procedure
                var dbTools = mCaptureDbProcedureExecutor;
                var cmd = dbTools.CreateCommand(SP_NAME_STORE_MYEMSL_STATS, CommandType.StoredProcedure);

                var subDir = mTaskParams.GetParam("OutputDirectoryName", mTaskParams.GetParam("OutputFolderName"));

                byte testInstanceFlag;
                if (usedTestInstance)
                {
                    testInstanceFlag = 1;
                }
                else
                {
                    testInstanceFlag = 0;
                }

                dbTools.AddParameter(cmd, "@Return", SqlType.Int, ParameterDirection.ReturnValue);
                dbTools.AddParameter(cmd, "@Job", SqlType.Int).Value = mTaskParams.GetParam("Job", 0);
                dbTools.AddParameter(cmd, "@DatasetID", SqlType.Int).Value = mTaskParams.GetParam("Dataset_ID", 0);
                dbTools.AddParameter(cmd, "@Subfolder", SqlType.VarChar, 128, subDir);
                dbTools.AddParameter(cmd, "@FileCountNew", SqlType.Int).Value = fileCountNew;
                dbTools.AddParameter(cmd, "@FileCountUpdated", SqlType.Int).Value = fileCountUpdated;
                dbTools.AddParameter(cmd, "@Bytes", SqlType.BigInt).Value = bytes;
                dbTools.AddParameter(cmd, "@UploadTimeSeconds", SqlType.Real).Value = (float)uploadTimeSeconds;
                dbTools.AddParameter(cmd, "@StatusURI", SqlType.VarChar, 255, statusURI);
                dbTools.AddParameter(cmd, "@ErrorCode", SqlType.Int).Value = errorCode;
                dbTools.AddParameter(cmd, "@UsedTestInstance", SqlType.TinyInt).Value = testInstanceFlag;
                dbTools.AddParameter(cmd, "@EUSInstrumentID", SqlType.Int).Value = eusInstrumentID;
                dbTools.AddParameter(cmd, "@EUSProposalID", SqlType.VarChar, 10, eusProjectID);
                dbTools.AddParameter(cmd, "@EUSUploaderID", SqlType.Int).Value = eusUploaderID;

                // Execute the SP (retry the call up to 4 times)
                var resCode = dbTools.ExecuteSP(cmd, 4);

                if (resCode == 0)
                {
                    return true;
                }

                LogError("Error " + resCode + " storing MyEMSL Upload Stats");
                return false;
            }
            catch (Exception ex)
            {
                LogError("Exception storing the MyEMSL upload stats: " + ex.Message);
                return false;
            }
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfo()
        {
            LogDebug("Determining tool version info");

            var toolVersionInfo = string.Empty;
            var appDirectory = CTMUtilities.GetAppDirectoryPath();

            if (string.IsNullOrWhiteSpace(appDirectory))
            {
                LogError("GetAppDirectoryPath returned an empty directory path to StoreToolVersionInfo for the Dataset Archive plugin");
                return false;
            }

            // Lookup the version of the Dataset Archive plugin
            var pluginPath = Path.Combine(appDirectory, "DatasetArchivePlugin.dll");
            var success = StoreToolVersionInfoOneFile(ref toolVersionInfo, pluginPath);
            if (!success)
            {
                return false;
            }

            // Store path to DatasetArchivePlugin.dll in toolFiles
            var toolFiles = new List<FileInfo>
            {
                new FileInfo(pluginPath)
            };

            try
            {
                return SetStepTaskToolVersion(toolVersionInfo, toolFiles, false);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }
        }

        private void MyEMSLUploadCompleteHandler(object sender, MyEMSLUploadEventArgs e)
        {
            StoreMyEMSLUploadStats(
                e.FileCountNew, e.FileCountUpdated,
                e.BytesUploaded, e.UploadTimeSeconds, e.StatusURI,
                e.EUSInfo.EUSInstrumentID, e.EUSInfo.EUSProjectID, e.EUSInfo.EUSUploaderID,
                e.ErrorCode, e.UsedTestInstance);
        }

        #endregion
    }
}
