//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 10/08/2009
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using CaptureTaskManager;
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
        private const string SP_NAME_STORE_MYEMSL_STATS = "store_myemsl_upload_stats";

        private bool mSubmittedToMyEMSL;

        private bool mMyEMSLAlreadyUpToDate;

        /// <summary>
        /// Runs the archive and archive update step tools
        /// </summary>
        /// <returns>Enum indicating success or failure</returns>
        public override ToolReturnData RunTool()
        {
            string archiveOpDescription;
            mSubmittedToMyEMSL = false;
            mMyEMSLAlreadyUpToDate = false;

            LogDebug("Starting DatasetArchivePlugin.PluginMain.RunTool()");

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

            LogMessage("Starting {0}, job {1}, dataset {2}", archiveOpDescription, mJob, mDataset);

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

            if (returnData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS && mSubmittedToMyEMSL)
            {
                // Note that stored procedure set_ctm_step_task_complete will update MyEMSL State values if returnData.EvalCode is 4 or 7
                if (mMyEMSLAlreadyUpToDate)
                {
                    returnData.EvalCode = EnumEvalCode.EVAL_CODE_MYEMSL_IS_ALREADY_UP_TO_DATE;
                }
                else
                {
                    returnData.EvalCode = EnumEvalCode.EVAL_CODE_SUBMITTED_TO_MYEMSL;
                }
            }

            LogMessage("Completed {0}, job {1}", archiveOpDescription, mJob);

            LogDebug("Completed PluginMain.RunTool()");

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
            LogDebug("Starting PluginMain.Setup()");

            base.Setup(mgrParams, taskParams, statusTools);

            LogDebug("Completed PluginMain.Setup()");
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
        /// <param name="eusUploaderID">EUS user ID of the instrument operator (aka the submitter)</param>
        /// <param name="errorCode"></param>
        /// <param name="usedTestInstance"></param>
        /// <returns>True for success, False for failure</returns>
        private void StoreMyEMSLUploadStats(
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

                var testInstanceFlag = usedTestInstance ? (byte)1 : (byte)0;

                // Define parameter for procedure's return value
                // If querying a Postgres DB, dbTools will auto-change "@return" to "_returnCode"
                var returnParam = dbTools.AddParameter(cmd, "@Return", SqlType.Int, ParameterDirection.ReturnValue);

                dbTools.AddParameter(cmd, "@job", SqlType.Int).Value = mTaskParams.GetParam("Job", 0);
                dbTools.AddParameter(cmd, "@datasetID", SqlType.Int).Value = mTaskParams.GetParam("Dataset_ID", 0);
                dbTools.AddParameter(cmd, "@subfolder", SqlType.VarChar, 128, subDir);
                dbTools.AddParameter(cmd, "@fileCountNew", SqlType.Int).Value = fileCountNew;
                dbTools.AddParameter(cmd, "@fileCountUpdated", SqlType.Int).Value = fileCountUpdated;
                dbTools.AddParameter(cmd, "@bytes", SqlType.BigInt).Value = bytes;
                dbTools.AddParameter(cmd, "@uploadTimeSeconds", SqlType.Real).Value = (float)uploadTimeSeconds;
                dbTools.AddParameter(cmd, "@statusURI", SqlType.VarChar, 255, statusURI);
                dbTools.AddParameter(cmd, "@errorCode", SqlType.Int).Value = errorCode;
                dbTools.AddParameter(cmd, "@usedTestInstance", SqlType.TinyInt).Value = testInstanceFlag;
                dbTools.AddParameter(cmd, "@eusInstrumentID", SqlType.Int).Value = eusInstrumentID;
                dbTools.AddParameter(cmd, "@eusProposalID", SqlType.VarChar, 10, eusProjectID);
                dbTools.AddParameter(cmd, "@eusUploaderID", SqlType.Int).Value = eusUploaderID;

                // Call the procedure (retry the call, up to 4 times)
                dbTools.ExecuteSP(cmd, 4);

                var returnCode = DBToolsBase.GetReturnCode(returnParam);

                if (returnCode == 0)
                {
                    return;
                }

                LogError("Error storing MyEMSL Upload Stats, return code " + returnParam.Value.CastDBVal<string>());
            }
            catch (Exception ex)
            {
                LogError("Exception storing the MyEMSL upload stats: " + ex.Message);
            }
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        private bool StoreToolVersionInfo()
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
                new(pluginPath)
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
    }
}
