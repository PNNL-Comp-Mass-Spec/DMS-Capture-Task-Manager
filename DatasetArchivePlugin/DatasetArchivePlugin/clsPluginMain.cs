
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 10/08/2009
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using CaptureTaskManager;

namespace DatasetArchivePlugin
{
    public class clsPluginMain : clsToolRunnerBase
    {
        //*********************************************************************************************************
        // Main class for plugin
        //**********************************************************************************************************

        #region "Constants"
        protected const string SP_NAME_STORE_MYEMSL_STATS = "StoreMyEMSLUploadStats";
        #endregion

        #region "Classwide Variables"

        bool mSubmittedToMyEMSL;

        bool mMyEMSLAlreadyUpToDate;

        #endregion

        #region "Methods"
        /// <summary>
        /// Runs the archive and archive update step tools
        /// </summary>
        /// <returns>Enum indicating success or failure</returns>
        public override clsToolReturnData RunTool()
        {
            string archiveOpDescription;
            mSubmittedToMyEMSL = false;
            mMyEMSLAlreadyUpToDate = false;

            var msg = "Starting DatasetArchivePlugin.clsPluginMain.RunTool()";
            LogDebug(msg);

            // Perform base class operations, if any
            var retData = base.RunTool();
            if (retData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED)
                return retData;

            // Store the version info in the database
            if (!StoreToolVersionInfo())
            {
                LogError("Aborting since StoreToolVersionInfo returned false");
                retData.CloseoutMsg = "Error determining tool version info";
                retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return retData;
            }

            // Always use clsArchiveUpdate for both archiving new datasets and updating existing datasets
            clsOpsBase archOpTool = new clsArchiveUpdate(m_MgrParams, m_TaskParams, m_StatusTools);
            RegisterEvents(archOpTool);

            if (m_TaskParams.GetParam("StepTool").ToLower() == "datasetarchive")
            {
                archiveOpDescription = "archive";
            }
            else
            {
                archiveOpDescription = "archive update";
            }

            // Attach the MyEMSL Upload event handler
            archOpTool.MyEMSLUploadComplete += MyEMSLUploadCompleteHandler;

            msg = "Starting " + archiveOpDescription + ", job " + m_Job + ", dataset " + m_Dataset;

            LogMessage(msg);
            if (archOpTool.PerformTask())
            {
                retData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
            }
            else
            {
                retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                retData.CloseoutMsg = archOpTool.ErrMsg;

                if (archOpTool.FailureDoNotRetry)
                    retData.EvalCode = EnumEvalCode.EVAL_CODE_FAILURE_DO_NOT_RETRY;

            }

            if (!string.IsNullOrEmpty(archOpTool.WarningMsg))
                retData.EvalMsg = archOpTool.WarningMsg;

            if (retData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS)
            {
                if (mSubmittedToMyEMSL)
                {
                    // Note that stored procedure SetStepTaskComplete will update MyEMSL State values if retData.EvalCode is 4 or 7
                    if (mMyEMSLAlreadyUpToDate)
                        retData.EvalCode = EnumEvalCode.EVAL_CODE_MYEMSL_IS_ALREADY_UP_TO_DATE;
                    else
                        retData.EvalCode = EnumEvalCode.EVAL_CODE_SUBMITTED_TO_MYEMSL;
                }
            }

            msg = "Completed " + archiveOpDescription + ", job " + m_Job;
            LogMessage(msg);

            msg = "Completed clsPluginMain.RunTool()";
            LogDebug(msg);

            return retData;
        }

        /// <summary>
        /// Initializes the dataset archive tool
        /// </summary>
        /// <param name="mgrParams">Parameters for manager operation</param>
        /// <param name="taskParams">Parameters for the assigned task</param>
        /// <param name="statusTools">Tools for status reporting</param>
        public override void Setup(IMgrParams mgrParams, ITaskParams taskParams, IStatusFile statusTools)
        {
            var msg = "Starting clsPluginMain.Setup()";
            LogDebug(msg);

            base.Setup(mgrParams, taskParams, statusTools);

            msg = "Completed clsPluginMain.Setup()";
            LogDebug(msg);
        }

        /// <summary>
        /// Communicates with database to store the MyEMSL upload stats
        /// </summary>
        /// <param name="fileCountNew"></param>
        /// <param name="fileCountUpdated"></param>
        /// <param name="bytes"></param>
        /// <param name="uploadTimeSeconds"></param>
        /// <param name="statusURI"></param>
        /// <param name="eusInstrumentID">EUS Instrument ID</param>
        /// <param name="eusProposalID">EUS Proposal number (usually an integer but sometimes includes letters, for example 8491a)</param>
        /// <param name="eusUploaderID">EUS user ID of the instrument operator</param>
        /// <param name="errorCode"></param>
        /// <param name="usedTestInstance"></param>
        /// <returns>True for success, False for failure</returns>
        protected bool StoreMyEMSLUploadStats(
            int fileCountNew,
            int fileCountUpdated,
            Int64 bytes,
            double uploadTimeSeconds,
            string statusURI,
            int eusInstrumentID,
            string eusProposalID,
            int eusUploaderID,
            int errorCode,
            bool usedTestInstance)
        {
            bool Outcome;

            mSubmittedToMyEMSL = true;
            if (fileCountNew == 0 && fileCountUpdated == 0)
                mMyEMSLAlreadyUpToDate = true;

            try
            {

                // Setup for execution of the stored procedure
                var spCmd = new SqlCommand(SP_NAME_STORE_MYEMSL_STATS)
                {
                    CommandType = CommandType.StoredProcedure
                };

                spCmd.Parameters.Add("@Return", SqlDbType.Int).Direction = ParameterDirection.ReturnValue;

                spCmd.Parameters.Add("@Job", SqlDbType.Int).Value = m_TaskParams.GetParam("Job", 0);

                spCmd.Parameters.Add("@DatasetID", SqlDbType.Int).Value = m_TaskParams.GetParam("Dataset_ID", 0);

                spCmd.Parameters.Add("@Subfolder", SqlDbType.VarChar, 128).Value = m_TaskParams.GetParam("OutputFolderName", string.Empty);

                spCmd.Parameters.Add("@FileCountNew", SqlDbType.Int).Value = fileCountNew;

                spCmd.Parameters.Add("@FileCountUpdated", SqlDbType.Int).Value = fileCountUpdated;

                spCmd.Parameters.Add("@Bytes", SqlDbType.BigInt).Value = bytes;

                spCmd.Parameters.Add("@UploadTimeSeconds", SqlDbType.Real).Value = (float)uploadTimeSeconds;

                spCmd.Parameters.Add("@StatusURI", SqlDbType.VarChar, 255).Value = statusURI;

                spCmd.Parameters.Add("@ErrorCode", SqlDbType.Int).Value = errorCode;

                byte testInstanceFlag;
                if (usedTestInstance)
                    testInstanceFlag = 1;
                else
                    testInstanceFlag = 0;

                spCmd.Parameters.Add("@UsedTestInstance", SqlDbType.TinyInt).Value = testInstanceFlag;

                spCmd.Parameters.Add("@EUSInstrumentID", SqlDbType.Int).Value = eusInstrumentID;

                spCmd.Parameters.Add("@EUSProposalID", SqlDbType.VarChar, 10).Value = eusProposalID;

                spCmd.Parameters.Add("@EUSUploaderID", SqlDbType.Int).Value = eusUploaderID;

                // Execute the SP (retry the call up to 4 times)
                var resCode = CaptureDBProcedureExecutor.ExecuteSP(spCmd, 4);

                if (resCode == 0)
                {
                    Outcome = true;
                }
                else
                {
                    var Msg = "Error " + resCode + " storing MyEMSL Upload Stats";
                    LogError(Msg);
                    Outcome = false;
                }

            }
            catch (Exception ex)
            {
                LogError("Exception storing the MyEMSL upload stats: " + ex.Message);
                Outcome = false;
            }

            return Outcome;

        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfo()
        {

            LogDebug("Determining tool version info");

            var strToolVersionInfo = string.Empty;
            var appFolder = clsUtilities.GetAppFolderPath();

            if (string.IsNullOrWhiteSpace(appFolder))
            {
                LogError("GetAppFolderPath returned an empty directory path to StoreToolVersionInfo for the Dataset Archive plugin");
                return false;
            }

            // Lookup the version of the Dataset Archive plugin
            var strPluginPath = Path.Combine(appFolder, "DatasetArchivePlugin.dll");
            var bSuccess = StoreToolVersionInfoOneFile(ref strToolVersionInfo, strPluginPath);
            if (!bSuccess)
                return false;

            // Store path to DatasetArchivePlugin.dll in ioToolFiles
            var ioToolFiles = new List<FileInfo>
            {
                new FileInfo(strPluginPath)
            };

            try
            {
                return SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, false);
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
                e.EUSInfo.EUSInstrumentID, e.EUSInfo.EUSProposalID, e.EUSInfo.EUSUploaderID,
                e.ErrorCode, e.UsedTestInstance);
        }


        #endregion
    }
}
