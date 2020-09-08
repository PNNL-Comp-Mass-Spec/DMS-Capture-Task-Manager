//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 09/25/2009
//*********************************************************************************************************

using CaptureTaskManager;
using System;
using System.Collections.Generic;
using System.IO;

namespace CaptureToolPlugin
{
    /// <summary>
    /// Dataset capture plugin
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class clsPluginMain : clsToolRunnerBase
    {

        #region "Methods"
        /// <summary>
        /// Runs the capture step tool
        /// </summary>
        /// <returns>clsToolReturnData object containing tool operation results</returns>
        public override clsToolReturnData RunTool()
        {
            LogDebug("Starting CaptureToolPlugin.clsPluginMain.RunTool()");

            // Note that retData.CloseoutMsg will be stored in the Completion_Message field of the database
            // Similarly, retData.EvalMsg will be stored in the Evaluation_Message field of the database

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

            LogMessage( "Capturing dataset '" + mDataset + "'");

            // Determine if instrument is on Bionet
            var capMethod = mTaskParams.GetParam("Method");
            bool useBionet;
            if (string.Equals(capMethod, "secfso", StringComparison.OrdinalIgnoreCase))
            {
                useBionet = true;
            }
            else
            {
                useBionet = false;
            }

            ResetTimestampForQueueWaitTimeLogging();

            // Create the object that will perform capture operation
            var capOpTool = new clsCaptureOps(mMgrParams, mFileTools, useBionet, mTraceMode);
            try
            {
                LogDebug("clsPluginMain.RunTool(): Starting capture operation");

                var success = capOpTool.DoOperation(mTaskParams, retData);

                if (!success && !string.IsNullOrWhiteSpace(retData.CloseoutMsg) && mTraceMode)
                    ShowTraceMessage(retData.CloseoutMsg);

                if (capOpTool.NeedToAbortProcessing)
                {
                    mNeedToAbortProcessing = true;
                    if (retData.CloseoutType != EnumCloseOutType.CLOSEOUT_NEED_TO_ABORT_PROCESSING)
                        retData.CloseoutType = EnumCloseOutType.CLOSEOUT_NEED_TO_ABORT_PROCESSING;
                }

                LogDebug("clsPluginMain.RunTool(): Completed capture operation");
            }
            catch (Exception ex)
            {
                var msg = "clsPluginMain.RunTool(): Exception during capture operation (useBionet=" + useBionet + ")";
                if (ex.Message.Contains("unknown user name or bad password"))
                {
                    // This error randomly occurs; no need to log a full stack trace
                    msg += ", Logon failure: unknown user name or bad password";
                    LogError(msg);
                    // Set the EvalCode to 3 so that capture can be retried
                    retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    retData.EvalCode = EnumEvalCode.EVAL_CODE_NETWORK_ERROR_RETRY_CAPTURE;
                    retData.CloseoutMsg = msg;
                }
                else
                {
                    LogError(msg, ex);
                    retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    retData.CloseoutMsg = msg;
                }

            }

            capOpTool.DetachEvents();

            LogDebug("Completed clsPluginMain.RunTool()");

            return retData;

        }

        /// <summary>
        /// Initializes the capture tool
        /// </summary>
        /// <param name="mgrParams">Parameters for manager operation</param>
        /// <param name="taskParams">Parameters for the assigned task</param>
        /// <param name="statusTools">Tools for status reporting</param>
        public override void Setup(IMgrParams mgrParams, ITaskParams taskParams, IStatusFile statusTools)
        {
            LogDebug("Starting clsPluginMain.Setup()");

            base.Setup(mgrParams, taskParams, statusTools);

            LogDebug("Completed clsPluginMain.Setup()");
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfo()
        {

            LogDebug("Determining tool version info");

            var toolVersionInfo = string.Empty;
            var appDirectory = clsUtilities.GetAppDirectoryPath();

            if (string.IsNullOrWhiteSpace(appDirectory))
            {
                LogError("GetAppDirectoryPath returned an empty directory path to StoreToolVersionInfo for the Capture plugin");
                return false;
            }

            // Lookup the version of the Capture tool plugin
            var pluginPath = Path.Combine(appDirectory, "CaptureToolPlugin.dll");
            var success = StoreToolVersionInfoOneFile(ref toolVersionInfo, pluginPath);
            if (!success)
            {
                return false;
            }

            // Lookup the version of the Capture task manager
            var ctmPath = Path.Combine(appDirectory, "CaptureTaskManager.exe");
            success = StoreToolVersionInfoOneFile(ref toolVersionInfo, ctmPath);
            if (!success)
            {
                return false;
            }

            // Store path to CaptureToolPlugin.dll in toolFiles
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
                LogError("Exception calling SetStepTaskToolVersion", ex);
                return false;
            }
        }

        #endregion
    }
}
