//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 09/25/2009
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using CaptureTaskManager;

namespace CaptureToolPlugin
{
    /// <summary>
    /// Dataset capture plugin
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class PluginMain : ToolRunnerBase
    {
        // Ignore Spelling: Bionet, secfso, Logon

        /// <summary>
        /// Runs the capture step tool
        /// </summary>
        /// <returns>ToolReturnData object containing tool operation results</returns>
        public override ToolReturnData RunTool()
        {
            LogDebug("Starting CaptureToolPlugin.PluginMain.RunTool()");

            // Note that returnData.CloseoutMsg will be stored in the Completion_Message field of the database
            // Similarly, returnData.EvalMsg will be stored in the Evaluation_Message field of the database

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

            LogMessage( "Capturing dataset '" + mDataset + "'");

            // Determine if instrument is on Bionet
            var capMethod = mTaskParams.GetParam("Method");
            var useBionet = string.Equals(capMethod, "secfso", StringComparison.OrdinalIgnoreCase);

            ResetTimestampForQueueWaitTimeLogging();

            // Create the object that will perform capture operation
            var capOpTool = new CaptureOps(mMgrParams, mFileTools, useBionet, mTraceMode);
            try
            {
                LogDebug("PluginMain.RunTool(): Starting capture operation");

                var success = capOpTool.DoOperation(mTaskParams, returnData);

                if (!success && !string.IsNullOrWhiteSpace(returnData.CloseoutMsg) && mTraceMode)
                {
                    ShowTraceMessage(returnData.CloseoutMsg);
                }

                if (capOpTool.NeedToAbortProcessing)
                {
                    mNeedToAbortProcessing = true;

                    if (returnData.CloseoutType != EnumCloseOutType.CLOSEOUT_NEED_TO_ABORT_PROCESSING)
                    {
                        returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_NEED_TO_ABORT_PROCESSING;
                    }
                }

                LogDebug("PluginMain.RunTool(): Completed capture operation");
            }
            catch (Exception ex)
            {
                var msg = string.Format("PluginMain.RunTool(): Exception during capture operation (useBionet={0})", useBionet);

                if (ex.Message.Contains("unknown user name or bad password"))
                {
                    // This error randomly occurs; no need to log a full stack trace
                    returnData.CloseoutMsg = msg + ", Logon failure: unknown user name or bad password";
                    returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;

                    LogError(returnData.CloseoutMsg);

                    // Set the EvalCode to 3 so that capture can be retried
                    returnData.EvalCode = EnumEvalCode.EVAL_CODE_NETWORK_ERROR_RETRY_CAPTURE;
                }
                else
                {
                    returnData.CloseoutMsg = msg;
                    returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    LogError(msg, ex);
                }
            }

            capOpTool.DetachEvents();

            LogDebug("Completed PluginMain.RunTool()");

            return returnData;
        }

        /// <summary>
        /// Initializes the capture tool
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
        /// Stores the tool version info in the database
        /// </summary>
        private bool StoreToolVersionInfo()
        {
            LogDebug("Determining tool version info");

            var toolVersionInfo = string.Empty;
            var appDirectory = CTMUtilities.GetAppDirectoryPath();

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
                    new(pluginPath)
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
    }
}
