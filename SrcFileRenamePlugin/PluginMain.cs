//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 11/17/2009
//*********************************************************************************************************

using CaptureTaskManager;
using System;

namespace SrcFileRenamePlugin
{
    /// <summary>
    /// Source file rename plugin
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class PluginMain : ToolRunnerBase
    {
        // Ignore Spelling: Bionet, secfso

        #region "Methods"
        /// <summary>
        /// Runs the source file rename tool
        /// </summary>
        /// <returns>ToolReturnData object containing tool operation results</returns>
        public override ToolReturnData RunTool()
        {
            var msg = "Starting SrcFileRenamePlugin.PluginMain.RunTool()";
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

            msg = "Renaming dataset " + mDataset;
            LogMessage(msg);

            // Determine if instrument is on Bionet
            var capMethod = mTaskParams.GetParam("Method");
            var useBionet = string.Equals(capMethod, "secfso", StringComparison.OrdinalIgnoreCase);

            // Create the object that will perform capture operation
            var renameOpTool = new RenameOps(mMgrParams, useBionet);
            RegisterEvents(renameOpTool);

            try
            {
                msg = "PluginMain.RunTool(): Starting rename operation";
                LogDebug(msg);

                returnData.CloseoutType = renameOpTool.DoOperation(mTaskParams, out var errorMessage);

                if (returnData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED)
                {
                    returnData.CloseoutMsg = errorMessage;
                }

                msg = "PluginMain.RunTool(): Completed rename operation";
                LogDebug(msg);
            }
            catch (Exception ex)
            {
                msg = "PluginMain.RunTool(): Exception during rename operation (useBionet=" + useBionet + ")";
                LogError(msg, ex);
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
            }

            msg = "Completed PluginMain.RunTool()";
            LogDebug(msg);

            return returnData;
        }

        /// <summary>
        /// Initializes the rename tool
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
                LogError("GetAppDirectoryPath returned an empty directory path to StoreToolVersionInfo for the Source File Rename plugin");
                return false;
            }

            // Lookup the version of the Source File Rename plugin
            var pluginPath = System.IO.Path.Combine(appDirectory, "SrcFileRenamePlugin.dll");
            var success = StoreToolVersionInfoOneFile(ref toolVersionInfo, pluginPath);
            if (!success)
            {
                return false;
            }

            // Lookup the version of the Capture task manager
            var ctmPath = System.IO.Path.Combine(appDirectory, "CaptureTaskManager.exe");
            success = StoreToolVersionInfoOneFile(ref toolVersionInfo, ctmPath);
            if (!success)
            {
                return false;
            }

            // Store path to SrcFileRenamePlugin.dll in toolFiles
            var toolFiles = new System.Collections.Generic.List<System.IO.FileInfo>
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

        #endregion
    }
}
