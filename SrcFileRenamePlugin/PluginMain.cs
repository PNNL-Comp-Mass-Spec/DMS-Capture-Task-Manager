//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 11/17/2009
//*********************************************************************************************************

using System;
using CaptureTaskManager;

namespace SrcFileRenamePlugin
{
    /// <summary>
    /// Source file rename plugin
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class PluginMain : ToolRunnerBase
    {
        // Ignore Spelling: Bionet, secfso

        /// <summary>
        /// Runs the source file rename tool
        /// </summary>
        /// <returns>ToolReturnData object containing tool operation results</returns>
        public override ToolReturnData RunTool()
        {
            LogDebug("Starting SrcFileRenamePlugin.PluginMain.RunTool()");

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

            LogMessage("Renaming dataset " + mDataset);

            // Determine if instrument is on Bionet
            var capMethod = mTaskParams.GetParam("Method");
            var useBionet = string.Equals(capMethod, "secfso", StringComparison.OrdinalIgnoreCase);

            // Create the object that will perform capture operation
            var renameOpTool = new RenameOps(mMgrParams, useBionet);
            RegisterEvents(renameOpTool);

            try
            {
                LogDebug("PluginMain.RunTool(): Starting rename operation");

                returnData.CloseoutType = renameOpTool.DoOperation(mTaskParams, out var errorMessage);

                if (returnData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED)
                {
                    returnData.CloseoutMsg = errorMessage;
                }

                LogDebug("PluginMain.RunTool(): Completed rename operation");
            }
            catch (Exception ex)
            {
                LogError(string.Format(
                    "PluginMain.RunTool(): Exception during rename operation (useBionet={0})", useBionet), ex);

                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
            }

            LogDebug("Completed PluginMain.RunTool()");

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
    }
}
