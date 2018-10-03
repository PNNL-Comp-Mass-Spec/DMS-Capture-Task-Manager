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
    public class clsPluginMain : clsToolRunnerBase
    {

        #region "Methods"
        /// <summary>
        /// Runs the source file rename tool
        /// </summary>
        /// <returns>clsToolReturnData object containing tool operation results</returns>
        public override clsToolReturnData RunTool()
        {
            var msg = "Starting SrcFileRenamePlugin.clsPluginMain.RunTool()";
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

            msg = "Renaming dataset '" + mDataset + "'";
            LogMessage(msg);

            // Determine if instrument is on Bionet
            var capMethod = mTaskParams.GetParam("Method");
            bool useBionet;
            if (capMethod.ToLower() == "secfso")
            {
                useBionet = true;
            }
            else
            {
                useBionet = false;
            }

            // Create the object that will perform capture operation
            var renameOpTool = new clsRenameOps(mMgrParams, useBionet);
            RegisterEvents(renameOpTool);

            try
            {
                msg = "clsPluginMain.RunTool(): Starting rename operation";
                LogDebug(msg);

                retData.CloseoutType = renameOpTool.DoOperation(mTaskParams, out var errorMessage);

                if (retData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED)
                    retData.CloseoutMsg = errorMessage;

                msg = "clsPluginMain.RunTool(): Completed rename operation";
                LogDebug(msg);
            }
            catch (Exception ex)
            {
                msg = "clsPluginMain.RunTool(): Exception during rename operation (useBionet=" + useBionet + ")";
                LogError(msg, ex);
                retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
            }

            msg = "Completed clsPluginMain.RunTool()";
            LogDebug(msg);

            return retData;
        }

        /// <summary>
        /// Initializes the rename tool
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
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfo()
        {
            LogDebug("Determining tool version info");

            var strToolVersionInfo = string.Empty;
            var appDirectory = clsUtilities.GetAppDirectoryPath();

            if (string.IsNullOrWhiteSpace(appDirectory))
            {
                LogError("GetAppDirectoryPath returned an empty directory path to StoreToolVersionInfo for the Source File Rename plugin");
                return false;
            }

            // Lookup the version of the Source File Rename plugin
            var strPluginPath = System.IO.Path.Combine(appDirectory, "SrcFileRenamePlugin.dll");
            var bSuccess = StoreToolVersionInfoOneFile(ref strToolVersionInfo, strPluginPath);
            if (!bSuccess)
                return false;

            // Lookup the version of the Capture task manager
            var strCTMPath = System.IO.Path.Combine(appDirectory, "CaptureTaskManager.exe");
            bSuccess = StoreToolVersionInfoOneFile(ref strToolVersionInfo, strCTMPath);
            if (!bSuccess)
                return false;

            // Store path to SrcFileRenamePlugin.dll in toolFiles
            var toolFiles = new System.Collections.Generic.List<System.IO.FileInfo> {
                new System.IO.FileInfo(strPluginPath)
            };

            try
            {
                return SetStepTaskToolVersion(strToolVersionInfo, toolFiles, false);
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
