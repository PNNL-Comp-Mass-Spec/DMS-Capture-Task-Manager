
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 11/17/2009
//
//*********************************************************************************************************

using System;
using CaptureTaskManager;

namespace SrcFileRenamePlugin
{
    public class clsPluginMain : clsToolRunnerBase
    {
        //*********************************************************************************************************
        // Main class for plugin
        //**********************************************************************************************************

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

            msg = "Renaming dataset '" + m_Dataset + "'";
            LogMessage(msg);

            // Determine if instrument is on Bionet
            var capMethod = m_TaskParams.GetParam("Method");
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
            var renameOpTool = new clsRenameOps(m_MgrParams, useBionet);
            RegisterEvents(renameOpTool);

            try
            {
                msg = "clsPluginMain.RunTool(): Starting rename operation";
                LogDebug(msg);

                string errorMessage;
                retData.CloseoutType = renameOpTool.DoOperation(m_TaskParams, out errorMessage);

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
            var appFolder = clsUtilities.GetAppFolderPath();

            if (string.IsNullOrWhiteSpace(appFolder))
            {
                LogError("GetAppFolderPath returned an empty directory path to StoreToolVersionInfo for the Source File Rename plugin");
                return false;
            }

            // Lookup the version of the Source File Rename plugin
            var strPluginPath = System.IO.Path.Combine(appFolder, "SrcFileRenamePlugin.dll");
            var bSuccess = StoreToolVersionInfoOneFile(ref strToolVersionInfo, strPluginPath);
            if (!bSuccess)
                return false;

            // Lookup the version of the Capture task manager
            var strCTMPath = System.IO.Path.Combine(appFolder, "CaptureTaskManager.exe");
            bSuccess = StoreToolVersionInfoOneFile(ref strToolVersionInfo, strCTMPath);
            if (!bSuccess)
                return false;

            // Store path to SrcFileRenamePlugin.dll in ioToolFiles
            var ioToolFiles = new System.Collections.Generic.List<System.IO.FileInfo> {
                new System.IO.FileInfo(strPluginPath)
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

        #endregion
    }
}
