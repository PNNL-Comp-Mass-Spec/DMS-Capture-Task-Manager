//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2011, Battelle Memorial Institute
// Created 03/07/2011
//
// Last modified 03/07/2011
//               04/22/2011 (DAC) - Modified to use "real" demultiplexing dll's
//*********************************************************************************************************
using System;
using System.IO;
using CaptureTaskManager;
using UIMFDemultiplexer;
using FileProcessor;
using System.Threading;

namespace ImsDemuxPlugin
{
	public static class clsDemuxTools
	{
		//*********************************************************************************************************
		//Insert general class description here
		//**********************************************************************************************************

		#region "Module variables"
			static UIMFDemultiplexer.UIMFDemultiplexer deMuxTool;
		#endregion

		#region "Events"
			// Events used for communication back to clsPluginMain, where the logging and status updates are handled
			//public static event DelDemuxErrorHandler DemuxError;
			//public static event DelDemuxMessageHandler DumuxMsg;
			//public static event DelDumuxExceptionHandler DemuxException;
			public static event DelDemuxProgressHandler DemuxProgress;
		#endregion

		#region "Constructor"
			static clsDemuxTools()
			{
				deMuxTool = new UIMFDemultiplexer.UIMFDemultiplexer();
				deMuxTool.ErrorEvent += new clsProcessFilesBaseClass.MessageEventHandler(deMuxTool_ErrorEvent);
				deMuxTool.WarningEvent += new clsProcessFilesBaseClass.MessageEventHandler(deMuxTool_WarningEvent);
				deMuxTool.MessageEvent += new clsProcessFilesBaseClass.MessageEventHandler(deMuxTool_MessageEvent);
			}
		#endregion

		#region "Methods"
			/// <summary>
			/// Performs de-multiplexing of IMS data files
			/// </summary>
			/// <param name="mgrParams">Parameters for manager operation</param>
			/// <param name="taskParams">Parameters for the assigned task</param>
			/// <returns>Enum indicating task success or failure</returns>
			public static clsToolReturnData PerformDemux(IMgrParams mgrParams, ITaskParams taskParams, string uimfFileName)
			{
				string msg = "Performing de-multiplexing, dataset " + taskParams.GetParam("Dataset");
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

				clsToolReturnData retData = new clsToolReturnData();

				string dataset = taskParams.GetParam("Dataset");

                // Make sure the working directory is empty
                string workDirPath = mgrParams.GetParam("workdir");
                ClearWorkingDirectory(workDirPath);

				// Locate data file on storage server

				string svrPath = Path.Combine(taskParams.GetParam("Storage_Vol_External"), taskParams.GetParam("Storage_Path"));
				string dsPath = Path.Combine(svrPath, taskParams.GetParam("Folder"));
				string uimfRemoteFileNamePath = Path.Combine(dsPath, uimfFileName);
				string uimfLocalFileNamePath = Path.Combine(workDirPath, dataset + ".uimf");
                
				// Copy uimf file to working directory
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Copying file from storage server");
				if (!CopyFile(uimfRemoteFileNamePath, uimfLocalFileNamePath, false))
				{
					retData.CloseoutMsg = "Error copying UIMF file to working directory";
					retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
					return retData;
				}

				// Perform demux operation
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Calling demux dll");

                try
                {
                    if (!DemultiplexFileThreaded(uimfLocalFileNamePath, dataset))
                    {
                        retData.CloseoutMsg = "Error demultiplexing UIMF file";
                        retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                        return retData;
                    }
                }
                catch (Exception ex)
                {
                    msg = "Exception calling DemultiplexFileThreaded for dataset " + dataset;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
                    retData.CloseoutMsg = "Error demultiplexing UIMF file";
                    retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    return retData;
                }

				// Rename uimf file on storage server
				msg = "Renaming uimf file on storage server";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
				// If this is a re-run, then encoded file has already been renamed
				// This is determined by looking for "encoded" in uimf file name
				if (!uimfFileName.Contains("encoded"))
				{
					if (!RenameFile(uimfRemoteFileNamePath, Path.Combine(dsPath, dataset + "_encoded.uimf")))
					{
						retData.CloseoutMsg = "Error renaming encoded UIMF file on storage server";
						retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
						return retData;
					}
				}

				// Copy demuxed file to storage server, renaming as datasetname.uimf in the process
				msg = "Copying de-mulitiplexed file to storage server";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
				string localUimfDecoded = Path.Combine(mgrParams.GetParam("workdir"), dataset + "_decoded.uimf");
				if (!CopyFile(localUimfDecoded, Path.Combine(dsPath, dataset + ".uimf"), true))
				{
					retData.CloseoutMsg = "Error copying decoded UIMF file to storage server";
					retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
					return retData;
				}

				// Delete local uimf file(s)
				msg = "Cleaning up working directory";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
				try
				{
					File.Delete(localUimfDecoded);
					File.Delete(uimfLocalFileNamePath);
				}
				catch (Exception ex)
				{
					msg = "Exception deleting working directory file(s): " + ex.Message;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
					retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
					retData.CloseoutMsg = "Problem cleaning working directory";
					return retData;
				}

				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
				retData.EvalMsg = "De-multiplexed";
				return retData;
			}	// End sub

            /// <summary>
            /// Makes sure the working directory is empty
            /// </summary>
            /// <param name="sWorkingDirectory"></param>
            private static void ClearWorkingDirectory(string sWorkingDirectory)
            {
                System.IO.DirectoryInfo diWorkDir = new System.IO.DirectoryInfo(sWorkingDirectory);

                if (diWorkDir.Exists)
                {
                    foreach (System.IO.FileInfo fiFile in diWorkDir.GetFiles())
                    {
                        try
                        {
                            fiFile.Delete();
                        }
                        catch (Exception ex)
                        {
					        string msg = "Exception deleting file '" + fiFile.Name + "' from work directory";
					        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
                        }
                        
                    }

                    foreach (System.IO.DirectoryInfo diSubFolder in diWorkDir.GetDirectories())
                    {
                        try
                        {
                            diSubFolder.Delete(true);
                        }
                        catch (Exception ex)
                        {
                            string msg = "Exception deleting folder '" + diSubFolder.Name + "' from work directory";
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
                        }
                    }
                }
                
            }

			/// <summary>
			/// Performs actual de-multiplexing operation in a separate thread
			/// </summary>
			/// <param name="inputFile">Input file name</param>
			/// <returns>Enum indicating success or failure</returns>
			private static bool DemultiplexFileThreaded(string inputFile, string datasetName)
			{
				bool success = false;

				FileInfo fi = new FileInfo(inputFile);
				string folderName = fi.DirectoryName;
				string outputFile = Path.Combine(folderName, datasetName + "_decoded.uimf");
				try
				{
					string msg = "Starting de-multiplexing, dataset " + datasetName;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);

					// Create a thread to run the demuxer
					Thread demuxThread;
					demuxThread = new Thread(new ThreadStart(() => deMuxTool.Demultiplex(inputFile, outputFile)));

                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Starting Demux Thread");

					// Start the demux thread
					demuxThread.Start();

					// Wait until the thread completes
					//TODO: Does this need a way to abort?
					while (demuxThread != null && !demuxThread.Join(5000))
					{
						if (DemuxProgress != null) DemuxProgress(deMuxTool.ProgressPercentComplete);
					}

                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Demux thread completed");

					// Check to determine if thread exited due to normal completion
                    if (deMuxTool != null && deMuxTool.ProcessingStatus == UIMFDemultiplexer.UIMFDemultiplexer.eProcessingStatus.Complete)
					{
						msg = "De-multiplexing complete, dataset " + datasetName;
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
						success = true;
					}
					else
					{
                        string errorMsg = "Unknown error";

						// Log the processing status
                        if (deMuxTool != null)
                        {
                            msg = "Demux processing status: " + deMuxTool.ProcessingStatus.ToString();
                            
                            // Get the error msg
                            errorMsg = deMuxTool.GetErrorMessage();
                            if (string.IsNullOrEmpty(errorMsg)) errorMsg = "Unknown error";

                        }
                        else
                        {
                            msg = "Demux processing status: ??? (deMuxTool is null)";
                        }

						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMsg);
						success = false;
					}
				}
				catch (Exception ex)
				{
					string msg = "Exception de-multiplexing dataset " + datasetName;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
					success = false;
				}

				return success;
			}	// End sub

			/// <summary>
			/// Copies a file
			/// </summary>
			/// <param name="sourceFileNamePath">Source file</param>
			/// <param name="TargetFileNamePath">Destination file</param>
			/// <returns></returns>
			private static bool CopyFile(string sourceFileNamePath, string TargetFileNamePath, bool overWrite)
			{
				try
				{
					File.Copy(sourceFileNamePath, TargetFileNamePath, overWrite);
					return true;
				}
				catch (Exception ex)
				{
					string msg = "Exception copying file " + sourceFileNamePath + ": " + ex.Message;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
					return false;
				}
			}	// End sub

			/// <summary>
			/// Renames a file
			/// </summary>
			/// <param name="currFileNamePath">Original file name and path</param>
			/// <param name="newFileNamePath">New file name and path</param>
			/// <returns></returns>
			private static bool RenameFile(string currFileNamePath, string newFileNamePath)
			{
				try
				{
					FileInfo fi = new FileInfo(currFileNamePath);
					fi.MoveTo(newFileNamePath);
					return true;
				}
				catch (Exception ex)
				{
					string msg = "Exception renaming file " + currFileNamePath + ": " + ex.Message;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
					return false;
				}
			}	// End sub
		#endregion

		#region "Event handlers"
			/// <summary>
			/// Logs a message from the demux dll
			/// </summary>
			/// <param name="sender"></param>
			/// <param name="e"></param>
			static void deMuxTool_MessageEvent(object sender, MessageEventArgs e)
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Demux message: " + e.Message);
			}

			/// <summary>
			/// Logs a warning from the demux dll
			/// </summary>
			/// <param name="sender"></param>
			/// <param name="e"></param>
			static void deMuxTool_WarningEvent(object sender, MessageEventArgs e)
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Demux warning: " + e.Message);
			}

			/// <summary>
			/// Logs an error from the debug dll
			/// </summary>
			/// <param name="sender"></param>
			/// <param name="e"></param>
			static void deMuxTool_ErrorEvent(object sender, MessageEventArgs e)
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Demux error: " + e.Message);
			}
		#endregion
	}	// End class
}	// End namespace
