//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2011, Battelle Memorial Institute
// Created 03/07/2011
//
// Last modified 03/07/2011
//*********************************************************************************************************
using System;
using System.IO;
using CaptureTaskManager;

namespace ImsDemuxPlugin
{
	public static class clsDemuxTools
	{
		//*********************************************************************************************************
		//Insert general class description here
		//**********************************************************************************************************

		#region "Methods"
			/// <summary>
			/// Performs de-multiplexing of IMS data files
			/// </summary>
			/// <param name="mgrParams">Parameters for manager operation</param>
			/// <param name="taskParams">Parameters for the assigned task</param>
			/// <returns>Enum indicating task success or failure</returns>
			public static clsToolReturnData PerformDemux(IMgrParams mgrParams, ITaskParams taskParams)
			{
				string msg = "Performing de-multiplexing, dataset " + taskParams.GetParam("Dataset");
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

				clsToolReturnData retData = new clsToolReturnData();

				string dataset = taskParams.GetParam("Dataset");
				string uimfFileName = dataset + ".uimf";

				// Locate data file on storage server
				string svrPath = Path.Combine(taskParams.GetParam("Storage_Vol_External"), taskParams.GetParam("Storage_Path"));
				string dsPath = Path.Combine(svrPath, taskParams.GetParam("Folder"));
				string uimfRemoteFileNamePath = Path.Combine(dsPath, uimfFileName);
				string uimfLocalFileNamePath = Path.Combine(mgrParams.GetParam("workdir"), uimfFileName);

				// Copy uimf file to working directory
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Copying file from storage server");
				if (!CopyFile(uimfRemoteFileNamePath, uimfLocalFileNamePath))
				{
					retData.CloseoutMsg = "Error copying UIMF file to working directory";
					retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
					return retData;
				}

				// Perform demux operation
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Calling demux dll");
				if (!DemultiplexFile(uimfLocalFileNamePath, dataset))
				{
					retData.CloseoutMsg = "Error demultiplexing UIMF file";
					retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
					return retData;
				}

				// Rename uimf file on storage server
				msg = "Renaming uimf file on storage server";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
				if (!RenameFile(uimfRemoteFileNamePath, Path.Combine(dsPath, dataset + "_encoded.uimf")))
				{
					retData.CloseoutMsg = "Error renaming encoded UIMF file on storage server";
					retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
					return retData;
				}

				// Copy demuxed file to storage server, renaming as datasetname.uimf in the process
				msg = "Copying de-mulitiplexed file to storage server";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
				string localUimfDecoded = Path.Combine(mgrParams.GetParam("workdir"), dataset + "_decoded.uimf");
				if (!CopyFile(localUimfDecoded, uimfRemoteFileNamePath))
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
			/// Performs actual de-multiplexing operation
			/// </summary>
			/// <param name="inputFile">Input file name</param>
			/// <returns>Enum indicating success or failure</returns>
			private static bool DemultiplexFile(string inputFile, string datasetName)
			{
				FileInfo fi = new FileInfo(inputFile);
				string folderName = fi.DirectoryName;
				string outputFile = Path.Combine(folderName, datasetName + "_decoded.uimf");
				try
				{
					string msg = "Starting de-multiplexing, dataset " + datasetName;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
					UIMFDemultiplexer.UIMFDemultiplexer.demultiplex(inputFile, outputFile);
					msg = "De-multiplexing complete, dataset " + datasetName;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
					return true;
				}
				catch (Exception ex)
				{
					string msg = "Exception de-multiplexing dataset " + datasetName;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
					return false;
				}
			}	// End sub

			/// <summary>
			/// Copies a file
			/// </summary>
			/// <param name="sourceFileNamePath">Source file</param>
			/// <param name="TargetFileNamePath">Destination file</param>
			/// <returns></returns>
			private static bool CopyFile(string sourceFileNamePath, string TargetFileNamePath)
			{
				try
				{
					File.Copy(sourceFileNamePath, TargetFileNamePath);
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
	}	// End class
}	// End namespace
