
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 10/08/2009
//
// Last modified 10/08/2009
//						02/03/2010 (DAC) - Modified logging to include job number
//*********************************************************************************************************
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Reflection;
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

		#region "Class-wide Variables"
		bool mSubmittedToMyEMSL;
		bool mMyEMSLAlreadyUpToDate;

		#endregion

		#region "Constructors"
		public clsPluginMain()
		{
			// Does nothing at present
		}
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
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

			// Perform base class operations, if any
			var retData = base.RunTool();
			if (retData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED) return retData;

			// Store the version info in the database
			if (!StoreToolVersionInfo())
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false");
				retData.CloseoutMsg = "Error determining tool version info";
				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
				return retData;
			}

			// Always use clsArchiveUpdate for both archiving new datasets and updating existing datasets
			IArchiveOps archOpTool = new clsArchiveUpdate(m_MgrParams, m_TaskParams, m_StatusTools);

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

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
			if (archOpTool.PerformTask())
			{
				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
			}
			else
			{
				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
				retData.CloseoutMsg = archOpTool.ErrMsg;
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
				else
				{
					retData.EvalCode = EnumEvalCode.EVAL_CODE_SKIPPED_MYEMSL_UPLOAD;
				}
			}

			msg = "Completed " + archiveOpDescription + ", job " + m_Job;
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);

			msg = "Completed clsPluginMain.RunTool()";
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

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
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

			base.Setup(mgrParams, taskParams, statusTools);

			msg = "Completed clsPluginMain.Setup()";
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
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

				//Setup for execution of the stored procedure
				var MyCmd = new SqlCommand();
				{
					MyCmd.CommandType = CommandType.StoredProcedure;
					MyCmd.CommandText = SP_NAME_STORE_MYEMSL_STATS;

					MyCmd.Parameters.Add(new SqlParameter("@Return", SqlDbType.Int));
					MyCmd.Parameters["@Return"].Direction = ParameterDirection.ReturnValue;

					MyCmd.Parameters.Add(new SqlParameter("@Job", SqlDbType.Int));
					MyCmd.Parameters["@Job"].Direction = ParameterDirection.Input;
                    MyCmd.Parameters["@Job"].Value = m_TaskParams.GetParam("Job", 0);

					MyCmd.Parameters.Add(new SqlParameter("@DatasetID", SqlDbType.Int));
					MyCmd.Parameters["@DatasetID"].Direction = ParameterDirection.Input;
					MyCmd.Parameters["@DatasetID"].Value = m_TaskParams.GetParam("Dataset_ID", 0);

					MyCmd.Parameters.Add(new SqlParameter("@Subfolder", SqlDbType.VarChar, 128));
					MyCmd.Parameters["@Subfolder"].Direction = ParameterDirection.Input;
					MyCmd.Parameters["@Subfolder"].Value = m_TaskParams.GetParam("OutputFolderName", string.Empty);

					MyCmd.Parameters.Add(new SqlParameter("@FileCountNew", SqlDbType.Int));
					MyCmd.Parameters["@FileCountNew"].Direction = ParameterDirection.Input;
					MyCmd.Parameters["@FileCountNew"].Value = fileCountNew;

					MyCmd.Parameters.Add(new SqlParameter("@FileCountUpdated", SqlDbType.Int));
					MyCmd.Parameters["@FileCountUpdated"].Direction = ParameterDirection.Input;
					MyCmd.Parameters["@FileCountUpdated"].Value = fileCountUpdated;

					MyCmd.Parameters.Add(new SqlParameter("@Bytes", SqlDbType.BigInt));
					MyCmd.Parameters["@Bytes"].Direction = ParameterDirection.Input;
					MyCmd.Parameters["@Bytes"].Value = bytes;

					MyCmd.Parameters.Add(new SqlParameter("@UploadTimeSeconds", SqlDbType.Real));
					MyCmd.Parameters["@UploadTimeSeconds"].Direction = ParameterDirection.Input;
					MyCmd.Parameters["@UploadTimeSeconds"].Value = (float)uploadTimeSeconds;

					MyCmd.Parameters.Add(new SqlParameter("@StatusURI", SqlDbType.VarChar, 255));
					MyCmd.Parameters["@StatusURI"].Direction = ParameterDirection.Input;
					MyCmd.Parameters["@StatusURI"].Value = statusURI;

					MyCmd.Parameters.Add(new SqlParameter("@ErrorCode", SqlDbType.Int));
					MyCmd.Parameters["@ErrorCode"].Direction = ParameterDirection.Input;
					MyCmd.Parameters["@ErrorCode"].Value = errorCode;

                    MyCmd.Parameters.Add(new SqlParameter("@UsedTestInstance", SqlDbType.TinyInt));
                    MyCmd.Parameters["@UsedTestInstance"].Direction = ParameterDirection.Input;

				    if (usedTestInstance)
                        MyCmd.Parameters["@UsedTestInstance"].Value = 1;
                    else
                        MyCmd.Parameters["@UsedTestInstance"].Value = 0;

                    MyCmd.Parameters.Add(new SqlParameter("@EUSInstrumentID", SqlDbType.Int));
					MyCmd.Parameters["@EUSInstrumentID"].Direction = ParameterDirection.Input;
					MyCmd.Parameters["@EUSInstrumentID"].Value = eusInstrumentID;

                    MyCmd.Parameters.Add(new SqlParameter("@EUSProposalID", SqlDbType.VarChar, 10));
					MyCmd.Parameters["@EUSProposalID"].Direction = ParameterDirection.Input;
					MyCmd.Parameters["@EUSProposalID"].Value = eusProposalID;

                    MyCmd.Parameters.Add(new SqlParameter("@EUSUploaderID", SqlDbType.Int));
					MyCmd.Parameters["@EUSUploaderID"].Direction = ParameterDirection.Input;
					MyCmd.Parameters["@EUSUploaderID"].Value = eusUploaderID;

				}

				// Execute the SP (retry the call up to 4 times)
                var ResCode = CaptureDBProcedureExecutor.ExecuteSP(MyCmd, 4);

				if (ResCode == 0)
				{
					Outcome = true;
				}
				else
				{
					var Msg = "Error " + ResCode + " storing MyEMSL Upload Stats";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg);
					Outcome = false;
				}

			}
			catch (Exception ex)
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception storing the MyEMSL upload stats: " + ex.Message);
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

			var strToolVersionInfo = string.Empty;
			var ioAppFileInfo = new FileInfo(Assembly.GetExecutingAssembly().Location);

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info");

			if (string.IsNullOrEmpty(ioAppFileInfo.DirectoryName))
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Cannot determine the parent folder name of DLL " + Assembly.GetExecutingAssembly().FullName);
				return false;
			}

			// Lookup the version of the Dataset Archive plugin
			var strPluginPath = Path.Combine(ioAppFileInfo.DirectoryName, "DatasetArchivePlugin.dll");
			var bSuccess = base.StoreToolVersionInfoOneFile(ref strToolVersionInfo, strPluginPath);
			if (!bSuccess)
				return false;

			// Lookup the version of the MyEMSLReader
			var strMD5StageFileCreatorPath = Path.Combine(ioAppFileInfo.DirectoryName, "MyEMSLReader.dll");
			bSuccess = base.StoreToolVersionInfoOneFile(ref strToolVersionInfo, strMD5StageFileCreatorPath);
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
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " + ex.Message);
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
	}	// End class
}	// End namespace
