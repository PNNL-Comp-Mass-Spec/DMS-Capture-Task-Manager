
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
			: base()
		{
			// Does nothing at present
		}	// End sub
		#endregion

		#region "Methods"
		/// <summary>
		/// Runs the archive and archive update step tools
		/// </summary>
		/// <returns>Enum indicating success or failure</returns>
		public override clsToolReturnData RunTool()
		{
			string msg;
			IArchiveOps archOpTool = null;
			string archiveOpDescription = string.Empty;
			mSubmittedToMyEMSL = false;
			mMyEMSLAlreadyUpToDate = false;

			msg = "Starting DatasetArchivePlugin.clsPluginMain.RunTool()";
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

			// Perform base class operations, if any
			clsToolReturnData retData = base.RunTool();
			if (retData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED) return retData;

			// Store the version info in the database
			if (!StoreToolVersionInfo())
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false");
				retData.CloseoutMsg = "Error determining tool version info";
				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
				return retData;
			}

			string instrumentName = m_TaskParams.GetParam("Instrument_Name");
			bool onlyUseMyEMSL = clsOpsBase.OnlyUseMyEMSL(instrumentName);

			if (onlyUseMyEMSL)
			{
				// Always use clsArchiveUpdate for both archiving new datasets and updating existing datasets
				archOpTool = new clsArchiveUpdate(m_MgrParams, m_TaskParams, m_StatusTools);

				if (m_TaskParams.GetParam("StepTool").ToLower() == "datasetarchive")
				{
					archiveOpDescription = "archive";
				}
				else
				{
					archiveOpDescription = "archive update";
				}

			}
			else
			{
				// Select appropriate operation tool based on StepTool specification
				if (m_TaskParams.GetParam("StepTool").ToLower() == "datasetarchive")
				{
					// This is an archive operation
					archOpTool = new clsArchiveDataset(m_MgrParams, m_TaskParams, m_StatusTools);
					archiveOpDescription = "archive";
				}
				else
				{
					// This is an archive update operation
					archOpTool = new clsArchiveUpdate(m_MgrParams, m_TaskParams, m_StatusTools);
					archiveOpDescription = "archive update";
				}
			}

			// Attach the MyEMSL Upload event handler
			archOpTool.MyEMSLUploadComplete += new MyEMSLUploadEventHandler(MyEMSLUploadCompleteHandler);

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
		}	// End sub

		/// <summary>
		/// Initializes the dataset archive tool
		/// </summary>
		/// <param name="mgrParams">Parameters for manager operation</param>
		/// <param name="taskParams">Parameters for the assigned task</param>
		/// <param name="statusTools">Tools for status reporting</param>
		public override void Setup(IMgrParams mgrParams, ITaskParams taskParams, IStatusFile statusTools)
		{
			string msg = "Starting clsPluginMain.Setup()";
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

			base.Setup(mgrParams, taskParams, statusTools);

			msg = "Completed clsPluginMain.Setup()";
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
		}	// End sub


		/// <summary>
		/// Communicates with database to store the MyEMSL upload stats
		/// </summary>
		/// <returns>True for success, False for failure</returns>
		protected bool StoreMyEMSLUploadStats(int fileCountNew, int fileCountUpdated, Int64 bytes, double uploadTimeSeconds, string statusURI, int errorCode)
		{

			bool Outcome = false;
			int ResCode = 0;

			mSubmittedToMyEMSL = true;
			if (fileCountNew == 0 && fileCountUpdated == 0)
				mMyEMSLAlreadyUpToDate = true;

			try
			{

				//Setup for execution of the stored procedure
				System.Data.SqlClient.SqlCommand MyCmd = new System.Data.SqlClient.SqlCommand();
				{
					MyCmd.CommandType = System.Data.CommandType.StoredProcedure;
					MyCmd.CommandText = SP_NAME_STORE_MYEMSL_STATS;

					MyCmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@Return", System.Data.SqlDbType.Int));
					MyCmd.Parameters["@Return"].Direction = System.Data.ParameterDirection.ReturnValue;

					MyCmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@Job", System.Data.SqlDbType.Int));
					MyCmd.Parameters["@Job"].Direction = System.Data.ParameterDirection.Input;
					MyCmd.Parameters["@Job"].Value = Convert.ToInt32(m_TaskParams.GetParam("Job"));

					MyCmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@DatasetID", System.Data.SqlDbType.Int));
					MyCmd.Parameters["@DatasetID"].Direction = System.Data.ParameterDirection.Input;
					MyCmd.Parameters["@DatasetID"].Value = Convert.ToInt32(m_TaskParams.GetParam("Dataset_ID"));

					MyCmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@Subfolder", System.Data.SqlDbType.VarChar, 128));
					MyCmd.Parameters["@Subfolder"].Direction = System.Data.ParameterDirection.Input;
					MyCmd.Parameters["@Subfolder"].Value = m_TaskParams.GetParam("OutputFolderName", string.Empty);

					MyCmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@FileCountNew", System.Data.SqlDbType.Int));
					MyCmd.Parameters["@FileCountNew"].Direction = System.Data.ParameterDirection.Input;
					MyCmd.Parameters["@FileCountNew"].Value = fileCountNew;

					MyCmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@FileCountUpdated", System.Data.SqlDbType.Int));
					MyCmd.Parameters["@FileCountUpdated"].Direction = System.Data.ParameterDirection.Input;
					MyCmd.Parameters["@FileCountUpdated"].Value = fileCountUpdated;

					MyCmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@Bytes", System.Data.SqlDbType.BigInt));
					MyCmd.Parameters["@Bytes"].Direction = System.Data.ParameterDirection.Input;
					MyCmd.Parameters["@Bytes"].Value = bytes;

					MyCmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@UploadTimeSeconds", System.Data.SqlDbType.Real));
					MyCmd.Parameters["@UploadTimeSeconds"].Direction = System.Data.ParameterDirection.Input;
					MyCmd.Parameters["@UploadTimeSeconds"].Value = (float)uploadTimeSeconds;

					MyCmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@StatusURI", System.Data.SqlDbType.VarChar, 255));
					MyCmd.Parameters["@StatusURI"].Direction = System.Data.ParameterDirection.Input;
					MyCmd.Parameters["@StatusURI"].Value = statusURI;

					MyCmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@ErrorCode", System.Data.SqlDbType.Int));
					MyCmd.Parameters["@ErrorCode"].Direction = System.Data.ParameterDirection.Input;
					MyCmd.Parameters["@ErrorCode"].Value = errorCode;
				}

				string strConnStr = m_MgrParams.GetParam("connectionstring");

				//Execute the SP (retry the call up to 4 times)
				ResCode = base.ExecuteSP(MyCmd, strConnStr, 4);

				if (ResCode == 0)
				{
					Outcome = true;
				}
				else
				{
					string Msg = "Error " + ResCode.ToString() + " storing tool version for current processing step";
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

			string strToolVersionInfo = string.Empty;
			var ioAppFileInfo = new System.IO.FileInfo(System.Reflection.Assembly.GetExecutingAssembly().Location);
			bool bSuccess;

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info");

			// Lookup the version of the Dataset Archive plugin
			string strPluginPath = System.IO.Path.Combine(ioAppFileInfo.DirectoryName, "DatasetArchivePlugin.dll");
			bSuccess = base.StoreToolVersionInfoOneFile(ref strToolVersionInfo, strPluginPath);
			if (!bSuccess)
				return false;

			// Lookup the version of the MyEMSLReader
			string strMD5StageFileCreatorPath = System.IO.Path.Combine(ioAppFileInfo.DirectoryName, "MyEMSLReader.dll");
			bSuccess = base.StoreToolVersionInfoOneFile(ref strToolVersionInfo, strMD5StageFileCreatorPath);
			if (!bSuccess)
				return false;

			// Store path to DatasetArchivePlugin.dll in ioToolFiles
			var ioToolFiles = new System.Collections.Generic.List<System.IO.FileInfo>();
			ioToolFiles.Add(new System.IO.FileInfo(strPluginPath));

			try
			{
				return base.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, false);
			}
			catch (System.Exception ex)
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " + ex.Message);
				return false;
			}
		}

		private void MyEMSLUploadCompleteHandler(object sender, MyEMSLUploadEventArgs e)
		{
			StoreMyEMSLUploadStats(e.fileCountNew, e.fileCountUpdated, e.bytes, e.uploadTimeSeconds, e.statusURI, e.errorCode);
		}


		#endregion
	}	// End class
}	// End namespace
