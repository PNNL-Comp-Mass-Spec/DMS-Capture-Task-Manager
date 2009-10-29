
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 10/20/2009
//
// Last modified 10/20/2009
//*********************************************************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using CaptureTaskManager;
using System.IO;
using PRISM.Files;

namespace DatasetArchivePlugin
{
	class clsUpdateArchive : IArchiveOps
	{
		//*********************************************************************************************************
		//Tools to perform archive update operations
		//**********************************************************************************************************

		#region "Class variables"
		#endregion

		#region "Properties"
		#endregion

		#region "Constructors"
			/// <summary>
			/// Constructor
			/// </summary>
			/// <param name="mgrParams">IMgrParams object holding manager parameters</param>
			/// <param name="taskParams">ITaskParams object containing task parameters</param>
			public clsUpdateArchive(IMgrParams mgrParams, ITaskParams taskParams)
			{
				m_MgrParams = mgrParams;
				m_TaskParams = taskParams;
			}	// End sub
		#endregion

		#region "Methods"
			/// <summary>
			/// Performs an archive update task (Implements IArchiveOps.PerformTask)
			/// </summary>
			/// <returns>TRUE for success, FALSE for failure</returns>
			public bool PerformTask()
			{
				string tempVol = "";
				string dsNamePath = "";
				string archiveNamePath = "";
				string sambaNamePath = "";
				string msg = "";
				clsFtpOperations ftpTools = null;
				string user = m_MgrParams.GetParam("username");
				string pwd = m_MgrParams.GetParam("userpwd");
				bool useTLS = bool.Parse(m_MgrParams.GetParam("usetls"));
				int serverPort = int.Parse(m_MgrParams.GetParam("serverport"));
				int ftpTimeOut = int.Parse(m_MgrParams.GetParam("timeout"));
				bool ftpPassive = bool.Parse(m_MgrParams.GetParam("passive"));
				bool ftpRestart = bool.Parse(m_MgrParams.GetParam("restart"));
				List<clsJobData> filesToUpdate = new List<clsJobData>();

				msg = "Beginning update, dataset " + m_TaskParams.GetParam("dataset");
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.INFO, msg);

				//Set client/server perspective & setup paths
				if (m_MgrParams.GetParam("perspective").ToLower() == "client")
				{
					tempVol = m_TaskParams.GetParam("Storage_Vol_External");
				}
				else
				{
					tempVol = m_TaskParams.GetParam("Storage_Vol");
				}
				dsNamePath = Path.Combine(Path.Combine(tempVol, m_TaskParams.GetParam("Storage_Path")),
									m_TaskParams.GetParam("Folder"));	//Path to dataset on storage server
				archiveNamePath = clsFileTools.CheckTerminator(m_TaskParams.GetParam("Archive_Path"), true, "/") +
									m_TaskParams.GetParam("Folder");		//Path to dataset for FTP operations
				sambaNamePath = Path.Combine(m_TaskParams.GetParam("Archive_Network_Share_Path"),
									m_TaskParams.GetParam("Folder"));	//Path to dataset for Samba operations

				//Verify dataset is in specified location
				if (!VerifyDSPresent(dsNamePath))
				{
					msg = "Dataset folder " + dsNamePath + " not found";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
					LogUpdateFailed(m_TaskParams.GetParam("dataset"));
					return false;
				}

				//Verify dataset directory exists in archive
				if (!Directory.Exists(sambaNamePath))
				{
					msg = "Archive folder " + sambaNamePath + " not found";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
					LogUpdateFailed(m_TaskParams.GetParam("dataset"));
					return false;
				}

				//Determine what files need updating
				UpdateTools = new clsUpdateOps();
				filesToUpdate = UpdateTools.CompareDatasetFolders(m_TaskParams.GetParam("dataset"), dsNamePath, sambaNamePath);

				//Check for errors
				if (filesToUpdate == null)
				{
					//There was a problem; log entries were made by UpdateTools
					LogUpdateFailed(m_TaskParams.GetParam("dataset"));
					return false;
				}

				//Check to see if any files needing update were found
				if (filesToUpdate.Count < 1)
				{
					//No files requiring update were found. Human intervention may be required.
					msg = "No files needing update found for dataset " + m_TaskParams.GetParam("dataset");
					WriteLog(LoggerTypes.LogDb, LogLevels.WARN, msg);
					return true;
				}

				try
				{
					//Open FTP client
					ftpTools = new clsFTPOperations(m_TaskParams.GetParam("Archive_Server_Name"), user, pwd, useTLS, serverPort);
					{
						ftpTools.FTPPassive = ftpPassive;
						ftpTools.FTPRestart = ftpRestart;
						ftpTools.FTPTimeOut = ftpTimeOut;
						ftpTools.UseLogFile = (bool)m_MgrParams.GetParam("ftplogging");
					}
					if (!ftpTools.OpenFTPConnection())
					{
						WriteLog(LoggerTypes.LogFile, LogLevels.ERROR, ftpTools.ErrMsg);
						LogUpdateFailed(m_TaskParams.GetParam("dataset"));
						ftpTools.CloseFTPConnection();
						if (!string.IsNullOrEmpty(ftpTools.ErrMsg)) WriteLog(LoggerTypes.LogFile, LogLevels.ERROR, ftpTools.ErrMsg);
						return false;
					}

					//Copy updated files to archive
					if (!UpdateArchive(filesToUpdate, archiveNamePath, true, ftpTools))
					{
						msg = "Error updating archive, dataset " + m_TaskParams.GetParam("dataset");
						WriteLog(LoggerTypes.LogDb, LogLevels.ERROR, msg);
						LogUpdateFailed(m_TaskParams.GetParam("dataset"));
						ftpTools.CloseFTPConnection();
						if (!string.IsNullOrEmpty(ftpTools.ErrMsg)) WriteLog(LoggerTypes.LogFile, LogLevels.ERROR, ftpTools.ErrMsg);
						return false;
					}

					//Close the archive server
					ftpTools.CloseFTPConnection();
					if (!string.IsNullOrEmpty(ftpTools.ErrMsg)) WriteLog(LoggerTypes.LogFile, LogLevels.ERROR, ftpTools.ErrMsg);
				}
				catch (Exception ex)
				{
					msg = "clsArchUpdateOps.PerformTask, exception performing task: " + ex.Message;
					WriteLog(LoggerTypes.LogFile, LogLevels.ERROR, msg);
					LogUpdateFailed(m_TaskParams.GetParam("dataset"));
					return false;
				}

				//If we got to here, then everything worked OK
				msg = "Update complete, dataset " + m_TaskParams.GetParam("dataset");
				WriteLog(LoggerTypes.LogDb, LogLevels.INFO, msg);

				return true;
			}	// End sub
		#endregion
	}	// End class
}	// End namespace
