
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 10/20/2009
//
// Last modified 10/20/2009
//*********************************************************************************************************
using System;
using CaptureTaskManager;
using System.IO;
using PRISM.Files;

namespace DatasetArchivePlugin
{
	class clsOpsBase : IArchiveOps
	{
		//*********************************************************************************************************
		// Base class for archive and archive update operations classes. This class should always be overridden.
		//**********************************************************************************************************

		#region "Constants"
			protected const string ARCHIVE = "Archive ";
			protected const string UPDATE = "Archive update ";
		#endregion

		#region "Class variables"
			protected IMgrParams m_MgrParams;
			protected ITaskParams m_TaskParams;
			protected string m_ErrMsg;
			protected string m_TempVol;
			protected string m_DSNamePath;
			protected string m_ArchiveNamePath;
			protected string m_Msg;
			protected clsFtpOperations m_FtpTools;
			protected string m_User;
			protected string m_Pwd;
			protected bool m_UseTls;
			protected int m_ServerPort;
			protected int m_FtpTimeOut;
			protected bool m_FtpPassive;
			protected bool m_FtpRestart;
			protected bool m_ConnectionOpen = false;
			protected string m_ArchiveOrUpdate;
		#endregion

		#region "Properties"
			/// <summary>
			/// Implements IArchiveOps.ErrMsg
			/// </summary>
			public string ErrMsg
			{
				get { return m_ErrMsg;	}
			}
		#endregion

		#region "Constructors"
			public clsOpsBase(IMgrParams MgrParams, ITaskParams TaskParams)
			{
				m_MgrParams = MgrParams;
				m_TaskParams = TaskParams;
				m_User = m_MgrParams.GetParam("username");
				m_Pwd = m_MgrParams.GetParam("userpwd");
				m_UseTls = bool.Parse(m_MgrParams.GetParam("usetls"));
				m_ServerPort = int.Parse(m_MgrParams.GetParam("serverport"));
				m_FtpTimeOut = int.Parse(m_MgrParams.GetParam("timeout"));
				m_FtpPassive = bool.Parse(m_MgrParams.GetParam("passive"));
				m_FtpRestart = bool.Parse(m_MgrParams.GetParam("restart"));
				if (m_TaskParams.GetParam("steptool") == "DatasetArchive")
				{
					m_ArchiveOrUpdate = ARCHIVE;
				}
				else
				{
					m_ArchiveOrUpdate = UPDATE;
				}
			}	// End sub
		#endregion

		#region "Methods"
			/// <summary>
			/// Sets up to perform an archive or update task (Implements IArchiveOps.PerformTask)
			/// Must be overridden in derived class
			/// </summary>
			/// <returns>TRUE for success, FALSE for failure</returns>
			public virtual bool PerformTask()
			{
				//Set client/server perspective & setup paths
				if (m_MgrParams.GetParam("perspective").ToLower() == "client")
				{
					m_TempVol = m_TaskParams.GetParam("Storage_Vol_External");
				}
				else
				{
					m_TempVol = m_TaskParams.GetParam("Storage_Vol");
				}
				m_DSNamePath = Path.Combine(Path.Combine(m_TempVol, m_TaskParams.GetParam("Storage_Path")),
									m_TaskParams.GetParam("Folder"));	//Path to dataset on storage server
				m_ArchiveNamePath = clsFileTools.CheckTerminator(m_TaskParams.GetParam("Archive_Path"), true, "/") +
									m_TaskParams.GetParam("Folder");		//Path to dataset for FTP operations

				//Verify dataset is in specified location
				if (!VerifyDSPresent(m_DSNamePath))
				{
					m_Msg = "Dataset folder " + m_DSNamePath + " not found";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg);
					LogOperationFailed(m_TaskParams.GetParam("dataset"));
					return false;
				}

				// Got to here, everything's OK, so let let the derived class take over
				return true;
			}	// End sub

			/// <summary>
			/// Verifies specified dataset is present
			/// </summary>
			/// <param name="DSNamePath">Fully qualified path to dataset folder</param>
			/// <returns>TRUE if dataset folder is present; otherwise FALSE</returns>
			protected bool VerifyDSPresent(string dsNamePath)
			{
				//Verifies specified dataset is present
				if (Directory.Exists(dsNamePath))
				{
					return true;
				}
				else
				{
					return false;
				}
			}	// End sub

			/// <summary>
			/// Writes a database log entry for a failed archive operation
			/// </summary>
			/// <param name="DSName">Name of dataset</param>
			protected void LogOperationFailed(string dsName)
			{
				string msg = m_ArchiveOrUpdate + "failed, dataset " + dsName;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
			}	// End sub

			/// <summary>
			/// Opens the archive server connection
			/// </summary>
			/// <param name="archiveOrUpdate">Indicates whether this is an archive or update operation for logging</param>
			/// <returns>TRUE for success; otherwise FALSE</returns>
			protected bool OpenArchiveServer()
			{
				try
				{
					m_FtpTools = new clsFtpOperations(m_TaskParams.GetParam("Archive_Server"), m_User, m_Pwd,
																	m_UseTls, m_ServerPort);
					
					// Set the tool parameters
					m_FtpTools.FtpPassive = m_FtpPassive;
					m_FtpTools.FtpRestart = m_FtpRestart;
					m_FtpTools.FtpTimeOut = m_FtpTimeOut;
					m_FtpTools.UseLogFile=bool.Parse(m_MgrParams.GetParam("ftplogging"));

					// Open the connection (I hope!)
					if (m_FtpTools.OpenFTPConnection())
					{
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile,clsLogTools.LogLevels.INFO,"Archive connection opened");
						m_ConnectionOpen=true;
						return true;
					}
					else
					{
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile,clsLogTools.LogLevels.ERROR,m_FtpTools.ErrMsg);
						m_FtpTools.CloseFTPConnection();
						if (m_FtpTools.ErrMsg != "")
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_FtpTools.ErrMsg);
						m_Msg = "clsNewArchive.PerformTask: closed FTP connection";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_Msg);
						LogOperationFailed(m_TaskParams.GetParam("dataset"));
						m_ConnectionOpen=false;
						return false;
					}
				}
				catch (Exception ex)
				{
					m_Msg = "clsOpsBase.OpenArchiveServer: Exception opening server connection";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile,clsLogTools.LogLevels.ERROR,m_Msg,ex);
					LogOperationFailed(m_TaskParams.GetParam("dataset"));
					return false;
				}
			}	// End sub

			/// <summary>
			/// Closes an archive connection
			/// </summary>
			/// <param name="archiveOrUpdate"></param>
			/// <returns></returns>
			protected bool CloseArchiveServer()
			{
				try
				{
					m_FtpTools.CloseFTPConnection();
					if (m_FtpTools.ErrMsg == "")
					{
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Closed FTP connection");
						m_ConnectionOpen = false;
						return true;
					}
					else
					{
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_FtpTools.ErrMsg);
						return false;
					}
				}
				catch (Exception ex)
				{
					m_Msg = "clsOpsBase.OpenArchiveServer: Exception closing server connection";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg, ex);
					LogOperationFailed(m_TaskParams.GetParam("dataset"));
					return false;
				}
			}	// End sub

			/// <summary>
			/// General method for copying a folder to the archive
			/// </summary>
			/// <param name="sourceFolder">Folder name/path to copy</param>
			/// <param name="destFolder">Folder name/path on archive</param>
			/// <returns></returns>
			protected bool CopyOneFolderToArchive(string sourceFolder, string destFolder)
			{
				// Open archive connection
				if (!OpenArchiveServer()) return false;

				// Copy specified folder to archive
				try
				{
					if (!m_FtpTools.CopyDirectory(sourceFolder, destFolder, true))
					{
						m_Msg = "Error copying folder " + sourceFolder + ": error " + m_FtpTools.ErrMsg;
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg);
						LogOperationFailed(m_TaskParams.GetParam("dataset"));
						CloseArchiveServer();
						return false;
					}
				}
				catch (Exception ex)
				{
					m_Msg = "clsOpsBase.PerformTask: Exception copying  folder " + sourceFolder;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg, ex);
					LogOperationFailed(m_TaskParams.GetParam("dataset"));
					CloseArchiveServer();
					return false;
				}

				m_Msg = "Copied folder " + sourceFolder + " to archive";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, m_Msg);

				// Close the archive connection
				if (!CloseArchiveServer()) return false;

				// Finished successfully
				return true;
			}	// End sub
		#endregion
	}	// End class
}	// End namespace
