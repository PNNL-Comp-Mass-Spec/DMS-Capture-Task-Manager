
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
using PRISM.Files;
using MD5StageFileCreator;
using System.Collections.Generic;
using System.IO;

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
		protected IStatusFile m_StatusTools;

		protected string m_ErrMsg = string.Empty;
		protected string m_WarningMsg = string.Empty;
		protected string m_DSNamePath;
		
		[Obsolete("No longer needed since using MyEMSL")]
		protected string m_ArchiveNamePath;
		protected string m_Msg;
		protected clsFtpOperations m_FtpTools;

		protected bool m_MyEmslUploadSuccess;

		protected string m_User;
		protected string m_Pwd;
		protected bool m_UseTls;
		protected int m_ServerPort;
		protected int m_FtpTimeOut;
		protected bool m_FtpPassive;
		protected bool m_FtpRestart;
		protected bool m_ConnectionOpen = false;
		protected string m_ArchiveOrUpdate;
		protected string m_DatasetName = string.Empty;

		protected System.DateTime mLastStatusUpdateTime = System.DateTime.UtcNow;
		protected System.DateTime mLastProgressUpdateTime = System.DateTime.UtcNow;

		protected string mMostRecentLogMessage = string.Empty;
		protected System.DateTime mMostRecentLogTime = System.DateTime.UtcNow;

		protected clsMD5StageFileCreator mMD5StageFileCreator;
		protected clsFileTools m_FileTools;

		#endregion

		#region "Properties"
		/// <summary>
		/// Implements IArchiveOps.ErrMsg
		/// </summary>
		public string ErrMsg
		{
			get { return m_ErrMsg; }
		}

		public string WarningMsg
		{
			get { return m_WarningMsg; }
		}

		#endregion

		#region "Constructors"
		public clsOpsBase(IMgrParams MgrParams, ITaskParams TaskParams, IStatusFile StatusTools)
		{
			m_MgrParams = MgrParams;
			m_TaskParams = TaskParams;
			m_StatusTools = StatusTools;

			m_User = m_MgrParams.GetParam("username");
			m_Pwd = m_MgrParams.GetParam("userpwd");
			m_UseTls = bool.Parse(m_MgrParams.GetParam("usetls"));
			m_ServerPort = int.Parse(m_MgrParams.GetParam("serverport"));
			m_FtpTimeOut = int.Parse(m_MgrParams.GetParam("timeout"));
			m_FtpPassive = bool.Parse(m_MgrParams.GetParam("passive"));
			m_FtpRestart = bool.Parse(m_MgrParams.GetParam("restart"));

			

			if (m_TaskParams.GetParam("StepTool") == "DatasetArchive")
			{
				m_ArchiveOrUpdate = ARCHIVE;
			}
			else
			{
				m_ArchiveOrUpdate = UPDATE;
			}

			// Instantiate m_FileTools
			m_FileTools = new PRISM.Files.clsFileTools(m_MgrParams.GetParam("MgrName", "CaptureTaskManager"), 1);

		}	// End sub
		#endregion

		#region "Methods"

		public static bool OnlyUseMyEMSL(string instrumentName)
		{
			/*
			var lstExclusionPrefix = new List<string>();

			//lstExclusionPrefix.Add("DMS_Pipeline_Data");
			//                  .Add("QExact");
			//                  .Add("QTrap");
			//                  .Add("VOrbi05");
			//                  .Add("VOrbiETD03");

			foreach (string prefix in lstExclusionPrefix)
			{
				if (instrumentName.StartsWith(prefix))
					return false;
			}
			*/

			return true;
			
		}

		/// <summary>
		/// Sets up to perform an archive or update task (Implements IArchiveOps.PerformTask)
		/// Must be overridden in derived class
		/// </summary>
		/// <returns>TRUE for success, FALSE for failure</returns>
		public virtual bool PerformTask()
		{
			m_DatasetName = m_TaskParams.GetParam("Dataset");

			string instrumentName = m_TaskParams.GetParam("Instrument_Name");
			bool onlyUseMyEMSL = OnlyUseMyEMSL(instrumentName);

			// Set client/server perspective & setup paths
			string baseStoragePath;
			if (m_MgrParams.GetParam("perspective").ToLower() == "client")
			{
				baseStoragePath = m_TaskParams.GetParam("Storage_Vol_External");
			}
			else
			{
				baseStoragePath = m_TaskParams.GetParam("Storage_Vol");
			}

			//Path to dataset on storage server
			m_DSNamePath = Path.Combine(Path.Combine(baseStoragePath, m_TaskParams.GetParam("Storage_Path")), m_TaskParams.GetParam("Folder"));

			//Path to dataset for FTP operations
			m_ArchiveNamePath = clsFileTools.CheckTerminator(m_TaskParams.GetParam("Archive_Path"), true, "/") + m_TaskParams.GetParam("Folder");		

			//Verify dataset is in specified location
			if (!VerifyDSPresent(m_DSNamePath))
			{
				m_Msg = "Dataset folder " + m_DSNamePath + " not found";
				m_ErrMsg = string.Copy(m_Msg);
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg);
				LogOperationFailed(m_DatasetName);
				return false;
			}

			if (!onlyUseMyEMSL)
			{
				// Initialize the MD5 stage file creator
				InitializeMD5StageFileCreator();
			}

			// Got to here, everything's OK, so let let the derived class take over
			return true;

		}	// End sub

		protected string AppendToString(string text, string append)
		{
			if (string.IsNullOrEmpty(text))
				text = string.Empty;
			else
				text += "; ";

			return text + append;
		}

		protected bool UploadToMyEMSLWithRetry(int maxAttempts, bool recurse)
		{
			bool bSuccess = false;
			int iAttempts = 0;
			m_MyEmslUploadSuccess = false;

			if (maxAttempts < 1)
				maxAttempts = 1;

			if (Environment.UserName.ToLower() != "svc-dms")
			{
				// The current user is not svc-dms
				// Uploaded files would be associated with the wrong username and thus would not be visible to all DMS Users
				m_ErrMsg = "Files must be uploaded to MyEMSL using the svc-dms account; aborting";
				Console.WriteLine(m_ErrMsg);
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_ErrMsg);
				return false;
			}
			
			while (!bSuccess && iAttempts < maxAttempts)
			{
				iAttempts += 1;
				bSuccess = UploadToMyEMSL(recurse);

				if (!bSuccess && iAttempts < maxAttempts)
				{
					// Wait 5 seconds, then retry
					System.Threading.Thread.Sleep(5000);
				}
			}

			if (!bSuccess)
				m_WarningMsg = AppendToString(m_WarningMsg, "UploadToMyEMSL reports False");

			if (bSuccess && !m_MyEmslUploadSuccess)
				m_WarningMsg = AppendToString(m_WarningMsg, "UploadToMyEMSL reports True but m_MyEmslUploadSuccess is False");

			return bSuccess && m_MyEmslUploadSuccess;
		}

		/// <summary>
		/// Use MyEMSLUploader to upload the data to MyEMSL
		/// </summary>
		/// <returns>True if success, false if an error</returns>
		protected bool UploadToMyEMSL(bool recurse)
		{
			bool success;
			System.DateTime dtStartTime = System.DateTime.UtcNow;
			Pacifica.DMS_Metadata.MyEMSLUploader myEMSLUL = null;

			try
			{
				m_Msg = "Bundling changes to dataset " + m_DatasetName + " for transmission to MyEMSL";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, m_Msg);

				myEMSLUL = new Pacifica.DMS_Metadata.MyEMSLUploader(m_MgrParams.TaskDictionary, m_TaskParams.TaskDictionary);

				// Attach the events

				myEMSLUL.DebugEvent += new Pacifica.Core.DebugEventHandler(myEMSLUL_DebugEvent);
				myEMSLUL.ErrorEvent += new Pacifica.Core.DebugEventHandler(myEMSLUL_ErrorEvent);
				myEMSLUL.StatusUpdate += new Pacifica.Core.StatusUpdateEventHandler(myEMSLUL_StatusUpdate);
				myEMSLUL.UploadCompleted += new Pacifica.Core.UploadCompletedEventHandler(myEMSLUL_UploadCompleted);

				m_TaskParams.AddAdditionalParameter(Pacifica.DMS_Metadata.MyEMSLUploader.RECURSIVE_UPLOAD, recurse.ToString());

				string statusURL;

				// Start the upload
				myEMSLUL.StartUpload(out statusURL);

				System.DateTime myEMSLFinishTime = System.DateTime.UtcNow;

				var tsElapsedTime = myEMSLFinishTime.Subtract(dtStartTime);

				m_Msg = "Upload of " + m_DatasetName + " completed in " + tsElapsedTime.TotalSeconds.ToString("0.0") + " seconds: " + myEMSLUL.FileCountNew + " new files, " + myEMSLUL.FileCountUpdated + " updated files, " + myEMSLUL.Bytes + " bytes";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, m_Msg);

				m_Msg = "myEMSL statusURI => " + myEMSLUL.StatusURI;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, m_Msg);

				// Raise an event with the stats
				// This will cause clsPluginMain to call StoreMyEMSLUploadStats to store the results in the database (stored procedure StoreMyEMSLUploadStats)
				MyEMSLUploadEventArgs e = new MyEMSLUploadEventArgs(myEMSLUL.FileCountNew, myEMSLUL.FileCountUpdated, myEMSLUL.Bytes, tsElapsedTime.TotalSeconds, statusURL, iErrorCode: 0);
				OnMyEMSLUploadComplete(e);

				m_StatusTools.UpdateAndWrite(100);

				success = true;
			}
			catch (Exception ex)
			{
				m_Msg = "Exception uploading to MyEMSL";
				m_ErrMsg = string.Copy(m_Msg);
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg, ex);
				LogOperationFailed(m_DatasetName);

				// Raise an event with the stats

				int errorCode = ex.Message.GetHashCode();
				if (errorCode == 0)
					errorCode = 1;

				var tsElapsedTime = System.DateTime.UtcNow.Subtract(dtStartTime);

				MyEMSLUploadEventArgs e;
				if (myEMSLUL == null)
					e = new MyEMSLUploadEventArgs(0, 0, 0, tsElapsedTime.TotalSeconds, string.Empty, errorCode);
				else
					e = new MyEMSLUploadEventArgs(myEMSLUL.FileCountNew, myEMSLUL.FileCountUpdated, myEMSLUL.Bytes, tsElapsedTime.TotalSeconds, myEMSLUL.StatusURI, errorCode);

				OnMyEMSLUploadComplete(e);

				success = false;
			}
			finally
			{
				// Detach the event handlers
				if (myEMSLUL != null)
				{
					myEMSLUL.DebugEvent -= myEMSLUL_DebugEvent;
					myEMSLUL.ErrorEvent -= myEMSLUL_ErrorEvent;
					myEMSLUL.StatusUpdate -= myEMSLUL_StatusUpdate;
					myEMSLUL.UploadCompleted -= myEMSLUL_UploadCompleted;
				}
			}

			return success;

		}

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
		[Obsolete("No longer needed since using MyEMSL")]
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
				m_FtpTools.UseLogFile = bool.Parse(m_MgrParams.GetParam("ftplogging"));

				// Open the connection (I hope!)
				if (m_FtpTools.OpenFTPConnection())
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Archive connection opened");
					m_ConnectionOpen = true;
					return true;
				}
				else
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_FtpTools.ErrMsg);
					m_FtpTools.CloseFTPConnection();
					if (!string.IsNullOrWhiteSpace(m_FtpTools.ErrMsg))
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_FtpTools.ErrMsg);
					m_Msg = "Closed FTP connection";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_Msg);
					LogOperationFailed(m_DatasetName);
					m_ConnectionOpen = false;
					m_ErrMsg = string.Copy("Unable to open the FTP connection");
					return false;
				}
			}
			catch (Exception ex)
			{
				m_Msg = "clsOpsBase.OpenArchiveServer: Exception opening server connection";
				m_ErrMsg = string.Copy(m_Msg);
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg, ex);
				LogOperationFailed(m_DatasetName);
				return false;
			}
		}	// End sub

		/// <summary>
		/// Closes an archive connection
		/// </summary>
		/// <param name="archiveOrUpdate"></param>
		/// <returns></returns>
		[Obsolete("No longer needed since using MyEMSL")]
		protected bool CloseArchiveServer()
		{
			try
			{
				m_FtpTools.CloseFTPConnection();
				if (m_FtpTools.ErrMsg == string.Empty)
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Closed FTP connection");
					m_ConnectionOpen = false;
					return true;
				}
				else
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_FtpTools.ErrMsg);
					m_ErrMsg = "Error closing the FTP connection: " + m_FtpTools.ErrMsg;
					return false;
				}
			}
			catch (Exception ex)
			{
				m_Msg = "clsOpsBase.OpenArchiveServer: Exception closing server connection";
				m_ErrMsg = string.Copy(m_Msg);
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg, ex);
				LogOperationFailed(m_DatasetName);
				return false;
			}
		}	// End sub

		/// <summary>
		/// Determine the total size of all files in the specified folder (including subdirectories)
		/// </summary>
		/// <param name="sourceFolderPath"></param>
		/// <returns>Total size, in GB</returns>
		protected float ComputeFolderSizeGB(string sourceFolderPath)
		{
			DirectoryInfo diSourceFolder = new DirectoryInfo(sourceFolderPath);

			string msg = "Determing the total size of " + sourceFolderPath;
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

			if (!diSourceFolder.Exists)
			{
				msg = "Source folder not found by ComputeFolderSizeGB: " + sourceFolderPath;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);

				return 0;
			}
			float folderSizeGB = 0;

			foreach (FileInfo fiFile in diSourceFolder.GetFiles("*", SearchOption.AllDirectories))
			{
				folderSizeGB += (float)(fiFile.Length / 1024.0 / 1024.0 / 1024.0);
			}

			msg = "  Total size: " + folderSizeGB.ToString("0.0") + " GB";
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

			return folderSizeGB;

		}

		/// <summary>
		/// General method for copying a folder to the archive
		/// </summary>
		/// <param name="sourceFolder">Folder name/path to copy</param>
		/// <param name="destFolder">Folder name/path on archive</param>
		/// <returns></returns>
		[Obsolete("No longer needed since using MyEMSL")]
		protected bool CopyOneFolderToArchive(string sourceFolderPath, string destFolderPath)
		{
			// Verify source folder exists
			if (!Directory.Exists(sourceFolderPath))
			{
				m_Msg = "Source folder " + sourceFolderPath + " not found";
				m_ErrMsg = string.Copy(m_Msg);
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg);
				LogOperationFailed(m_DatasetName);
				return false;
			}

			m_Msg = "Copying " + sourceFolderPath + " to " + destFolderPath + " via FTP";
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.DEBUG, m_Msg);

			// Open archive connection
			if (!OpenArchiveServer()) return false;

			// Copy specified folder to archive
			try
			{
				if (!m_FtpTools.CopyDirectory(sourceFolderPath, destFolderPath, true))
				{
					m_Msg = "Error copying folder by ftp: " + m_FtpTools.ErrMsg;
					m_ErrMsg = string.Copy(m_Msg);
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg + "; " + sourceFolderPath);
					LogOperationFailed(m_DatasetName);
					CloseArchiveServer();
					return false;
				}
			}
			catch (Exception ex)
			{
				m_Msg = "Error copying folder by ftp: " + ex.Message;
				m_ErrMsg = string.Copy(m_Msg);
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg + "; " + sourceFolderPath, ex);
				LogOperationFailed(m_DatasetName);
				CloseArchiveServer();
				return false;
			}

			m_Msg = "Copied folder " + sourceFolderPath + " to archive";
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, m_Msg);

			// Close the archive connection
			if (!CloseArchiveServer()) return false;

			// Finished successfully
			return true;
		}	// End sub

		/// <summary>
		/// Create a stagemd5 file for all files (and subfolders) in sResultsFolderPathServer
		/// </summary>
		/// <param name="sResultsFolderPathServer"></param>
		/// <param name="sResultsFolderPathArchive"></param>
		/// <returns></returns>
		[Obsolete("No longer needed since using MyEMSL")]
		protected bool CreateMD5StagingFile(string sResultsFolderPathServer, string sResultsFolderPathArchive)
		{
			string sLocalParentFolderPathForDataset;
			string sArchiveStoragePathForDataset;

			System.Collections.Generic.List<string> lstFilePathsToStage;
			bool bSuccess;

			try
			{
				lstFilePathsToStage = new System.Collections.Generic.List<string>();

				// Determine the folder just above sResultsFolderPathServer and just above sResultsFolderPathArchive
				System.IO.DirectoryInfo diResultsFolderServer = new System.IO.DirectoryInfo(sResultsFolderPathServer);
				sLocalParentFolderPathForDataset = diResultsFolderServer.Parent.FullName;

				System.IO.DirectoryInfo diResultsFolderArchive = new System.IO.DirectoryInfo(sResultsFolderPathArchive);
				sArchiveStoragePathForDataset = diResultsFolderArchive.Parent.FullName;

				// Populate lstFilePathsToStage with each file found at sResultsFolderPathServer (including files in subfolders)
				foreach (System.IO.FileInfo fiFile in diResultsFolderServer.GetFiles("*", SearchOption.AllDirectories))
				{
					lstFilePathsToStage.Add(fiFile.FullName);
				}

				bSuccess = CreateMD5StagingFileWork(lstFilePathsToStage, m_DatasetName, sLocalParentFolderPathForDataset, sArchiveStoragePathForDataset);
			}
			catch (Exception ex)
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception creating MD5 staging file for newly archived files from folder " + sResultsFolderPathServer, ex);
				return false;
			}

			return bSuccess;
		}

		/// <summary>
		/// Create a stagemd5 file for all files in filesToUpdate
		/// </summary>
		/// <param name="sResultsFolderPathServer"></param>
		/// <param name="sResultsFolderPathArchive"></param>
		/// <param name="filesToUpdate"></param>
		/// <returns></returns>
		[Obsolete("No longer needed since using MyEMSL")]
		protected bool CreateMD5StagingFile(string sResultsFolderPathServer, string sResultsFolderPathArchive, System.Collections.Generic.List<clsJobData> filesToUpdate)
		{

			string sLocalParentFolderPathForDataset;
			string sArchiveStoragePathForDataset;

			System.Collections.Generic.List<string> lstFilePathsToStage;
			bool bSuccess;

			try
			{
				lstFilePathsToStage = new System.Collections.Generic.List<string>();

				// Determine the folder just above sResultsFolderPathServer and just above sResultsFolderPathArchive
				System.IO.DirectoryInfo diResultsFolderServer = new System.IO.DirectoryInfo(sResultsFolderPathServer);
				sLocalParentFolderPathForDataset = diResultsFolderServer.Parent.FullName;

				System.IO.DirectoryInfo diResultsFolderArchive = new System.IO.DirectoryInfo(sResultsFolderPathArchive);
				sArchiveStoragePathForDataset = diResultsFolderArchive.Parent.FullName;

				// Populate lstFilePathsToStage with each file in filesToUpdate
				foreach (clsJobData objFileInfo in filesToUpdate)
				{
					if (objFileInfo.CopySuccess)
						lstFilePathsToStage.Add(objFileInfo.SvrFileToUpdate);
				}

				bSuccess = CreateMD5StagingFileWork(lstFilePathsToStage, m_DatasetName, sLocalParentFolderPathForDataset, sArchiveStoragePathForDataset);
			}
			catch (Exception ex)
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception creating MD5 staging file for newly archived files defined in List filesToUpdate", ex);
				return false;
			}

			return bSuccess;
		}

		[Obsolete("No longer needed since using MyEMSL")]
		private bool CreateMD5StagingFileWork(System.Collections.Generic.List<string> lstFilePathsToStage, string sDatasetName, string sLocalParentFolderPathForDataset, string sArchiveStoragePathForDataset)
		{
			const string EXTRA_FILES_REGEX = clsMD5StageFileCreator.EXTRA_FILES_SUFFIX + @"(\d+)$";

			System.Text.RegularExpressions.Regex reExtraFiles;
			System.Text.RegularExpressions.Match reMatch;

			System.Collections.Generic.List<string> lstExtraStagingFiles = new System.Collections.Generic.List<string>();

			string sDatasetAndSuffix;
			int iExtraFileNumber = 1;
			bool bSuccess;

			// Convert sArchiveStoragePathForDataset from the form \\a2.emsl.pnl.gov\dmsarch\LTQ_ORB_2_2\
			// to the form /archive/dmsarch/LTQ_ORB_2_2/

			string sArchiveStoragePathForDatasetUnix = string.Empty;
			bSuccess = clsMD5StageFileCreator.ConvertArchiveSharePathToArchiveStoragePath(sArchiveStoragePathForDataset, false, ref sArchiveStoragePathForDatasetUnix);

			if (!bSuccess)
			{
				string msg = "Error converting the archive folder path (" + sArchiveStoragePathForDataset + ") to the archive storage path (something like /archive/dmsarch/LTQ_ORB_3_1/)";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
				return false;
			}

			// Look for existing stagemd5 or result files for dataset sDatasetName
			System.Collections.Generic.List<string> lstSearchFileSpec = new System.Collections.Generic.List<string>();

			sDatasetAndSuffix = sDatasetName + clsMD5StageFileCreator.EXTRA_FILES_SUFFIX;

			lstSearchFileSpec.Add(clsMD5StageFileCreator.STAGE_FILE_PREFIX + sDatasetAndSuffix + "*");
			lstSearchFileSpec.Add(clsMD5StageFileCreator.STAGE_FILE_INPROGRESS_PREFIX + sDatasetAndSuffix + "*");
			lstSearchFileSpec.Add(clsMD5StageFileCreator.MD5_RESULTS_FILE_PREFIX + sDatasetAndSuffix + "*");
			lstSearchFileSpec.Add(clsMD5StageFileCreator.MD5_RESULTS_INPROGRESS_FILE_PREFIX + sDatasetAndSuffix + "*");

			System.IO.DirectoryInfo diStagingFolder;
			diStagingFolder = new System.IO.DirectoryInfo(mMD5StageFileCreator.StagingFolderPath);

			reExtraFiles = new System.Text.RegularExpressions.Regex(EXTRA_FILES_REGEX, System.Text.RegularExpressions.RegexOptions.Compiled | System.Text.RegularExpressions.RegexOptions.IgnoreCase);

			// Check for each file in lstSearchFileSpec
			foreach (string sFileSpec in lstSearchFileSpec)
			{
				foreach (System.IO.FileInfo fiFile in diStagingFolder.GetFiles(sFileSpec))
				{
					// Examine each file to parse out the number after EXTRA_FILES_SUFFIX
					// For example, if the filename is results.DatasetName__ExtraFiles001 then we want to parse out "001" and convert that to an integer
					reMatch = reExtraFiles.Match(fiFile.Name);
					if (reMatch.Success)
					{
						int iStageFileNum;

						if (int.TryParse(reMatch.Groups[1].Value, out iStageFileNum))
						{
							// Number parsed out
							// Adjust iExtraFileNumberNew if necessary
							if (iStageFileNum >= iExtraFileNumber)
								iExtraFileNumber = iStageFileNum + 1;

							lstExtraStagingFiles.Add(fiFile.FullName);

						}
					}
				}
			} // foreach (sFileSpec in lstSearchFileSpec)

			// Create the new stagemd5 file
			// Copy iExtraFileNumber to iExtraFileNumberNew in case the ExtraFile suffix value gets incremented by mMD5StageFileCreator.WriteStagingFile
			// If the number does get auto-incremented, then we won't append the contents of lstExtraStagingFiles to the newly created staging file
			int iExtraFileNumberNew = iExtraFileNumber;
			bSuccess = mMD5StageFileCreator.WriteStagingFile(ref lstFilePathsToStage, sDatasetName, sLocalParentFolderPathForDataset, sArchiveStoragePathForDatasetUnix, ref iExtraFileNumberNew);

			if (bSuccess && lstExtraStagingFiles.Count > 0 && iExtraFileNumberNew == iExtraFileNumber)
			{
				try
				{
					// Append the contents of each file in lstExtraStagingFiles to the newly created staging file

					// Open the newly created staging file for Append
					System.IO.StreamWriter swStageFile;
					string sLineIn;
					swStageFile = new System.IO.StreamWriter(new System.IO.FileStream(mMD5StageFileCreator.StagingFilePath, FileMode.Append, FileAccess.Write, FileShare.Read));

					// Append the contents of each file in lstExtraStagingFiles
					foreach (string sExtraFilePath in lstExtraStagingFiles)
					{

						try
						{
							System.IO.StreamReader srExtraStageFile;
							srExtraStageFile = new System.IO.StreamReader(new System.IO.FileStream(sExtraFilePath, FileMode.Open, FileAccess.Read, FileShare.Read));

							while (srExtraStageFile.Peek() > -1)
							{
								sLineIn = srExtraStageFile.ReadLine();
								if (!string.IsNullOrWhiteSpace(sLineIn))
									swStageFile.WriteLine(sLineIn);
							}

							srExtraStageFile.Close();
						}
						catch (Exception ex)
						{
							// Log the error, but continue trying to merge files
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error appending extra staging file " + sExtraFilePath + "to " + mMD5StageFileCreator.StagingFilePath, ex);
						}
					}

					swStageFile.Close();

					// Delete each file in lstExtraStagingFiles
					foreach (string sExtraFilePath in lstExtraStagingFiles)
					{
						try
						{
							System.IO.File.Delete(sExtraFilePath);
						}
						catch
						{
							// Ignore errors deleting the extra staging files
						}

					}
				}
				catch (Exception ex)
				{
					// Log the error, but leave bSuccess as True
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error appending extra staging files to " + mMD5StageFileCreator.StagingFilePath, ex);
				}

			}

			return bSuccess;

		}

		#endregion

		#region "Event Delegates and Classes"

		public event MyEMSLUploadEventHandler MyEMSLUploadComplete;

		#endregion

		#region "Event Handlers"

		void LogStatusMessageSkipDuplicate(string message)
		{
			if (string.Compare(message, mMostRecentLogMessage) != 0 || System.DateTime.UtcNow.Subtract(mMostRecentLogTime).TotalSeconds >= 60)
			{
				mMostRecentLogMessage = string.Copy(message);
				mMostRecentLogTime = System.DateTime.UtcNow;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, message);
			}
		}

		void myEMSLUL_DebugEvent(object sender, Pacifica.Core.MessageEventArgs e)
		{
			string msg = "  ... " + e.CallingFunction + ": " + e.Message;
			LogStatusMessageSkipDuplicate(msg);
		}

		void myEMSLUL_ErrorEvent(object sender, Pacifica.Core.MessageEventArgs e)
		{
			string msg = "MyEmslUpload error in function " + e.CallingFunction + ": " + e.Message;
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
		}

		void myEMSLUL_StatusUpdate(object sender, Pacifica.Core.StatusEventArgs e)
		{

			if (System.DateTime.UtcNow.Subtract(mLastStatusUpdateTime).TotalSeconds >= 60 && e.PercentCompleted > 0)
			{
				mLastStatusUpdateTime = System.DateTime.UtcNow;
				string msg = "  ... uploading, " + e.PercentCompleted.ToString("0.0") + "% complete for " + (e.TotalBytesToSend / 1024.0).ToString("#,##0") + " KB";
				if (!(string.IsNullOrEmpty(e.StatusMessage)))
					msg += "; " + e.StatusMessage;

				LogStatusMessageSkipDuplicate(msg);
			}


			if (System.DateTime.UtcNow.Subtract(mLastProgressUpdateTime).TotalSeconds >= 3 && e.PercentCompleted > 0)
			{
				mLastProgressUpdateTime = System.DateTime.UtcNow;
				m_StatusTools.UpdateAndWrite((float)e.PercentCompleted);
			}			
			
		}

		void myEMSLUL_UploadCompleted(object sender, Pacifica.Core.UploadCompletedEventArgs e)
		{
			string msg = "  ... MyEmsl upload task complete";
			
			// Note that e.ServerResponse will simply have the StatusURL if the upload succeeded
			// If a problem occurred, then e.ServerResponse will either have the full server reponse, or may even be blank
			if (string.IsNullOrEmpty(e.ServerResponse))
				msg += ": empty server reponse";
			else
				msg += ": " + e.ServerResponse;

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
			m_MyEmslUploadSuccess = true;
		}

		public void OnMyEMSLUploadComplete(MyEMSLUploadEventArgs e)
		{
			if (MyEMSLUploadComplete != null)
				MyEMSLUploadComplete(this, e);
		}
		#endregion

		#region "MD5StageFileCreator initialization and event handlers"

		[Obsolete("No longer needed since using MyEMSL")]
		private void InitializeMD5StageFileCreator()
		{
			string sArchiveStagingFolderPath;
			sArchiveStagingFolderPath = m_MgrParams.GetParam("HashFileLocation");

			if (string.IsNullOrWhiteSpace(sArchiveStagingFolderPath))
			{
				sArchiveStagingFolderPath = MD5StageFileCreator.clsMD5StageFileCreator.DEFAULT_STAGING_FOLDER_PATH;
				string msg = "Manager parameter HashFileLocation is not defined; will use default path of '" + sArchiveStagingFolderPath + "'";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
			}

			mMD5StageFileCreator = new MD5StageFileCreator.clsMD5StageFileCreator(sArchiveStagingFolderPath);

			// Attach the events
			mMD5StageFileCreator.OnErrorEvent += new MD5StageFileCreator.clsMD5StageFileCreator.OnErrorEventEventHandler(MD5ErrorEventHandler);
			mMD5StageFileCreator.OnMessageEvent += new MD5StageFileCreator.clsMD5StageFileCreator.OnMessageEventEventHandler(MD5MessageEventHandler);
		}

		[Obsolete("No longer needed since using MyEMSL")]
		private void MD5ErrorEventHandler(string sErrorMessage)
		{
			string msg = "MD5StageFileCreator Error: " + sErrorMessage;
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
		}

		[Obsolete("No longer needed since using MyEMSL")]
		private void MD5MessageEventHandler(string sMessage)
		{
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, sMessage);
		}

		#endregion

	}	// End class


}	// End namespace
