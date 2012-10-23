
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
using MD5StageFileCreator;

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
		protected string m_ErrMsg = string.Empty;
		protected string m_WarningMsg = string.Empty;
		protected string m_TempVol;
		protected string m_DSNamePath;
		protected string m_ArchiveNamePath;
		protected string m_Msg;
		protected clsFtpOperations m_FtpTools;
		
		protected bool m_MyEmslUploadPermissionsError;
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

		protected string mMostRecentLogMessage = string.Empty;
		protected System.DateTime mMostRecentLogTime = System.DateTime.UtcNow;

		clsMD5StageFileCreator mMD5StageFileCreator;

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
			if (m_TaskParams.GetParam("StepTool") == "DatasetArchive")
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
			m_DatasetName = m_TaskParams.GetParam("Dataset");
			bool bMyEmslUpload = false;
			bool bMyEmslUploadSuccess = true;

			if (bool.TryParse(m_MgrParams.GetParam("MyEmslUpload"), out bMyEmslUpload) && bMyEmslUpload)
			{
				// Possibly copy this dataset to MyEmsl
				string sInstrument = m_TaskParams.GetParam("Instrument_Name");
				string sEUSInstrumentID = m_TaskParams.GetParam("EUS_Instrument_ID");
                int iMaxMyEMSLUploadAttempts = 3;
				mMostRecentLogTime = System.DateTime.UtcNow;
				mLastStatusUpdateTime = System.DateTime.UtcNow;

				if (sEUSInstrumentID.Length > 0 && !sInstrument.ToLower().Contains("fticr"))
				{
					if (sInstrument == "Exact03" || sInstrument == "LTQ_Orb_2" || sInstrument == "LTQ_Orb_3")
					{
						if (System.DateTime.Now.Hour % 6 == 0)
						{
							bMyEmslUploadSuccess = UploadToMyEMSLWithRetry(iMaxMyEMSLUploadAttempts);
						}
					}
				}

			}

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
				LogOperationFailed(m_DatasetName);
				return false;
			}

			// Initialize the MD5 stage file creator
			InitializeMD5StageFileCreator();

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

        protected bool UploadToMyEMSLWithRetry(int maxAttempts)
        {
            bool bSuccess = false;
            int iAttempts = 0;
			m_MyEmslUploadPermissionsError = false;
			m_MyEmslUploadSuccess = false;

            if (maxAttempts < 1)
                maxAttempts = 1;

            while (!bSuccess && iAttempts < maxAttempts) 
            {
                iAttempts += 1;
                bSuccess = UploadToMyEMSL();

				if (m_MyEmslUploadPermissionsError)
					break;

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
		protected bool UploadToMyEMSL()
		{
			bool success;
			System.TimeSpan tsElapsedTime = new System.TimeSpan();
			Pacifica.DMS_Metadata.MyEMSLUploader myEMSLUL = null;

			try
			{
				m_Msg = "Bundling changes to dataset " + m_DatasetName + " for transmission to MyEMSL";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, m_Msg);

				System.DateTime myEMSLStartTime = System.DateTime.Now;

				myEMSLUL = new Pacifica.DMS_Metadata.MyEMSLUploader();

				// Attach the events
				myEMSLUL.DebugEvent += new Pacifica.Core.DebugEventHandler(myEMSLUpload_DebugEvent);
				myEMSLUL.ErrorEvent += new Pacifica.Core.DebugEventHandler(myEMSLUpload_ErrorEvent);
				myEMSLUL.StatusUpdate += new Pacifica.Core.StatusUpdateEventHandler(myEMSLUpload_StatusUpdate);
				myEMSLUL.TaskCompleted += new Pacifica.Core.TaskCompletedEventHandler(myEMSLUpload_TaskCompleted);
				myEMSLUL.DataReceivedAndVerified += new Pacifica.Core.DataVerifiedHandler(myEMSLUpload_DataReceivedAndVerified);

				// Start the upload
				myEMSLUL.StartUpload(m_TaskParams.TaskDictionary, m_MgrParams.TaskDictionary);

				System.DateTime myEMSLFinishTime = System.DateTime.Now;

				tsElapsedTime = myEMSLFinishTime.Subtract(myEMSLStartTime);


				m_Msg = "Upload of " + m_DatasetName + " completed in " + tsElapsedTime.TotalSeconds.ToString("0.0") + " seconds: " + myEMSLUL.FileCountNew + " new files, " + myEMSLUL.FileCountUpdated + " updated files, " + myEMSLUL.Bytes + " bytes";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, m_Msg);

				m_Msg = "myEMSL statusURI => " + myEMSLUL.StatusURI;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, m_Msg);

				m_Msg = "myEMSL content lookup URI => " + myEMSLUL.DirectoryLookupPath;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, m_Msg);

				// Raise an event with the stats
				MyEMSLUploadEventArgs e = new MyEMSLUploadEventArgs(myEMSLUL.FileCountNew, myEMSLUL.FileCountUpdated, myEMSLUL.Bytes, tsElapsedTime.TotalSeconds, myEMSLUL.StatusURI, myEMSLUL.DirectoryLookupPath, iErrorCode: 0);
				OnMyEMSLUploadComplete(e);

				success = true;
			}
			catch (Exception ex)
			{
				m_Msg = "Exception uploading to MyEMSL";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg, ex);
				LogOperationFailed(m_DatasetName);

				// Raise an event with the stats

				int errorCode = ex.Message.GetHashCode();
				if (errorCode == 0)
					errorCode = 1;

				MyEMSLUploadEventArgs e;
				if (myEMSLUL == null)
					e = new MyEMSLUploadEventArgs(0, 0, 0, tsElapsedTime.TotalSeconds, string.Empty, string.Empty, errorCode);
				else
					e = new MyEMSLUploadEventArgs(myEMSLUL.FileCountNew, myEMSLUL.FileCountUpdated, myEMSLUL.Bytes, tsElapsedTime.TotalSeconds, myEMSLUL.StatusURI, myEMSLUL.DirectoryLookupPath, errorCode);

				OnMyEMSLUploadComplete(e);

				success = false;
			}
			finally
			{
				// Detach the event handlers
				if (myEMSLUL != null)
				{
					myEMSLUL.DebugEvent -= myEMSLUpload_DebugEvent;
					myEMSLUL.ErrorEvent -= myEMSLUpload_ErrorEvent;
					myEMSLUL.StatusUpdate -= myEMSLUpload_StatusUpdate;
					myEMSLUL.TaskCompleted -= myEMSLUpload_TaskCompleted;
					myEMSLUL.DataReceivedAndVerified -= myEMSLUpload_DataReceivedAndVerified;
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
		/// General method for copying a folder to the archive
		/// </summary>
		/// <param name="sourceFolder">Folder name/path to copy</param>
		/// <param name="destFolder">Folder name/path on archive</param>
		/// <returns></returns>
		protected bool CopyOneFolderToArchive(string sourceFolder, string destFolder)
		{
			// Verify source folder exists
			if (!Directory.Exists(sourceFolder))
			{
				m_Msg = "Source folder " + sourceFolder + " not found";
				m_ErrMsg = string.Copy(m_Msg);
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg);
				LogOperationFailed(m_DatasetName);
				return false;
			}

			// Open archive connection
			if (!OpenArchiveServer()) return false;

			// Copy specified folder to archive
			try
			{
				if (!m_FtpTools.CopyDirectory(sourceFolder, destFolder, true))
				{
					m_Msg = "Error copying folder by ftp: " + m_FtpTools.ErrMsg;
					m_ErrMsg = string.Copy(m_Msg);
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg + "; " + sourceFolder);
					LogOperationFailed(m_DatasetName);
					CloseArchiveServer();
					return false;
				}
			}
			catch (Exception ex)
			{
				m_Msg = "Error copying folder by ftp: " + ex.Message;
				m_ErrMsg = string.Copy(m_Msg);
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg + "; " + sourceFolder, ex);
				LogOperationFailed(m_DatasetName);
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

		/// <summary>
		/// Create a stagemd5 file for all files (and subfolders) in sResultsFolderPathServer
		/// </summary>
		/// <param name="sResultsFolderPathServer"></param>
		/// <param name="sResultsFolderPathArchive"></param>
		/// <returns></returns>
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

		void myEMSLUpload_DebugEvent(string callingFunction, string currentTask)
		{
			string msg = "  ... " + callingFunction + ": " + currentTask;
			LogStatusMessageSkipDuplicate(msg);
		}

		void myEMSLUpload_ErrorEvent(string callingFunction, string errorMessage)
		{
			string msg = "MyEmslUpload error in function " + callingFunction + ": " + errorMessage;
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
		}
		
		void myEMSLUpload_StatusUpdate(string bundleIdentifier, int percentCompleted, long totalBytesSent, long totalBytesToSend, string averageUploadSpeed)
		{
			// Note that AverageUploadSpeed does not actually contain speed (as of 5/3/2012); it sometimes contains a comment, but we'll just ignore it

			if (System.DateTime.UtcNow.Subtract(mLastStatusUpdateTime).TotalSeconds >= 60 && percentCompleted > 0)
			{
				mLastStatusUpdateTime = System.DateTime.UtcNow;
				string msg = "  ... uploading " + bundleIdentifier + ", " + percentCompleted.ToString() + "% complete for " + (totalBytesToSend / 1024.0).ToString("#,##0") + " KB";
				LogStatusMessageSkipDuplicate(msg);
			}
		}		

		void myEMSLUpload_TaskCompleted(string bundleIdentifier, string serverResponse)
		{
			string msg = "  ... MyEmsl upload task complete for " + bundleIdentifier + ": " + serverResponse;
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
			m_MyEmslUploadSuccess = true;
		}

		void myEMSLUpload_DataReceivedAndVerified(bool successfulVerification, string errorMessage)
		{
			string msg;
			if (successfulVerification)
			{
				msg = "  ... DataReceivedAndVerified success = true";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
				m_MyEmslUploadSuccess = true;
			}
			else
			{
				msg = "  ... DataReceivedAndVerified success = false: " + errorMessage;
				if (errorMessage.Contains("do not have upload permissions"))
				{
					m_WarningMsg = AppendToString(m_WarningMsg, errorMessage);
					m_MyEmslUploadPermissionsError = true;
				}

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
				m_MyEmslUploadSuccess = false;
			}
		}

		public void OnMyEMSLUploadComplete(MyEMSLUploadEventArgs e)
		{
			if (MyEMSLUploadComplete != null)
				MyEMSLUploadComplete(this, e);
		}
		#endregion

		#region "MD5StageFileCreator initialization and event handlers"

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

		private void MD5ErrorEventHandler(string sErrorMessage)
		{
			string msg = "MD5StageFileCreator Error: " + sErrorMessage;
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
		}

		private void MD5MessageEventHandler(string sMessage)
		{
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, sMessage);
		}

		#endregion

	}	// End class

	
}	// End namespace
