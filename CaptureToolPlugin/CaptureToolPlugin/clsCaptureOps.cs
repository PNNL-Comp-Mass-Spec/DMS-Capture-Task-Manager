
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 09/25/2009
//
// Last modified 06/16/2014
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.Linq;
using CaptureTaskManager;
using PRISM.Files;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading;

namespace CaptureToolPlugin
{
	public class clsCaptureOps
	{
		//*********************************************************************************************************
		// Base class for performing capture operations
		//**********************************************************************************************************

		#region "Enums"
		public enum RawDSTypes
		{
			None,
			File,
			FolderNoExt,
			FolderExt,
			BrukerImaging,
			BrukerSpot,
			MultiFile
		}

		private enum DatasetFolderState
		{
			Empty,
			NotEmpty,
			Error
		}

		protected enum ConnectionType
		{
			NotConnected,
			Prism,
			DotNET
		}
		#endregion

		#region "Class variables"

		private readonly IMgrParams m_MgrParams;
		private int m_SleepInterval = 30;

		// True means MgrParam "perspective" =  "client" which means we will use paths like \\proto-5\Exact04\2012_1
		// False means MgrParam "perspective" = "server" which means we use paths like E:\Exact04\2012_1
		//
		// The capture task managers running on the Proto-x servers have "perspective" = "server"
		// Capture tasks that occur on the Proto-x servers should be limited to certain instruments via table T_Processor_Instrument in the DMS_Capture DB
		// If a capture task manager running on a Proto-x server has the DatasetCapture tool enabled, yet does not have an entry in T_Processor_Instrument, 
		//  then no capture tasks are allowed to be assigned to avoid drive path problems
		private bool m_ClientServer;

		private bool m_UseBioNet;
		private readonly string m_UserName = "";
		private readonly string m_Pwd = "";
		private ShareConnector m_ShareConnectorPRISM;
		private NetworkConnection m_ShareConnectorDotNET;
		protected ConnectionType m_ConnectionType = ConnectionType.NotConnected;
		private bool m_NeedToAbortProcessing;

		private readonly clsFileTools m_FileTools;

		DateTime m_LastProgressUpdate = DateTime.Now;

		string m_LastProgressFileName = string.Empty;
		float m_LastProgressPercent = -1;
		private bool mFileCopyEventsWired;

		string m_ErrorMessage = string.Empty;

		#endregion

		#region "Properties"

		public bool NeedToAbortProcessing
		{
			get { return m_NeedToAbortProcessing; }
		}

		#endregion

		#region "Constructors"
		/// <summary>
		/// Constructor
		/// </summary>
		/// <param name="mgrParams">Parameters for manager operation</param>
		/// <param name="useBioNet">Flag to indicate if source instrument is on Bionet</param>
		public clsCaptureOps(IMgrParams mgrParams, bool useBioNet)
		{
			m_MgrParams = mgrParams;

			//Get client/server perspective
			// True means MgrParam "perspective" =  "client" which means we will use paths like \\proto-5\Exact04\2012_1
			// False means MgrParam "perspective" = "server" which means we use paths like E:\Exact04\2012_1
			string tmpParam = m_MgrParams.GetParam("perspective");
			if (tmpParam.ToLower() == "client")
			{
				m_ClientServer = true;
			}
			else
			{
				m_ClientServer = false;
			}

			//Setup for BioNet use, if applicable
			m_UseBioNet = useBioNet;
			if (m_UseBioNet)
			{
				m_UserName = m_MgrParams.GetParam("bionetuser");
				m_Pwd = m_MgrParams.GetParam("bionetpwd");

                if (!m_UserName.Contains(@"\"))
			    {
                     // Prepend this computer's name to the username
			        m_UserName = Environment.MachineName + @"\" + m_UserName;
			    }
			}

			//Sleep interval for "is dataset complete" testing
			m_SleepInterval = int.Parse(m_MgrParams.GetParam("sleepinterval"));

			// Instantiate m_FileTools
			m_FileTools = new clsFileTools(m_MgrParams.GetParam("MgrName", "CaptureTaskManager"), 1);

			// Note that all of the events and methods in clsFileTools are static
			if (!mFileCopyEventsWired)
			{
				mFileCopyEventsWired = true;
				m_FileTools.CopyingFile += OnCopyingFile;
				m_FileTools.FileCopyProgress += OnFileCopyProgress;
				m_FileTools.ResumingFileCopy += OnResumingFileCopy;
			}


		}

		#endregion

		#region "Methods"

		public void DetachEvents()
		{
			// Un-wire the events
			if (mFileCopyEventsWired && m_FileTools != null)
			{
				mFileCopyEventsWired = false;
				m_FileTools.CopyingFile -= OnCopyingFile;
				m_FileTools.FileCopyProgress -= OnFileCopyProgress;
				m_FileTools.ResumingFileCopy -= OnResumingFileCopy;
			}
		}

		/// <summary>
		/// Creates specified folder; if the folder already exists, returns true
		/// </summary>
		/// <param name="inpPath">Fully qualified path for folder to be created</param>
		/// <returns>TRUE for success, FALSE for failure</returns>
		private void MakeFolderPath(string inpPath)
		{
			//Create specified directory
			try
			{
				var diFolder = new DirectoryInfo(inpPath);

				if (!diFolder.Exists)
					diFolder.Create();

			}
			catch (Exception ex)
			{
				m_ErrorMessage = "Exception creating directory " + inpPath;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, m_ErrorMessage, ex);
			}

		}

		/// <summary>
		/// Renames each file and subfolder at folderPath to start with x_
		/// </summary>
		/// <param name="folderPath"></param>
		/// <returns></returns>
		/// <remarks>Does not rename LCMethod*.xml files</remarks>
		private bool MarkSupersededFiles(string folderPath)
		{

			try
			{
				var diFolder = new DirectoryInfo(folderPath);

				if (diFolder.Exists)
				{
					string sLogMessage;
					string sTargetPath;

					FileInfo[] fiFiles = diFolder.GetFiles();
					FileInfo[] fiFilesToSkip = diFolder.GetFiles("LCMethod*.xml");

					// Rename superseded files (but skip LCMethod files)
					foreach (FileInfo fiFile in fiFiles)
					{
						// Rename the file, but only if it is not in fiFilesToSkip
						bool bSkipFile = false;
						foreach (FileInfo fiFileToSkip in fiFilesToSkip)
						{
							if (fiFileToSkip.FullName == fiFile.FullName)
							{
								bSkipFile = true;
								break;
							}
						}

						if (!bSkipFile)
						{
							sTargetPath = Path.Combine(diFolder.FullName, "x_" + fiFile.Name);

							if (File.Exists(sTargetPath))
							{
								// Target exists; delete it
								File.Delete(sTargetPath);
							}

							fiFile.MoveTo(sTargetPath);
						}

					}

					if (fiFiles.Length > 0)
					{
						sLogMessage = "Renamed superseded file(s) at " + diFolder.FullName + " to start with x_";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, sLogMessage);
					}

					// Renamed superseded folders
					DirectoryInfo[] diSubFolders = diFolder.GetDirectories();

					foreach (DirectoryInfo diSubFolder in diSubFolders)
					{
						sTargetPath = Path.Combine(diFolder.FullName, "x_" + diSubFolder.Name);

						if (Directory.Exists(sTargetPath))
						{
							// Target exists; delete it
							Directory.Delete(sTargetPath, true);
						}

						diSubFolder.MoveTo(sTargetPath);
					}

					if (diSubFolders.Length > 0)
					{
						sLogMessage = "Renamed superseded folder(s) at " + diFolder.FullName + " to start with x_";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, sLogMessage);
					}

				}

				return true;
			}
			catch (Exception ex)
			{
				m_ErrorMessage = "Exception renaming files/folders to start with x_";
				string msg = m_ErrorMessage + " at " + folderPath;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg, ex);
				return false;
			}

		}

		/// <summary>
		/// Checks to determine if specified folder is empty
		/// </summary>
		/// <param name="dsFolder">Full path specifying folder to be checked</param>
		/// <param name="fileCount">Output parameter: number of files</param>
		/// <param name="folderCount">Output parameter: number of folders</param>
		/// <returns>Empty=0, NotEmpty=1, or Error=2</returns>
		private DatasetFolderState IsDSFolderEmpty(string dsFolder, out int fileCount, out int folderCount)
		{
			//Returns count of files or folders if folder is not empty
			//Returns 0 if folder is empty
			//returns -1 on error

			fileCount = 0;
			folderCount = 0;

			try
			{
				//Check for files
				fileCount = Directory.GetFiles(dsFolder).Length;

				//Check for folders
				folderCount = Directory.GetDirectories(dsFolder).Length;

				if (fileCount > 0) return DatasetFolderState.NotEmpty;
				if (folderCount > 0) return DatasetFolderState.NotEmpty;
			}
			catch (Exception ex)
			{
				//Something really bad happened
				m_ErrorMessage = "Error checking for empty dataset folder";

				string msg = m_ErrorMessage + ": " + dsFolder;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg, ex);
				return DatasetFolderState.Error;
			}

			//If we got to here, then the directory is empty
			return DatasetFolderState.Empty;

		}	// End sub

		/// <summary>
		/// Performs action specified by DSFolderExistsAction mgr param if a dataset folder already exists
		/// </summary>
		/// <param name="dsFolder">Full path to dataset folder</param>
		/// <param name="bCopyWithResume">True when we will be using Copy with Resume to capture this instrument's data</param>
		/// <param name="maxFileCountToAllowResume">Maximum number of files that can existing in the dataset folder if we are going to allow CopyWithResume to be used</param>
		/// <param name="maxFolderCountToAllowResume">Maximum number of subfolders that can existing in the dataset folder if we are going to allow CopyWithResume to be used</param>		
		/// <param name="retData">Return data</param>
		/// <returns>TRUE for success, FALSE for failure</returns>
		/// <remarks>If both maxFileCountToAllowResume and maxFolderCountToAllowResume are zero, then requires a minimum number of subfolders or files be present to allow for CopyToResume to be used</remarks>
		private bool PerformDSExistsActions(
			string dsFolder,
			bool bCopyWithResume,
			int maxFileCountToAllowResume,
			int maxFolderCountToAllowResume,
			ref clsToolReturnData retData)
		{
			bool switchResult = false;
			int fileCount;
			int folderCount;

			switch (IsDSFolderEmpty(dsFolder, out fileCount, out folderCount))
			{
				case DatasetFolderState.Empty:
					// Directory is empty; all is good
					switchResult = true;
					break;
				case DatasetFolderState.Error:
					// There was an error attempting to determine the dataset directory contents
					// (Error reporting was handled by call to IsDSFolderEmpty above)
					switchResult = false;
					break;
				case DatasetFolderState.NotEmpty:
					string DSAction = m_MgrParams.GetParam("DSFolderExistsAction");
					string msg;
					switch (DSAction.ToLower())
					{
						case "overwrite_single_item":
							// If the folder only contains one or two files or only one subfolder
							// then we're likely retrying capture; rename the one file to start with x_

							bool tooManyFilesOrFolders = false;
							if (maxFileCountToAllowResume > 0 || maxFolderCountToAllowResume > 0)
							{
								if (fileCount > maxFileCountToAllowResume || folderCount > maxFolderCountToAllowResume)
									tooManyFilesOrFolders = true;
							}
							else
							{
								if (folderCount == 0 && fileCount > 2 || fileCount == 0 && folderCount > 1)
									tooManyFilesOrFolders = true;
							}

							if (!tooManyFilesOrFolders)
							{
								if (bCopyWithResume)
									// Do not rename the folder or file; leave as-is and we'll resume the copy
									switchResult = true;
								else
									switchResult = MarkSupersededFiles(dsFolder);
							}
							else
							{
								if (folderCount == 0 && bCopyWithResume)
									// Do not rename the files; leave as-is and we'll resume the copy
									switchResult = true;
								else
								{
									// Fail the capture task
									retData.CloseoutMsg = "Dataset folder already exists and has multiple files or subfolders";
									msg = retData.CloseoutMsg + ": " + dsFolder;
									clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
									switchResult = false;
								}
							}

							break;

						case "delete":
							//Attempt to delete dataset folder
							try
							{
								Directory.Delete(dsFolder, true);
								switchResult = true;
							}
							catch (Exception ex)
							{
								retData.CloseoutMsg = "Dataset folder already exists and cannot be deleted";
								msg = retData.CloseoutMsg + ": " + dsFolder;
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg, ex);
								switchResult = false;
							}
							break;
						case "rename":
							//Attempt to rename dataset folder
							if (RenameDatasetFolder(dsFolder))
							{
								switchResult = true;
							}
							else
							{
								//(Error reporting was handled by previous call to RenameDatasetFolder)
								switchResult = false;
							}
							break;
						case "fail":
							//Fail the capture task
							retData.CloseoutMsg = "Dataset folder already exists";
							msg = retData.CloseoutMsg + ": " + dsFolder;
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
							switchResult = false;
							break;
						default:
							//An invalid value for DSFolderExistsAction was specified

							retData.CloseoutMsg = "Dataset folder already exists; Invalid action " + DSAction + " specified";
							msg = retData.CloseoutMsg + " (" + dsFolder + ")";
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
							switchResult = false;
							break;
					}	//DSAction selection
					break;
				default:
					//Shouldn't ever get to here
					break;
			}	// End switch

			return switchResult;

		}

		/// <summary>
		/// Prefixes specified folder name with "x_"
		/// </summary>
		/// <param name="DSPath">Full path specifying folder to be renamed</param>
		/// <returns>TRUE for success, FALSE for failure</returns>
		private bool RenameDatasetFolder(string DSPath)
		{
			//Rename dataset folder on instrument
			try
			{
				var di = new DirectoryInfo(DSPath);
				if (di.Parent != null)
				{
					string n = Path.Combine(di.Parent.FullName, "x_" + di.Name);
					di.MoveTo(n);

					string msg = "Renamed directory " + DSPath;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
				}

				return true;
			}
			catch (Exception ex)
			{
				m_ErrorMessage = "Error renaming directory " + DSPath;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_ErrorMessage, ex);
				return false;
			}
		}	// End sub

		/// <summary>
		/// Decrypts password received from ini file
		/// </summary>
		/// <param name="enPwd">Encoded password</param>
		/// <returns>Clear text password</returns>
		private string DecodePassword(string enPwd)
		{
			// Decrypts password received from ini file
			// Password was created by alternately subtracting or adding 1 to the ASCII value of each character

			// Convert the password string to a character array
			char[] pwdChars = enPwd.ToCharArray();
			var pwdBytes = new byte[pwdChars.Length];
			var pwdCharsAdj = new char[pwdChars.Length];

			for (int i = 0; i < pwdChars.Length; i++)
			{
				pwdBytes[i] = (byte)pwdChars[i];
			}

			// Modify the byte array by shifting alternating bytes up or down and convert back to char, and add to output string
			string retStr = "";
			for (int byteCntr = 0; byteCntr < pwdBytes.Length; byteCntr++)
			{
				if ((byteCntr % 2) == 0)
				{
					pwdBytes[byteCntr] += 1;
				}
				else
				{
					pwdBytes[byteCntr] -= 1;
				}
				pwdCharsAdj[byteCntr] = (char)pwdBytes[byteCntr];
				retStr += pwdCharsAdj[byteCntr].ToString();
			}
			return retStr;
		}	// End sub

		/// <summary>
		/// Checks to see if folder size is changing -- possible sign acquisition hasn't finished
		/// </summary>
		/// <param name="folderPath">Full path specifying folder to check</param>
		/// <param name="sleepIntervalSeconds">Interval for checking (seconds)</param>
		/// <param name="retData">Output: return data</param>
		/// <returns>TRUE if folder size hasn't changed during SleepInt; FALSE otherwise</returns>
		private bool VerifyConstantFolderSize(string folderPath, int sleepIntervalSeconds, ref clsToolReturnData retData)
		{

			try
			{

				//Determines if the size of a folder changes over specified time interval

				// Sleep interval should be between 1 second and 15 minutes (900 seconds)
				if (sleepIntervalSeconds > 900)
					sleepIntervalSeconds = 900;

				if (sleepIntervalSeconds < 1)
					sleepIntervalSeconds = 1;

				//Get the initial size of the folder
				long InitialFolderSize = m_FileTools.GetDirectorySize(folderPath);

				//Wait for specified sleep interval
				Thread.Sleep(sleepIntervalSeconds * 1000);
				//Delay for specified interval

				//Get the final size of the folder and compare
				long FinalFolderSize = m_FileTools.GetDirectorySize(folderPath);

				if (FinalFolderSize == InitialFolderSize)
					return true;

				return false;

			}
			catch (Exception ex)
			{
				retData.CloseoutMsg = "Exception validating constant folder size";
				string msg = retData.CloseoutMsg + ": " + folderPath;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);

				HandleCopyException(ref retData, ex);
				return false;
			}

		}	// End sub

		/// <summary>
		/// Checks to see if file size is changing -- possible sign acquisition hasn't finished
		/// </summary>
		/// <param name="filePath">Full path specifying file to check</param>
		/// <param name="sleepIntervalSeconds">Interval for checking (seconds)</param>
		/// <param name="retData">Output: return data</param>
		/// <returns>TRUE if file size hasn't changed during SleepInt; FALSE otherwise</returns>
		private bool VerifyConstantFileSize(string filePath, int sleepIntervalSeconds, ref clsToolReturnData retData)
		{
			try
			{

				//Determines if the size of a file changes over specified time interval

				// Sleep interval should be between 1 second and 15 minutes (900 seconds)
				if (sleepIntervalSeconds > 900)
					sleepIntervalSeconds = 900;

				if (sleepIntervalSeconds < 1)
					sleepIntervalSeconds = 1;

				//Get the initial size of the file
				var Fi = new FileInfo(filePath);
				long InitialFileSize = Fi.Length;

				//Wait for specified sleep interval
				Thread.Sleep(sleepIntervalSeconds * 1000);
				//Delay for specified interval

				//Get the final size of the file and compare
				Fi = new FileInfo(filePath);
				long FinalFileSize = Fi.Length;

				if (FinalFileSize == InitialFileSize)
					return true;

				return false;

			}
			catch (Exception ex)
			{
				retData.CloseoutMsg = "Exception validating constant file size";
				string msg = retData.CloseoutMsg + ": " + filePath;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);

				HandleCopyException(ref retData, ex);
				return false;
			}

		}	// End sub

		/// <summary>
		/// Returns a string that describes the username and connection method currently active
		/// </summary>
		/// <returns></returns>
		private string GetConnectionDescription()
		{
			string sConnectionMode;

			switch (m_ConnectionType)
			{
				case ConnectionType.NotConnected:
					sConnectionMode = " as user " + Environment.UserName + " using fso";
					break;
				case ConnectionType.DotNET:
					sConnectionMode = " as user " + m_UserName + " using CaptureTaskManager.NetworkConnection";
					break;
				case ConnectionType.Prism:
					sConnectionMode = " as user " + m_UserName + " using PRISM.Files.ShareConnector";
					break;
				default:
					sConnectionMode = " via unknown connection mode";
					break;
			}

			return sConnectionMode;
		}
		/// <summary>
		/// Determines if raw dataset exists as a file or folder
		/// </summary>
		/// <param name="InstFolder">Full path to instrument transfer folder</param>
		/// <param name="DSName">Dataset name</param>
		/// <param name="instrumentClass">Instrument class for dataet to be located</param>
		/// <returns>clsDatasetInfo object containing info on found dataset</returns>
		private clsDatasetInfo GetRawDSType(string InstFolder, string DSName, clsInstrumentClassInfo.eInstrumentClass instrumentClass)
		{
			// Determines if raw dataset exists as a single file, folder with same name as dataset, or 
			// folder with dataset name + extension. Returns object containing info on dataset found

			bool bLookForDatasetFile;

            var datasetInfo = new clsDatasetInfo(DSName);

			// Verify that the instrument transfer folder exists
			if (!Directory.Exists(InstFolder))
			{
				datasetInfo.DatasetType = RawDSTypes.None;
				return datasetInfo;
			}

			switch (instrumentClass)
			{
				case clsInstrumentClassInfo.eInstrumentClass.BrukerMALDI_Imaging:
				case clsInstrumentClassInfo.eInstrumentClass.BrukerMALDI_Imaging_V2:
					bLookForDatasetFile = false;
					break;
				default:
					bLookForDatasetFile = true;
					break;
			}

			// First look for a file with name DSName, if not found, look for a folder
			// If bLookForDatasetFile=False, then we do the reverse: first look for a folder, then look for a file
			for (int iIteration = 0; iIteration < 2; ++iIteration)
			{
				if (bLookForDatasetFile)
				{
					// Get all files with a specified name
					var foundFiles = Directory.GetFiles(InstFolder, DSName + ".*");
					if (foundFiles.Length > 0)
					{
						datasetInfo.FileOrFolderName = DSName;
						datasetInfo.FileList = foundFiles;
						if (datasetInfo.FileCount == 1)
						{
							datasetInfo.FileOrFolderName = Path.GetFileName(datasetInfo.FileList[0]);
							datasetInfo.DatasetType = RawDSTypes.File;
						}
						else
							datasetInfo.DatasetType = RawDSTypes.MultiFile;

						return datasetInfo;
					}
				}
				else
				{
					// Check for a folder with specified name
					var subFolders = Directory.GetDirectories(InstFolder);
					foreach (string testFolder in subFolders)
					{
						//Using Path.GetFileNameWithoutExtension on folders is cheezy, but it works. I did this
						//	because the Path class methods that deal with directories ignore the possibilty there
						//	might be an extension. Apparently when sending in a string, Path can't tell a file from
						//	a directory
						var fileNameWithoutExtension = Path.GetFileNameWithoutExtension(testFolder);
						if (fileNameWithoutExtension != null && String.Equals(fileNameWithoutExtension, DSName, StringComparison.CurrentCultureIgnoreCase))
						{
							if (string.IsNullOrEmpty(Path.GetExtension(testFolder)))
							{
								// Found a directory that has no extension
								datasetInfo.FileOrFolderName = Path.GetFileName(testFolder);

								// Check the instrument class to determine the appropriate return type
								switch (instrumentClass)
								{
									case clsInstrumentClassInfo.eInstrumentClass.BrukerMALDI_Imaging:
										datasetInfo.DatasetType = RawDSTypes.BrukerImaging;
										break;
									case clsInstrumentClassInfo.eInstrumentClass.BrukerMALDI_Spot:
										datasetInfo.DatasetType = RawDSTypes.BrukerSpot;
										break;
									default:
										datasetInfo.DatasetType = RawDSTypes.FolderNoExt;
										break;
								}
								return datasetInfo;
							}

							// Directory name has an extension
							datasetInfo.FileOrFolderName = Path.GetFileName(testFolder);
							datasetInfo.DatasetType = RawDSTypes.FolderExt;
							return datasetInfo;
						}
					}
				}

				bLookForDatasetFile = !bLookForDatasetFile;
			}

			// If we got to here, then the raw dataset wasn't found (either as a file or a folder), so there was a problem
			datasetInfo.DatasetType = RawDSTypes.None;
			return datasetInfo;

		}	// End sub

		/// <summary>
		/// Connect to a BioNet share using either m_ShareConnectorPRISM or m_ShareConnectorDotNET
		/// </summary>
		/// <param name="userName">Username</param>
		/// <param name="pwd">Password</param>
		/// <param name="shareFolderPath">Share path</param>
		/// <param name="eConnectionType">Connection type enum (ConnectionType.DotNET or ConnectionType.Prism)</param>
		/// <param name="eCloseoutType">Closeout code (output)</param>
		/// <param name="eEvalCode"></param>
		/// <returns>True if success, false if an error</returns>
		private bool ConnectToShare(string userName, string pwd, string shareFolderPath, ConnectionType eConnectionType, out EnumCloseOutType eCloseoutType, out EnumEvalCode eEvalCode)
		{
			bool bSuccess;

			if (eConnectionType == ConnectionType.DotNET)
			{
				bSuccess = ConnectToShare(userName, pwd, shareFolderPath, out m_ShareConnectorDotNET, out eCloseoutType, out eEvalCode);
			}
			else
			{
				// Assume Prism Connector
				bSuccess = ConnectToShare(userName, pwd, shareFolderPath, out m_ShareConnectorPRISM, out eCloseoutType, out eEvalCode);
			}

			return bSuccess;

		}

		/// <summary>
		/// Connect to a remote share using a specific username and password
		/// Uses class PRISM.Files.ShareConnector
		/// </summary>
		/// <param name="userName">Username</param>
		/// <param name="pwd">Password</param>
		/// <param name="shareFolderPath">Share path</param>
		/// <param name="myConn">Connection object (output)</param>
		/// <param name="eCloseoutType">Closeout code (output)</param>
		/// <param name="eEvalCode"></param>
		/// <returns>True if success, false if an error</returns>
		private bool ConnectToShare(string userName, string pwd, string shareFolderPath, out ShareConnector myConn, out EnumCloseOutType eCloseoutType, out EnumEvalCode eEvalCode)
		{
			eCloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
			eEvalCode = EnumEvalCode.EVAL_CODE_SUCCESS;

			myConn = new ShareConnector(userName, pwd)
			{
				Share = shareFolderPath
			};

			if (myConn.Connect())
			{
				string msg = "Connected to Bionet (" + shareFolderPath + ") as user " + userName + " using PRISM.Files.ShareConnector";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
				m_ConnectionType = ConnectionType.Prism;
				return true;
			}
			else
			{
				m_ErrorMessage = "Error " + myConn.ErrorMessage + " connecting to " + shareFolderPath + " as user " + userName + " using 'secfso'";

				string msg = string.Copy(m_ErrorMessage);

				if (myConn.ErrorMessage == "1326")
					msg += "; you likely need to change the Capture_Method from secfso to fso";
				if (myConn.ErrorMessage == "53")
					msg += "; the password may need to be reset";

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);

				if (myConn.ErrorMessage == "1219" || myConn.ErrorMessage == "1203" || myConn.ErrorMessage == "53" || myConn.ErrorMessage == "64")
				{
					// Likely had error "An unexpected network error occurred" while copying a file for a previous dataset
					// Need to completely exit the capture task manager
					m_NeedToAbortProcessing = true;
					eCloseoutType = EnumCloseOutType.CLOSEOUT_NEED_TO_ABORT_PROCESSING;
					eEvalCode = EnumEvalCode.EVAL_CODE_NETWORK_ERROR_RETRY_CAPTURE;
				}
				else
				{
					eCloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
				}

				m_ConnectionType = ConnectionType.NotConnected;
				return false;
			}

		}

		/// <summary>
		/// Connect to a remote share using a specific username and password
		/// Uses class CaptureTaskManager.NetworkConnection
		/// </summary>
		/// <param name="userName">Username</param>
		/// <param name="pwd">Password</param>
		/// <param name="shareFolderPath">Share path</param>
		/// <param name="myConn">Connection object (output)</param>
		/// <param name="eCloseoutType">Closeout code (output)</param>
		/// <param name="eEvalCode"></param>
		/// <returns>True if success, false if an error</returns>
		private bool ConnectToShare(string userName, string pwd, string shareFolderPath, out NetworkConnection myConn, out EnumCloseOutType eCloseoutType, out EnumEvalCode eEvalCode)
		{
			eEvalCode = EnumEvalCode.EVAL_CODE_SUCCESS;
			myConn = null;

			try
			{
				// Make sure shareFolderPath does not end in a back slash
				if (shareFolderPath.EndsWith(@"\"))
					shareFolderPath = shareFolderPath.Substring(0, shareFolderPath.Length - 1);

				var accessCredentials = new System.Net.NetworkCredential(userName, pwd, "");

				myConn = new NetworkConnection(shareFolderPath, accessCredentials);

				string msg = "Connected to Bionet (" + shareFolderPath + ") as user " + userName + " using CaptureTaskManager.NetworkConnection";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
				m_ConnectionType = ConnectionType.DotNET;

				eCloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
				return true;

			}
			catch (Exception ex)
			{
				m_ErrorMessage = "Error connecting to " + shareFolderPath + " as user " + userName + " (using NetworkConnection class)";
				string msg = m_ErrorMessage + ": " + ex.Message;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);

				var retData = new clsToolReturnData();
				HandleCopyException(ref retData, ex);

				eCloseoutType = retData.CloseoutType;
				eEvalCode = retData.EvalCode;

				m_ConnectionType = ConnectionType.NotConnected;
				return false;

			}

		}

		/// <summary>
		/// Disconnect from a bionet share if required
		/// </summary>
		private void DisconnectShareIfRequired()
		{
			if (m_ConnectionType == ConnectionType.Prism)
				DisconnectShare(ref m_ShareConnectorPRISM);
			else if (m_ConnectionType == ConnectionType.DotNET)
				DisconnectShare(ref m_ShareConnectorDotNET);
		}

		/// <summary>
		/// Disconnects a Bionet shared drive
		/// </summary>
		/// <param name="myConn">Connection object (class PRISM.Files.ShareConnector) for shared drive</param>
		private void DisconnectShare(ref ShareConnector myConn)
		{
			myConn.Disconnect();
			PRISM.Processes.clsProgRunner.GarbageCollectNow();

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Bionet disconnected");
			m_ConnectionType = ConnectionType.NotConnected;

		}	// End sub

		/// <summary>
		/// Disconnects a Bionet shared drive
		/// </summary>
		/// <param name="myConn">Connection object (class CaptureTaskManager.NetworkConnection) for shared drive</param>
		private void DisconnectShare(ref NetworkConnection myConn)
		{
			myConn.Dispose();
			myConn = null;
			PRISM.Processes.clsProgRunner.GarbageCollectNow();

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Bionet disconnected");
			m_ConnectionType = ConnectionType.NotConnected;

		}	// End sub


		/// <summary>
		/// Perform a single capture operation
		/// </summary>
		/// <param name="taskParams">Enum indicating status of task</param>
		/// <param name="retData">Return data class; update CloseoutMsg or EvalMsg with text to store in T_Job_Step_Params</param>
		/// <returns></returns>
		public bool DoOperation(ITaskParams taskParams, ref clsToolReturnData retData)
		{
			string datasetName = taskParams.GetParam("Dataset");
			string sourceVol = taskParams.GetParam("Source_Vol");						// Example: \\exact04.bionet\
			string sourcePath = taskParams.GetParam("Source_Path");						// Example: ProteomicsData\
			string storageVol = taskParams.GetParam("Storage_Vol");						// Example: E:\
			string storagePath = taskParams.GetParam("Storage_Path");					// Example: Exact04\2012_1\
			string storageVolExternal = taskParams.GetParam("Storage_Vol_External");	// Example: \\proto-5\

			string instClassName = taskParams.GetParam("Instrument_Class");
			clsInstrumentClassInfo.eInstrumentClass instrumentClass = clsInstrumentClassInfo.GetInstrumentClass(instClassName);

			string shareConnectorType = m_MgrParams.GetParam("ShareConnectorType");		// Should be PRISM or DotNET
			string computerName = Environment.MachineName;

			ConnectionType eConnectionType;

			int maxFileCountToAllowResume = 0;
			int maxFolderCountToAllowResume = 0;

            if ((computerName.ToUpper() == "MONROE3") && datasetName == "2014_10_14_SRFAI-20ppm_Neg_15T_TOF0p65_IAT0p05_144scans_8M_000001")
			{
                // Override sourceVol, sourcePath, and m_UseBioNet when processing 2014_10_14_SRFAI-20ppm_Neg_15T_TOF0p65_IAT0p05_144scans_8M_000001 on Monroe3
				sourceVol = "\\\\protoapps\\";
				sourcePath = "userdata\\Matt\\";
				m_UseBioNet = false;
				m_SleepInterval = 2;
			}

			// Determine when connector class will be used to connect to Bionet
			// This is defined by manager parameter ShareConnectorType
            // Default in October 2014 is PRISM
			if (shareConnectorType.ToLower() == "dotnet")
				eConnectionType = ConnectionType.DotNET;
			else
				eConnectionType = ConnectionType.Prism;

			// Determine whether or not we will use Copy with Resume
			// This determines whether or not we add x_ to an existing file or folder, 
			// and determines whether we use CopyDirectory or CopyFolderWithResume/CopyFileWithResume
			bool bCopyWithResume = false;
			switch (instrumentClass)
			{
				case clsInstrumentClassInfo.eInstrumentClass.BrukerFT_BAF:
					bCopyWithResume = true;
					break;
				case clsInstrumentClassInfo.eInstrumentClass.BrukerMALDI_Imaging:
				case clsInstrumentClassInfo.eInstrumentClass.BrukerMALDI_Imaging_V2:
					bCopyWithResume = true;
					maxFileCountToAllowResume = 10;
					maxFolderCountToAllowResume = 1;
					break;
			}

			RawDSTypes sourceType;
			string pwd = DecodePassword(m_Pwd);
			string msg;
			string tempVol;
			clsDatasetInfo datasetInfo;

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Started clsCaptureOps.DoOperation()");

			// Setup destination folder based on client/server switch
			// True means MgrParam "perspective" =  "client" which means we will use paths like \\proto-5\Exact04\2012_1
			// False means MgrParam "perspective" = "server" which means we use paths like E:\Exact04\2012_1

			if (!m_ClientServer)
			{
				// Look for job parameter Storage_Server_Name in storageVolExternal
				// If m_ClientServer=false but storageVolExternal does not contain Storage_Server_Name then auto-switch m_ClientServer to true

				if (!storageVolExternal.ToLower().Contains(computerName.ToLower()))
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Auto-changing m_ClientServer to True (perspective=client) because " + storageVolExternal + " does not contain " + computerName);
					m_ClientServer = true;
				}
			}

			if (m_ClientServer)
			{
				// Example: \\proto-5\
				tempVol = storageVolExternal;
			}
			else
			{
				// Example: E:\
				tempVol = storageVol;
			}

			// Set up paths
			string storageFolderPath = Path.Combine(tempVol, storagePath);	// Directory on storage server where dataset folder goes
			string datasetFolderPath;

			// If Storage_Folder_Name <> "", then use it in target folder path. Otherwise use dataset name
			if (taskParams.GetParam("Storage_Folder_Name") != "")
			{
				datasetFolderPath = Path.Combine(storageFolderPath, taskParams.GetParam("Storage_Folder_Name")); // HPLC run folder storage path
			}
			else
			{
                datasetFolderPath = Path.Combine(storageFolderPath, datasetName);	// Dataset folder complete path
			}

			// Verify that the storage folder on storage server does exist; e.g. \\proto-9\VOrbiETD02\2011_2
			if (!ValidateFolderPath(storageFolderPath))
			{
				msg = "Storage folder '" + storageFolderPath + "' does not exist; will auto-create";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);

				try
				{
					Directory.CreateDirectory(storageFolderPath);
					msg = "Successfully created " + storageFolderPath;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
				}
				catch
				{
					retData.CloseoutMsg = "Error creating missing storage folder";
					msg = retData.CloseoutMsg + ": " + storageFolderPath;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);

					PossiblyStoreErrorMessage(ref retData);
					if (retData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS)
						retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;

					return false;
				}
			}

			// Verify dataset folder path doesn't already exist or is empty
			// Example: \\proto-9\VOrbiETD02\2011_2\PTO_Na_iTRAQ_2_17May11_Owl_11-05-09
			if (ValidateFolderPath(datasetFolderPath))
			{
				// Dataset folder exists, so take action specified in configuration
				if (!PerformDSExistsActions(datasetFolderPath, bCopyWithResume, maxFileCountToAllowResume, maxFolderCountToAllowResume, ref retData))
				{
					PossiblyStoreErrorMessage(ref retData);
					if (retData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS)
						retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;

					if (string.IsNullOrEmpty(retData.CloseoutMsg))
						retData.CloseoutMsg = "PerformDSExistsActions returned false";

					return false;
				}
			}

			// Construct the path to the dataset on the instrument
			// Determine if source dataset exists, and if it is a file or a folder
			string sourceFolderPath = Path.Combine(sourceVol, sourcePath);

			// Connect to Bionet if necessary
			if (m_UseBioNet)
			{
				msg = "Bionet connection required";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

				EnumCloseOutType eCloseoutType;
				EnumEvalCode eEvalCode;

				if (!ConnectToShare(m_UserName, pwd, sourceFolderPath, eConnectionType, out eCloseoutType, out eEvalCode))
				{
					retData.CloseoutType = eCloseoutType;
					retData.EvalCode = eEvalCode;

					PossiblyStoreErrorMessage(ref retData);
					if (retData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS)
						retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;

					if (string.IsNullOrEmpty(retData.CloseoutMsg))
						retData.CloseoutMsg = "Error connecting to Bionet share";

					return false;
				}
			}
			else
			{
				msg = "Bionet connection not required";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
			}

			//If Source_Folder_Name is non-blank, use it. Otherwise use dataset name
			string sSourceFolderName = taskParams.GetParam("Source_Folder_Name");

			if (!string.IsNullOrWhiteSpace(sSourceFolderName))
			{
				datasetInfo = GetRawDSType(sourceFolderPath, sSourceFolderName, instrumentClass);
				sourceType = datasetInfo.DatasetType;
                datasetInfo.DatasetName = datasetName;
			}
			else
			{
                datasetInfo = GetRawDSType(sourceFolderPath, datasetName, instrumentClass);
				sourceType = datasetInfo.DatasetType;
			}

			// Set the closeout type to Failed for now
			retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
			bool sourceIsValid;

			if (sourceType == RawDSTypes.None)
			{
				// No dataset file or folder found
				retData.CloseoutMsg = "Dataset data file not found";
                msg = retData.CloseoutMsg + ": " + datasetName;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
				sourceIsValid = false;
			}
			else
			{
                sourceIsValid = ValidateWithInstrumentClass(datasetName, sourceFolderPath, sourceType, instrumentClass, datasetInfo, ref retData);
			}

			if (sourceIsValid)
			{
				// Perform copy based on source type
				switch (sourceType)
				{
					case RawDSTypes.File:
						CaptureFile(ref msg, ref retData, datasetInfo, sourceFolderPath, datasetFolderPath, bCopyWithResume);
						break;

					case RawDSTypes.FolderExt:
						CaptureFolderExt(ref msg, ref retData, datasetInfo, sourceFolderPath, datasetFolderPath, bCopyWithResume);
						break;

					case RawDSTypes.FolderNoExt:
						CaptureFolderNoExt(ref msg, ref retData, datasetInfo, sourceFolderPath, datasetFolderPath,
										   bCopyWithResume, instrumentClass);
						break;

					case RawDSTypes.BrukerImaging:
						CaptureBrukerImaging(ref msg, ref retData, datasetInfo, sourceFolderPath, datasetFolderPath,
											 bCopyWithResume);
						break;

					case RawDSTypes.BrukerSpot:
						CaptureBrukerSpot(ref msg, ref retData, datasetInfo, sourceFolderPath, datasetFolderPath);
						break;

					default:
						retData.CloseoutMsg = "Invalid dataset type found: " + sourceType;
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, retData.CloseoutMsg);
						DisconnectShareIfRequired();
						retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
						break;
				}
			}

			PossiblyStoreErrorMessage(ref retData);

			if (retData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS)
				return true;

			if (string.IsNullOrEmpty(retData.CloseoutMsg))
			{
				retData.CloseoutMsg = string.Copy(msg);
				if (string.IsNullOrEmpty(retData.CloseoutMsg))
					retData.CloseoutMsg = "Unknown error performing capture";
			}
			return false;
		}

		private void CaptureFile(ref string msg, ref clsToolReturnData retData, clsDatasetInfo datasetInfo, string sourceFolderPath, string datasetFolderPath, bool bCopyWithResume)
		{
			// Dataset found, and it's a single file
			// First, verify the file size is constant (indicates acquisition is actually finished)
			string sourceFilePath = Path.Combine(sourceFolderPath, datasetInfo.FileOrFolderName);
			bool bSuccess;

			if (!VerifyConstantFileSize(sourceFilePath, m_SleepInterval, ref retData))
			{
                msg = "Dataset '" + datasetInfo.DatasetName + "' not ready";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
				DisconnectShareIfRequired();
				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_NOT_READY;
				return;
			}

			// Make a dataset folder (it's OK if it already exists)
			try
			{
				MakeFolderPath(datasetFolderPath);
			}
			catch (Exception ex)
			{
				retData.CloseoutMsg = "Exception creating dataset folder";
				msg = retData.CloseoutMsg + " at " + datasetFolderPath;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg, ex);
				DisconnectShareIfRequired();

				HandleCopyException(ref retData, ex);
				return;
			}

			// Copy the file to the dataset folder
			try
			{
				// Copy the raw spectra file
				string targetFilePath = Path.Combine(datasetFolderPath, datasetInfo.FileOrFolderName);

				if (bCopyWithResume)
				{
					var fiSourceFile = new FileInfo(sourceFilePath);
					bool bResumed = false;

					bSuccess = m_FileTools.CopyFileWithResume(fiSourceFile, targetFilePath, out bResumed);
				}
				else
				{
					File.Copy(sourceFilePath, targetFilePath);
					bSuccess = true;
				}

				if (bSuccess)
				{
					msg = "  copied file " + sourceFilePath + " to " + targetFilePath + GetConnectionDescription();
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
				}
				else
				{
					msg = "  file copy failed for " + sourceFilePath + " to " + targetFilePath + GetConnectionDescription();
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
				}
			}
			catch (Exception ex)
			{
                msg = "Copy exception for dataset " + datasetInfo.DatasetName + GetConnectionDescription();
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg, ex);

				HandleCopyException(ref retData, ex);
				return;
			}
			finally
			{
				DisconnectShareIfRequired();
			}

			if (bSuccess)
			{
                bSuccess = CaptureLCMethodFile(datasetInfo.DatasetName, datasetFolderPath);
			}

			if (bSuccess)
				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
			else
				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;

		}

		/// <summary>
		/// Looks for the LCMethod file for this dataset
		/// Copies this file to the dataset folder
		/// </summary>
		/// <param name="datasetName"></param>
		/// <param name="datasetFolderPath"></param>
		/// <returns>True if file found and copied; false if an error</returns>
		/// <remarks>Returns true if the .lcmethod file is not found</remarks>
		private bool CaptureLCMethodFile(string datasetName, string datasetFolderPath)
		{
			const string METHOD_FOLDER_BASE_PATH = "\\\\proto-5\\BionetXfer\\Run_Complete_Trigger\\MethodFiles";

			string msg;
			bool bSuccess = true;

			// Look for an LCMethod file associated with this raw spectra file
			// Note that this file is often created 45 minutes to 60 minutes after the run completes
			// and thus when capturing a dataset with an auto-created trigger file, we most likely will not find the .lcmethod file

			// Files are stored at \\proto-5\BionetXfer\Run_Complete_Trigger\MethodFiles\  (we could lookup this parameter from the manager control DB, but it rarely changes and thus isn't worth it)
			// The file will either be located in a folder with the dataset name, or will be in a subfolder based on the year and quarter that the data was acquired

			try
			{
				var diSourceFolder = new DirectoryInfo(METHOD_FOLDER_BASE_PATH);
				if (!diSourceFolder.Exists)
				{
					msg = "LCMethods folder not found: " + METHOD_FOLDER_BASE_PATH;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, msg);

					// Return true despite not having found the folder since this is not a fatal error for capture
					return true;
				}

				// Construct a list of folders to search
				var lstFoldersToSearch = new List<string>
				{
					datasetName
				};

				int iYear = DateTime.Now.Year;
				int iQuarter = GetQuarter(DateTime.Now);

				while (iYear >= 2011)
				{
					lstFoldersToSearch.Add(iYear + "_" + iQuarter);

					if (iQuarter > 1)
						--iQuarter;
					else
					{
						iQuarter = 4;
						--iYear;
					}

					if (iYear == 2011 && iQuarter == 2)
						break;
				}

				// This regex is used to match files with names like:
				// Cheetah_01.04.2012_08.46.17_Sarc_P28_D01_2629_192_3Jan12_Cheetah_11-09-32.lcmethod
				var reLCMethodFile = new Regex(@".+\d+\.\d+\.\d+_\d+\.\d+\.\d+_.+\.lcmethod");
				var lstMethodFiles = new List<FileInfo>();

				// Define the file match spec
				string sLCMethodSearchSpec = "*_" + datasetName + ".lcmethod";

				for (int iIteration = 0; iIteration <= 1; iIteration++)
				{

					foreach (string sFolderName in lstFoldersToSearch)
					{
						var diSubFolder = new DirectoryInfo(Path.Combine(diSourceFolder.FullName, sFolderName));
						if (diSubFolder.Exists)
						{
							// Look for files that match sLCMethodSearchSpec
							// There might be multiple files if the dataset was analyzed more than once
							foreach (FileInfo fiFile in diSubFolder.GetFiles(sLCMethodSearchSpec))
							{
								if (iIteration == 0)
								{
									// First iteration
									// Check each file against the RegEx
									if (reLCMethodFile.IsMatch(fiFile.Name))
									{
										// Match found
										lstMethodFiles.Add(fiFile);
									}
								}
								else
								{
									// Second iteration; accept any match
									lstMethodFiles.Add(fiFile);
								}
							}
						}

						if (lstMethodFiles.Count > 0)
							break;

					} // End ForEach

				} // End For

				if (lstMethodFiles.Count == 0)
				{
					// LCMethod file not found; exit function
					return true;
				}

				// LCMethod file found
				// Copy to the dataset folder

				foreach (FileInfo fiFile in lstMethodFiles)
				{
					try
					{
						string targetFilePath = Path.Combine(datasetFolderPath, fiFile.Name);
						fiFile.CopyTo(targetFilePath, true);
					}
					catch (Exception ex)
					{
						msg = "Exception copying LCMethod file " + fiFile.FullName;
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg, ex);
					}

				}

				// If the file was found in a dataset folder, then rename the source folder to start with x_
				if (String.Equals(lstMethodFiles[0].Directory.Name, datasetName, StringComparison.CurrentCultureIgnoreCase))
				{
					try
					{
						string strRenamedSourceFolder = Path.Combine(METHOD_FOLDER_BASE_PATH, "x_" + datasetName);

						if (Directory.Exists(strRenamedSourceFolder))
						{
							// x_ folder already exists; move the files
							foreach (FileInfo fiFile in lstMethodFiles)
							{
								string targetFilePath = Path.Combine(strRenamedSourceFolder, fiFile.Name);

								fiFile.CopyTo(targetFilePath, true);
								fiFile.Delete();
							}
							diSourceFolder.Delete(false);
						}
						else
						{
							// Rename the folder
							diSourceFolder.MoveTo(strRenamedSourceFolder);
						}
					}
					catch (Exception ex)
					{
						// Exception renaming the folder; only log this as a debug message
						msg = "Exception renaming source LCMethods folder for " + datasetName;
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg, ex);
					}
				}

			}
			catch (Exception ex)
			{
				msg = "Exception copying LCMethod file for " + datasetName;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
				bSuccess = false;
			}

			DateTime dtCurrentTime = DateTime.Now;
			if (dtCurrentTime.Hour == 18 || dtCurrentTime.Hour == 19 || (Environment.MachineName.ToUpper() == "MONROE3"))
			{
				// Time is between 6 pm and 7:59 pm
				// Check for folders at METHOD_FOLDER_BASE_PATH that start with x_ and have .lcmethod files that are all at least 14 days old
				// These folders are safe to delete
				DeleteOldLCMethodFolders(METHOD_FOLDER_BASE_PATH);
			}

			return bSuccess;
		}

		private void CaptureFolderExt(ref string msg, ref clsToolReturnData retData, clsDatasetInfo datasetInfo, string sourceFolderPath, string datasetFolderPath, bool bCopyWithResume)
		{
			// Dataset found in a folder with an extension on the folder name

			List<string> filesToSkip = null;
			
			bool bSuccess;
			int iSleepInterval = m_SleepInterval;

			string copySourceDir = Path.Combine(sourceFolderPath, datasetInfo.FileOrFolderName);
			string copyTargetDir = Path.Combine(datasetFolderPath, datasetInfo.FileOrFolderName);

			if (!VerifyConstantFolderSize(copySourceDir, iSleepInterval, ref retData))
			{
                msg = "Dataset '" + datasetInfo.DatasetName + "' not ready";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
				DisconnectShareIfRequired();
				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_NOT_READY;
				return;
			}

		    bool brukerDotDFolder = false;

		    if (datasetInfo.FileOrFolderName.ToLower().EndsWith(".d"))
            {
                // Bruker .D folder (common for the 12T and 15T)
                // Look for journal files, which we can never copy because they are always locked

                brukerDotDFolder = true;

                if (!FindFilesToSkip(copySourceDir, datasetInfo, "*.mcf_idx-journal", "journal file", ref  retData, out filesToSkip))
                    return;
		    }
		  

			// Make a dataset folder
			try
			{
				MakeFolderPath(datasetFolderPath);
			}
			catch (Exception ex)
			{
				retData.CloseoutMsg = "Exception creating dataset folder";
				msg = retData.CloseoutMsg + " at " + datasetFolderPath;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg, ex);
				DisconnectShareIfRequired();

				HandleCopyException(ref retData, ex);
				return;
			}

			// Copy the source folder to the dataset folder
			try
			{
				// Copy the dataset folder
				// Resume copying files that are already present in the target

				if (bCopyWithResume)
				{
					const bool bRecurse = true;
					bSuccess = CopyFolderWithResume(copySourceDir, copyTargetDir, bRecurse, ref retData, filesToSkip);
				}
				else
				{
					m_FileTools.CopyDirectory(copySourceDir, copyTargetDir, filesToSkip);
					bSuccess = true;
				}

				if (bSuccess)
				{
					msg = "Copied folder " + copySourceDir + " to " + copyTargetDir + GetConnectionDescription();
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
				}
			}
			catch (Exception ex)
			{
                msg = "Copy exception for dataset " + datasetInfo.DatasetName + GetConnectionDescription();
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg, ex);

                DisconnectShareIfRequired();

                HandleCopyException(ref retData, ex);
                return;
			}

			DisconnectShareIfRequired();

			if (bSuccess)
			{
                bSuccess = CaptureLCMethodFile(datasetInfo.DatasetName, datasetFolderPath);
                
                if (brukerDotDFolder)
                {
                    // Look for and delete certain zero-byte files
                    DeleteZeroByteBrukerFiles(copyTargetDir);
                }
            }

            if (bSuccess)
				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
			else
				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;

		}

	    private void DeleteZeroByteBrukerFiles(string copyTargetDir)
	    {
	        try
	        {
	            var fileNamesToDelete = new List<string>
	            {
	                "ProjectCreationHelper",
	                "SyncHelper",
                    "lock.file"
	            };

	            var fileCountDeleted = 0;
                string deletedFileList = string.Empty;

	            var diTarget = new DirectoryInfo(copyTargetDir);
                if (!diTarget.Exists)
                    return;

	            var lstFiles = diTarget.GetFiles("*", SearchOption.AllDirectories).ToList();

                foreach (var fiFile in lstFiles)
	            {
	                if (fiFile.Length == 0)
	                {
	                    if (fileNamesToDelete.Contains(fiFile.Name))
	                    {
                            // Delete this zero-byte file
	                        fiFile.Delete();
	                        fileCountDeleted++;
                            if (string.IsNullOrEmpty(deletedFileList))
                                deletedFileList = fiFile.Name;
                            else
                                deletedFileList += ", " + fiFile.Name;
	                    }
	                }
	            }

	            if (fileCountDeleted > 0)
	            {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Deleted " + fileCountDeleted + " zero byte files in the dataset folder: " + deletedFileList);
	            }
	        }
            catch (Exception ex)
            {              
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in DeleteZeroByteBrukerFiles", ex);
            }

	    }

	    private bool FindFilesToSkip(string copySourceDir, clsDatasetInfo datasetInfo, string searchSpec, string searchSpecDescription, ref clsToolReturnData retData, out List<string> filesToSkip)
	    {
           
            filesToSkip = new List<string>();

            try
            {
                var fileList = Directory.GetFiles(copySourceDir, searchSpec, SearchOption.AllDirectories).ToList();
                foreach (string fileName in fileList)
                {
                    filesToSkip.Add(Path.GetFileName(fileName));
                }

                if (filesToSkip.Count == 1)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Skipping " + searchSpecDescription + " " + filesToSkip.FirstOrDefault());
                }
                else if (filesToSkip.Count > 1)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Skipping " + filesToSkip.Count + " " + searchSpecDescription + "s (" + filesToSkip.FirstOrDefault() + " through " + filesToSkip.LastOrDefault() + ")");
                }

                return true;

            }
            catch (Exception ex)
            {
                retData.CloseoutMsg = "Exception getting list of files to skip";
                var msg = retData.CloseoutMsg + " for dataset " + datasetInfo.DatasetName;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg, ex);
                DisconnectShareIfRequired();

                HandleCopyException(ref retData, ex);
                return false;
            }

	    }

	    private void CaptureFolderNoExt(ref string msg, ref clsToolReturnData retData, clsDatasetInfo datasetInfo, string sourceFolderPath, string datasetFolderPath, bool bCopyWithResume, clsInstrumentClassInfo.eInstrumentClass instrumentClass)
		{
			// Dataset found; it's a folder with no extension on the name

			bool bSuccess;

			var diSourceDir = new DirectoryInfo(Path.Combine(sourceFolderPath, datasetInfo.FileOrFolderName));

			// First, verify the folder size is constant (indicates acquisition is actually finished)
			if (!VerifyConstantFolderSize(diSourceDir.FullName, m_SleepInterval, ref retData))
			{
                msg = "Dataset '" + datasetInfo.DatasetName + "' not ready";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
				DisconnectShareIfRequired();
				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_NOT_READY;
				return;
			}

			// Verify the folder doesn't contain a group of ".d" folders
			var lstDotDFolders = diSourceDir.GetDirectories("*.d", SearchOption.TopDirectoryOnly);
			if (lstDotDFolders.Length > 1)
			{
				bool allowMultipleFolders = false;

				if (lstDotDFolders.Length == 2)
				{
					// If one folder contains a ser file and the other folder contains an analysis.baf, then we'll allow this
					// This is somtimes the case for the 15T_FTICR_Imaging
					int serCount = 0;
					int bafCount = 0;
					foreach (var diFolder in lstDotDFolders)
					{
						if (diFolder.GetFiles("ser", SearchOption.TopDirectoryOnly).Length == 1)
							serCount += 1;

						if (diFolder.GetFiles("analysis.baf", SearchOption.TopDirectoryOnly).Length == 1)
							bafCount += 1;
					}

					if (bafCount == 1 && serCount == 1)
						allowMultipleFolders = true;
				}

				if (!allowMultipleFolders)
				{
					retData.CloseoutMsg = "Multiple .D folders found in dataset folder";
					msg = retData.CloseoutMsg + " " + diSourceDir.FullName;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
					retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
					return;
				}
			}

			// Verify the folder doesn't contain ".IMF" files
			if (diSourceDir.GetFiles("*.imf", SearchOption.TopDirectoryOnly).Length > 0)
			{
				retData.CloseoutMsg = "Dataset folder contains a series of .IMF files -- upload a .UIMF file instead";
				msg = retData.CloseoutMsg + " " + diSourceDir.FullName;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
				return;
			}


			if (instrumentClass == clsInstrumentClassInfo.eInstrumentClass.Sciex_QTrap)
			{
				// Make sure that it doesn't have more than 2 subfolders (it typically won't have any, but we'll allow 2)				
				if (diSourceDir.GetDirectories("*", SearchOption.TopDirectoryOnly).Length > 2)
				{
					retData.CloseoutMsg = "Dataset folder has more than 2 subfolders";
					msg = retData.CloseoutMsg + " " + diSourceDir.FullName;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
					retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
					return;
				}

				// Verify that the folder has a .wiff or a .wiff.scan file
				if (diSourceDir.GetFiles("*.wiff*", SearchOption.TopDirectoryOnly).Length == 0)
				{
					retData.CloseoutMsg = "Dataset folder does not contain any .wiff files";
					msg = retData.CloseoutMsg + " " + diSourceDir.FullName;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
					retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
					return;
				}
			}

			// Copy the dataset folder to the storage server
			try
			{

				if (bCopyWithResume)
				{
					const bool bRecurse = true;
					bSuccess = CopyFolderWithResume(diSourceDir.FullName, datasetFolderPath, bRecurse, ref retData);
				}
				else
				{
					m_FileTools.CopyDirectory(diSourceDir.FullName, datasetFolderPath);
					bSuccess = true;
				}

				if (bSuccess)
				{
					msg = "Copied folder " + diSourceDir.FullName + " to " +
						Path.Combine(datasetFolderPath, datasetInfo.FileOrFolderName) + GetConnectionDescription();
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
				}
			}
			catch (Exception ex)
			{
				msg = "Exception copying dataset folder " + diSourceDir.FullName + GetConnectionDescription();
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg, ex);

				HandleCopyException(ref retData, ex);
				return;
			}
			finally
			{
				DisconnectShareIfRequired();
			}

			if (bSuccess)
			{
                bSuccess = CaptureLCMethodFile(datasetInfo.DatasetName, datasetFolderPath);
			}

			if (bSuccess)
				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
			else
				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
			
		}

		private void CaptureBrukerImaging(ref string msg, ref clsToolReturnData retData, clsDatasetInfo datasetInfo, string sourceFolderPath, string datasetFolderPath, bool bCopyWithResume)
		{
			// Dataset found; it's a Bruker imaging folder

			bool bSuccess;

			// First, verify the folder size is constant (indicates acquisition is actually finished)
			string copySourceDir = Path.Combine(sourceFolderPath, datasetInfo.FileOrFolderName);
			if (!VerifyConstantFolderSize(copySourceDir, m_SleepInterval, ref retData))
			{
                msg = "Dataset '" + datasetInfo.DatasetName + "' not ready";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
				DisconnectShareIfRequired();
				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_NOT_READY;
				return;
			}

			// Check to see if the folders have been zipped
			string[] zipFileList = Directory.GetFiles(copySourceDir, "*.zip");
			if (zipFileList.Length < 1)
			{
				// Data files haven't been zipped, so throw error
				retData.CloseoutMsg = "No zip files found in dataset folder";
				msg = retData.CloseoutMsg + " at " + datasetFolderPath;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
				DisconnectShareIfRequired();

				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
				return;
			}

			// Make a dataset folder
			try
			{
				MakeFolderPath(datasetFolderPath);
			}
			catch (Exception ex)
			{
				retData.CloseoutMsg = "Exception creating dataset folder";
				msg = retData.CloseoutMsg + " at " + datasetFolderPath;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg, ex);
				DisconnectShareIfRequired();

				HandleCopyException(ref retData, ex);
				return;
			}

			// Copy only the files in the dataset folder to the storage server. Do not copy folders
			try
			{
				if (bCopyWithResume)
				{
					const bool bRecurse = false;
					bSuccess = CopyFolderWithResume(copySourceDir, datasetFolderPath, bRecurse, ref retData);
				}
				else
				{

					string[] fileList = Directory.GetFiles(copySourceDir);

					foreach (string fileToCopy in fileList)
					{
						var fi = new FileInfo(fileToCopy);
						fi.CopyTo(Path.Combine(datasetFolderPath, fi.Name));
					}
					bSuccess = true;
				}

				if (bSuccess)
				{
					msg = "Copied files in folder " + copySourceDir + " to " +
						Path.Combine(datasetFolderPath, datasetInfo.FileOrFolderName) + GetConnectionDescription();
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
				}
			}
			catch (Exception ex)
			{
				retData.CloseoutMsg = "Exception copying files from dataset folder";
				msg = retData.CloseoutMsg + " " + copySourceDir + GetConnectionDescription();
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg, ex);
				DisconnectShareIfRequired();

				HandleCopyException(ref retData, ex);
				return;
			}
			finally
			{
				DisconnectShareIfRequired();
			}

			if (bSuccess)
				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
			else
				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;

		}

		private void CaptureBrukerSpot(ref string msg, ref clsToolReturnData retData, clsDatasetInfo datasetInfo, string sourceFolderPath, string datasetFolderPath)
		{
			// Dataset found; it's a Bruker_Spot instrument type
			// First, verify the folder size is constant (indicates acquisition is actually finished)
			string copySourceDir = Path.Combine(sourceFolderPath, datasetInfo.FileOrFolderName);
			if (!VerifyConstantFolderSize(copySourceDir, m_SleepInterval, ref retData))
			{
                msg = "Dataset '" + datasetInfo.DatasetName + "' not ready";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
				DisconnectShareIfRequired();
				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_NOT_READY;
				return;
			}

			// Verify the dataset folder doesn't contain any .zip files
			string[] zipFiles = Directory.GetFiles(copySourceDir, "*.zip");

			if (zipFiles.Length > 0)
			{
				retData.CloseoutMsg = "Zip files found in dataset folder";
				msg = retData.CloseoutMsg + " " + copySourceDir;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
				return;
			}


			// Check whether the dataset folder contains just one data folder or multiple data folders
			var dataFolders = Directory.GetDirectories(copySourceDir).ToList();

			if (dataFolders.Count < 1)
			{
				retData.CloseoutMsg = "No subfolders were found in the dataset folder ";
				msg = retData.CloseoutMsg + " " + copySourceDir;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
				return;
			}

			if (dataFolders.Count > 1)
			{
				// Make sure the subfolders match the naming convention for MALDI spot folders
				// Example folder names:
				//  0_D4
				//  0_E10
				//  0_N4

				const string MALDI_SPOT_FOLDER_REGEX = "^\\d_[A-Z]\\d+$";
				var reMaldiSpotFolder = new Regex(MALDI_SPOT_FOLDER_REGEX, RegexOptions.Compiled | RegexOptions.IgnoreCase);

				foreach (string sDir in dataFolders)
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Test folder " + sDir + " against RegEx " + reMaldiSpotFolder);

					string sDirName = Path.GetFileName(sDir);
					if (!reMaldiSpotFolder.IsMatch(sDirName, 0))
					{
						retData.CloseoutMsg = "Dataset folder contains multiple subfolders, but folder " + sDirName + " does not match the expected pattern";
						msg = retData.CloseoutMsg + " (" + reMaldiSpotFolder + "); see " + copySourceDir;
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
						retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
						return;
					}

				}
			}

			// Copy the dataset folder (and all subfolders) to the storage server
			try
			{
				m_FileTools.CopyDirectory(copySourceDir, datasetFolderPath);
				msg = "Copied folder " + copySourceDir + " to " +
					Path.Combine(datasetFolderPath, datasetInfo.FileOrFolderName) + GetConnectionDescription();
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;				
			}
			catch (Exception ex)
			{
				msg = "Exception copying dataset folder " + copySourceDir + GetConnectionDescription();
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg, ex);

				HandleCopyException(ref retData, ex);
			}
			finally
			{
				DisconnectShareIfRequired();
			}
		}

		private bool CopyFolderWithResume(string sSourceFolderPath, string sTargetFolderPath, bool bRecurse, ref clsToolReturnData retData)
		{
			return CopyFolderWithResume(sSourceFolderPath, sTargetFolderPath, bRecurse, ref retData, null);
		}

		private bool CopyFolderWithResume(string sSourceFolderPath, string sTargetFolderPath, bool bRecurse, ref clsToolReturnData retData, List<string> filesToSkip)
		{
			const clsFileTools.FileOverwriteMode eFileOverwriteMode = clsFileTools.FileOverwriteMode.OverWriteIfDateOrLengthDiffer;

		    bool bSuccess = false;
			bool bDoCopy = true;

			while (bDoCopy)
			{
				DateTime dtCopyStart = DateTime.UtcNow;

				string msg;
				try
				{
					// Clear any previous errors
					m_ErrorMessage = string.Empty;

				    int iFileCountSkipped;
				    int iFileCountResumed;
				    int iFileCountNewlyCopied;

				    bSuccess = m_FileTools.CopyDirectoryWithResume(sSourceFolderPath, sTargetFolderPath, bRecurse, eFileOverwriteMode, filesToSkip, out iFileCountSkipped, out iFileCountResumed, out iFileCountNewlyCopied);
					bDoCopy = false;

					if (bSuccess)
					{
						msg = "  directory copy complete; CountCopied = " + iFileCountNewlyCopied + "; CountSkipped = " + iFileCountSkipped + "; CountResumed = " + iFileCountResumed;
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
					}
					else
					{
						msg = "  directory copy failed for " + sSourceFolderPath + " to " + sTargetFolderPath + GetConnectionDescription();
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
					}

				}
				catch (Exception ex)
				{
					if (string.IsNullOrWhiteSpace(m_FileTools.CurrentSourceFile))
						msg = "Error while copying directory: ";
					else
						msg = "Error while copying " + m_FileTools.CurrentSourceFile + ": ";

					m_ErrorMessage = string.Copy(msg);

					if (ex.Message.Length <= 350)
						msg += ex.Message;
					else
                        msg += ex.Message.Substring(0, 350);

					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);

					bDoCopy = false;
					if (m_FileTools.CurrentCopyStatus == clsFileTools.CopyStatus.BufferedCopy ||
						m_FileTools.CurrentCopyStatus == clsFileTools.CopyStatus.BufferedCopyResume)
					{
						// Exception occurred during the middle of a buffered copy
						// If at least 10 seconds have elapsed, then auto-retry the copy again
						double dElapsedTime = DateTime.UtcNow.Subtract(dtCopyStart).TotalSeconds;
						if (dElapsedTime >= 10)
						{
							bDoCopy = true;
							msg = "  " + dElapsedTime.ToString("0") + " seconds have elapsed; will attempt to resume copy";
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
						}
					}

					HandleCopyException(ref retData, ex);

				}
			}

			if (bSuccess)
			{
				// CloseoutType may have been set to CLOSEOUT_FAILED by HandleCopyException; reset it to CLOSEOUT_SUCCESS
				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
				retData.EvalCode = EnumEvalCode.EVAL_CODE_SUCCESS;
			}
			return bSuccess;

		}

		/// <summary>
		/// Look for LCMethod folders that start with x_ and have .lcmethod files that are more than 2 weeks old
		/// Matching folders are deleted
		/// Note that in February 2012 we plan to switch to saving .lcmethod files in Year_Quarter folders (e.g. 2012_1 or 2012_2) and thus we won't need to call this function in the future
		/// </summary>
		/// <param name="sLCMethodsFolderPath"></param>
		private void DeleteOldLCMethodFolders(string sLCMethodsFolderPath)
		{
			string msg = string.Empty;

			try
			{
				var diLCMethodsFolder = new DirectoryInfo(sLCMethodsFolderPath);
				if (diLCMethodsFolder.Exists)
				{
					DirectoryInfo[] diSubfolders = diLCMethodsFolder.GetDirectories("x_*");

					foreach (DirectoryInfo diFolder in diSubfolders)
					{
						bool bSafeToDelete = true;

						// Make sure all of the files in the folder are at least 14 days old
						foreach (FileSystemInfo oFileOrFolder in diFolder.GetFileSystemInfos())
						{
							if (DateTime.UtcNow.Subtract(oFileOrFolder.LastWriteTimeUtc).TotalDays <= 14)
							{
								// File was modified within the last 2 weeks; do not delete this folder
								bSafeToDelete = false;
								break;
							}

						}

						if (bSafeToDelete)
						{
							try
							{
								msg = "LCMethods folder: " + diFolder.FullName;
								diFolder.Delete(true);

								msg = "Deleted old " + msg;
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
							}
							catch (Exception ex)
							{
								msg = "Exception deleting old " + msg;
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
							}

						}
					}
				}
			}
			catch (Exception ex)
			{
				msg = "Exception looking for old LC Method folders";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, msg, ex);
			}
		}

		/// <summary>
		/// Return the current quarter for a given date (based on the month)
		/// </summary>
		/// <param name="dtDate"></param>
		/// <returns></returns>
		private int GetQuarter(DateTime dtDate)
		{
			switch (dtDate.Month)
			{
				case 1:
				case 2:
				case 3:
					return 1;
				case 4:
				case 5:
				case 6:
					return 2;
				case 7:
				case 8:
				case 9:
					return 3;
				default:
					return 4;
			}
		}

		protected void HandleCopyException(ref clsToolReturnData retData, Exception ex)
		{
			if (ex.Message.Contains("An unexpected network error occurred") ||
				ex.Message.Contains("Multiple connections") ||
				ex.Message.Contains("specified network name is no longer available"))
			{
				// Need to completely exit the capture task manager
				m_NeedToAbortProcessing = true;
				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_NEED_TO_ABORT_PROCESSING;
				retData.EvalCode = EnumEvalCode.EVAL_CODE_NETWORK_ERROR_RETRY_CAPTURE;
			}
			else if (ex.Message.Contains("unknown user name or bad password"))
			{
				// This error randomly occurs; no need to log a full stack trace
				retData.CloseoutMsg = "Logon failure: unknown user name or bad password";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, retData.CloseoutMsg);

				// Set the EvalCode to 3 so that capture can be retried
				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
				retData.EvalCode = EnumEvalCode.EVAL_CODE_NETWORK_ERROR_RETRY_CAPTURE;
			}
			else
			{
				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
			}
		}

		/// <summary>
		/// Store m_ErrorMessage in retData.CloseoutMsg if an error exists yet retData.CloseoutMsg is empty
		/// </summary>
		/// <param name="retData"></param>
		protected void PossiblyStoreErrorMessage(ref clsToolReturnData retData)
		{

			if (!string.IsNullOrWhiteSpace(m_ErrorMessage) && string.IsNullOrWhiteSpace(retData.CloseoutMsg))
				retData.CloseoutMsg = m_ErrorMessage;

		}

		/// <summary>
		/// Verifies specified folder path exists
		/// </summary>
		/// <param name="InpPath">Folder path to test</param>
		/// <returns>TRUE if folder was found</returns>
		private bool ValidateFolderPath(string InpPath)
		{
			bool retVal;

			if (Directory.Exists(InpPath))
			{
				retVal = true;
			}
			else
			{
				retVal = false;
			}
			return retVal;
		}	// End sub

		#endregion

		#region "Event handlers"


		private void OnCopyingFile(string filename)
		{
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Copying file " + filename);
		}

		private void OnResumingFileCopy(string filename)
		{
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Resuming copy of file " + filename);
		}

		private void OnFileCopyProgress(string filename, float percentComplete)
		{

			if (DateTime.Now.Subtract(m_LastProgressUpdate).TotalSeconds >= 20 || percentComplete >= 100 && filename == m_LastProgressFileName)
			{
				if ((m_LastProgressFileName == filename) && (Math.Abs(m_LastProgressPercent - percentComplete) < Single.Epsilon))
					// Don't re-display this progress
					return;

				m_LastProgressUpdate = DateTime.Now;
				m_LastProgressFileName = filename;
				m_LastProgressPercent = percentComplete;
				string msg = "  copying " + Path.GetFileName(filename) + ": " + percentComplete.ToString("0.0") + "% complete";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
			}


		}	// End sub

		/// <summary>
		/// Make sure that we matched a file for instruments that save data as a file, or a folder for instruments that save data to a folder
		/// </summary>
		/// <param name="dataset"></param>
		/// <param name="sourceFolderPath"></param>
		/// <param name="sourceType"></param>
		/// <param name="instrumentClass"></param>
		/// <param name="datasetInfo"></param>
		/// <param name="retData"></param>
		/// <returns>True if the file or folder is appropriate for the instrument class</returns>
		private bool ValidateWithInstrumentClass(
			string dataset, 
			string sourceFolderPath,
			RawDSTypes sourceType, 
			clsInstrumentClassInfo.eInstrumentClass instrumentClass, 
			clsDatasetInfo datasetInfo,
			ref clsToolReturnData retData)
		{
			string entityDescription;

			retData.CloseoutMsg = string.Empty;

			switch (sourceType)
			{
				case RawDSTypes.File:
					entityDescription = "a file";
					break;
				case RawDSTypes.FolderNoExt:
					entityDescription = "a folder";
					break;
				case RawDSTypes.FolderExt:
					entityDescription = "a folder";
					break;
				case RawDSTypes.BrukerImaging:
				case RawDSTypes.BrukerSpot:
					entityDescription = "a folder";
					break;
				case RawDSTypes.MultiFile:
					entityDescription = "multiple files";
					break;
				default:
					entityDescription = "an unknown entity";
					break;
			}

			// Make sure we are capturing the correct entity type (file or folder) based on instrumentClass
			// See table T_Instrument_Class for allowed types
			switch (instrumentClass)
			{
				case clsInstrumentClassInfo.eInstrumentClass.Finnigan_Ion_Trap:
				case clsInstrumentClassInfo.eInstrumentClass.LTQ_FT:
				case clsInstrumentClassInfo.eInstrumentClass.Thermo_Exactive:
				case clsInstrumentClassInfo.eInstrumentClass.Triple_Quad:
					if (sourceType != RawDSTypes.File)
					{
						retData.CloseoutMsg = "Dataset name matched " + entityDescription + "; must be a .raw file";						
					}
					break;

				case clsInstrumentClassInfo.eInstrumentClass.BrukerMALDI_Imaging_V2:
					if (sourceType != RawDSTypes.FolderNoExt)
					{
						retData.CloseoutMsg = "Dataset name matched " + entityDescription + "; must be a folder with the dataset name, and inside the folder is a .D folder (and typically some jpg files)";
					}
					break;

				case clsInstrumentClassInfo.eInstrumentClass.Bruker_Amazon_Ion_Trap:
				case clsInstrumentClassInfo.eInstrumentClass.BrukerFT_BAF:
				case clsInstrumentClassInfo.eInstrumentClass.BrukerTOF_BAF:
				case clsInstrumentClassInfo.eInstrumentClass.Agilent_Ion_Trap:
				case clsInstrumentClassInfo.eInstrumentClass.Agilent_TOF_V2:
				case clsInstrumentClassInfo.eInstrumentClass.PrepHPLC:

					if (sourceType != RawDSTypes.FolderExt)
					{
						retData.CloseoutMsg = "Dataset name matched " + entityDescription + "; must be a .d folder";						
					}
					break;

				case clsInstrumentClassInfo.eInstrumentClass.BrukerMALDI_Imaging:
				case clsInstrumentClassInfo.eInstrumentClass.BrukerMALDI_Spot:

					if (sourceType != RawDSTypes.FolderNoExt)
					{
						retData.CloseoutMsg = "Dataset name matched " + entityDescription + "; must be a folder with the dataset name";						
					}
					break;

				case clsInstrumentClassInfo.eInstrumentClass.Sciex_TripleTOF:
					if (sourceType != RawDSTypes.File)
					{
						retData.CloseoutMsg = "Dataset name matched " + entityDescription + "; must be a file";						
					}
					break;

				case clsInstrumentClassInfo.eInstrumentClass.IMS_Agilent_TOF:
					if (sourceType != RawDSTypes.File)
					{

						if (sourceType == RawDSTypes.FolderExt)
						{
							// IMS08_AgQTOF05 collects data as .D folders, which the capture pipeline will then convert to a .uimf file
							// Make sure the matched folder is a .d file
							if (datasetInfo.FileOrFolderName.ToLower().EndsWith(".d"))								
								break;
						}
						if (sourceType == RawDSTypes.FolderNoExt)
						{
							// IMS04_AgTOF05 and similar instruments collect data into a folder named after the dataset
							// The folder contains a .UIMF file plus several related files
							// Make sure the folder contains just one .UIMF file

							var diSourceDir = new DirectoryInfo(Path.Combine(sourceFolderPath, datasetInfo.FileOrFolderName));
							var fiFiles = diSourceDir.GetFiles("*.uimf").ToList();
							if (fiFiles.Count == 1)
								break;

							if (fiFiles.Count > 1)
								retData.CloseoutMsg = "Dataset name matched " + entityDescription + " with multiple .uimf files; there must be only one .uimf file";
							else
								retData.CloseoutMsg = "Dataset name matched " + entityDescription + " but it does not have any .uimf files";

							break;
						}

						retData.CloseoutMsg = "Dataset name matched " + entityDescription + "; must be a .uimf file, .d folder, or folder with a single .uimf file";
					}
					break;
			}

			if (string.IsNullOrEmpty(retData.CloseoutMsg))
			{
				// We are capturing the right item for this dataset's instrument class
				return true;
			}

			string msg = retData.CloseoutMsg + ": " + dataset;
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
			
			return false;
		}

		#endregion

	}

}
