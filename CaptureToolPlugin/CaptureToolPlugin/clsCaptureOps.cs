
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 09/25/2009
//
// Last modified 09/25/2009
//*********************************************************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using CaptureTaskManager;
using PRISM.Files;
using System.IO;

namespace CaptureToolPlugin
{
	public class clsCaptureOps
	{
		//*********************************************************************************************************
		// Base class for performing capture operations
		//**********************************************************************************************************

		#region "Enums"
			protected enum RawDSTypes
			{
				None,
				File,
				FolderNoExt,
				FolderExt,
				BrukerImaging,
				BrukerSpot
			}

			protected enum DatasetFolderState
			{
				Empty,
				NotEmpty,
				Error
			}
		#endregion

		#region "Class variables"
			protected IMgrParams m_MgrParams;
			protected int m_SleepInterval = 30;
			protected string m_Msg = "";
			protected bool m_ClientServer;	// True = client
			protected bool m_UseBioNet = false;
			protected string m_UserName = "";
			protected string m_Pwd = "";
			protected ShareConnector m_ShareConnector;
			protected bool m_Connected = false;
			string msg = "";
		#endregion

		#region "Constructors"
			/// <summary>
			/// Constructor
			/// </summary>
			/// <param name="MgrParams">Parameters for manager operation</param>
			/// <param name="UseBioNet">Flag to indicate if source instrument is on Bionet</param>
			public clsCaptureOps(IMgrParams mgrParams, bool useBioNet)
			{
   			 m_MgrParams = mgrParams;
			    
   			 //Get client/server perspective
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
   			 }
			    
   			 //Sleep interval for "is dataset complete" testing
   			 m_SleepInterval = int.Parse(m_MgrParams.GetParam("sleepinterval"));
			}	// End sub
		#endregion

		#region "Methods"
			/// <summary>
			/// Creates specified folder
			/// </summary>
			/// <param name="InpPath">Fully qualified path for folder to be created</param>
			/// <returns>TRUE for success, FALSE for failure</returns>
			private bool MakeFolderPath(string inpPath)
			{
				//Create specified directory
				try
				{
					Directory.CreateDirectory(inpPath);
					return true;
				}
				catch (Exception ex)
				{
					msg = "Exception creating directory " + inpPath;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg, ex);
					return false;
				}
			}	// End sub

			/// <summary>
			/// Checks to determine if specified folder is empty
			/// </summary>
			/// <param name="DSFolder">Full path specifying folder to be checked</param>
			/// <returns>0 if folder is empty, count of files if not empty, -1 if error occurred</returns>
			private DatasetFolderState IsDSFolderEmpty(string dsFolder)
			{
				//Returns count of files or folders if folder is not empty
				//Returns 0 if folder is empty
				//returns -1 on error

				string[] Folderstuff = null;

				try
				{
					//Check for files
					Folderstuff = Directory.GetFiles(dsFolder);
					if (Folderstuff.GetLength(0) > 0) return DatasetFolderState.NotEmpty;

					//Check for folders
					Folderstuff = Directory.GetDirectories(dsFolder);
					if (Folderstuff.GetLength(0) > 0) return DatasetFolderState.NotEmpty;
				}
				catch (Exception ex)
				{
					//Something really bad happened
					msg = "Error checking for empty dataset folder " + dsFolder;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg, ex);
					return DatasetFolderState.Error;
				}

				//If we got to here, then the directory is empty

				return DatasetFolderState.Empty;
			}	// End sub

			/// <summary>
			/// Performs action specified by dsfolderexistsaction mgr param if a dataset folder already exists
			/// </summary>
			/// <param name="DSFolder">Full path to dataset folder</param>
			/// <returns>TRUE for success, FALSE for failure</returns>
			private bool PerformDSExistsActions(string dsFolder)
			{
				bool switchResult = false;

				switch (IsDSFolderEmpty(dsFolder))
				{
					case DatasetFolderState.Empty:
						//Directory is empty, attempt to delete it
						try
						{
							Directory.Delete(dsFolder);
							switchResult = true;
						}
						catch (Exception ex)
						{
							msg = "Dataset folder '" + dsFolder + "' already exists and cannot be deleted";
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg, ex);
							switchResult = false;
						}
						break;
					case   DatasetFolderState.Error:
						//There was an error attempting to determine the dataset directory contents
						//(Error reporting was handled by previous call to IsDSFolderEmpty)
						switchResult = false;
						break;
					case  DatasetFolderState.NotEmpty:
						string DSAction = m_MgrParams.GetParam("dsfolderexistsaction");
						switch (DSAction.ToLower())
						{
							case "delete":
								//Attempt to delete dataset folder
								try
								{
									Directory.Delete(dsFolder, true);
									switchResult = true;
								}
								catch (Exception ex)
								{
									msg = "Dataset folder '" + dsFolder + "' already exists and cannot be deleted";
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
								msg = "Dataset folder '" + dsFolder + "' already exists";
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
								switchResult = false;
								break;
							default:
								//An invalid value for dsfolderexistsaction was specified
								msg = "Dataset folder '" + dsFolder + "' already exists. Invalid action " + DSAction + " specified";
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
			}	// End sub

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
					DirectoryInfo di = new DirectoryInfo(DSPath);
					string n = Path.Combine(di.Parent.FullName, "x_" + di.Name);
					di.MoveTo(n);
					msg = "Renamed directory " + DSPath;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.INFO, msg);
					return true;
				}
				catch (Exception ex)
				{
					msg = "Error renaming directory " + DSPath;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
					return false;
				}
			}	// End sub

			/// <summary>
			/// Decrypts password received from ini file
			/// </summary>
			/// <param name="EnPwd">Encoded password</param>
			/// <returns>Clear text password</returns>
			private string DecodePassword(string enPwd)
			{
				// Decrypts password received from ini file
				// Password was created by alternately subtracting or adding 1 to the ASCII value of each character

				// Convert the password string to a character array
				char[] pwdChars = enPwd.ToCharArray();
				byte[] pwdBytes = new byte[pwdChars.Length];
				char[] pwdCharsAdj = new char[pwdChars.Length];

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
			/// <param name="FolderName">Full path specifying folder to check</param>
			/// <param name="SleepInt">Interval for checking (seconds)</param>
			/// <returns>TRUE if folder size hasn't changed during SleepInt; FALSE otherwise</returns>
			private bool VerifyConstantFolderSize(string FolderName, int SleepInt)
			{

				//Determines if the size of a folder changes over specified time interval
				long InitialFolderSize = 0;
				long FinalFolderSize = 0;

				//Verify maximum sleep interval
				if (((long)SleepInt * 1000) > int.MaxValue)
				{
					SleepInt = (int)(int.MaxValue / 1000);
				}

				//Get the initial size of the folder
				InitialFolderSize = clsFileTools.GetDirectorySize(FolderName);

				//Wait for specified sleep interval
				System.Threading.Thread.Sleep(SleepInt * 1000);
				//Delay for specified interval

				//Get the final size of the folder and compare
				FinalFolderSize = clsFileTools.GetDirectorySize(FolderName);
				if (FinalFolderSize == InitialFolderSize)
				{
					return true;
				}
				else
				{
					return false;

				}
			}	// End sub

			/// <summary>
			/// Checks to see if file size is changing -- possible sign acquisition hasn't finished
			/// </summary>
			/// <param name="FileName">Full path specifying file to check</param>
			/// <param name="SleepInt">Interval for checking (seconds)</param>
			/// <returns>TRUE if file size hasn't changed during SleepInt; FALSE otherwise</returns>
			private bool VerifyConstantFileSize(string FileName, int SleepInt)
			{
				//Determines if the size of a file changes over specified time interval
				FileInfo Fi = default(FileInfo);
				long InitialFileSize = 0;
				long FinalFileSize = 0;

				//Verify maximum sleep interval
				if (((long)SleepInt * 1000) > int.MaxValue)
				{
					SleepInt = (int)(int.MaxValue / 1000);
				}

				//Get the initial size of the folder
				Fi = new FileInfo(FileName);
				InitialFileSize = Fi.Length;

				//Wait for specified sleep interval
				System.Threading.Thread.Sleep(SleepInt * 1000);
				//Delay for specified interval

				//Get the final size of the file and compare
				Fi = new FileInfo(FileName);
				FinalFileSize = Fi.Length;
				if (FinalFileSize == InitialFileSize)
				{
					return true;
				}
				else
				{
					return false;
				}
			}	// End sub

			/// <summary>
			/// Determines if raw dataset exists as a file or folder
			/// </summary>
			/// <param name="InstFolder">Full path to instrument transfer folder</param>
			/// <param name="DSName">Dataset name</param>
			/// <param name="MyName">Return value for full name of file or folder found, if any</param>
			/// <param name="instClass">Instrument class for dataet to be located</param>
			/// <returns>Enum specifying type of file/folder found, if any</returns>
			private RawDSTypes GetRawDSType(string InstFolder, string DSName, ref string MyName, string instClass)
			{
				//Determines if raw dataset exists as a single file, folder with same name as dataset, or 
				//	folder with dataset name + extension. Returns enum specifying what was found and MyName
				// containing full name of file or folder

				string[] MyInfo = null;

				//Verify instrument transfer folder exists
				if (!Directory.Exists(InstFolder))
				{
					MyName = "";
					return RawDSTypes.None;
				}

				//Check for a file with specified name
				MyInfo = Directory.GetFiles(InstFolder);
				foreach (string TestFile in MyInfo)
				{
					if (Path.GetFileNameWithoutExtension(TestFile).ToLower() == DSName.ToLower())
					{
						MyName = Path.GetFileName(TestFile);
						return RawDSTypes.File;
					}
				}

				//Check for a folder with specified name
				MyInfo = Directory.GetDirectories(InstFolder);
				foreach (string TestFolder in MyInfo)
				{
					//Using Path.GetFileNameWithoutExtension on folders is cheezy, but it works. I did this
					//	because the Path class methods that deal with directories ignore the possibilty there
					//	might be an extension. Apparently when sending in a string, Path can't tell a file from
					//	a directory
					if (Path.GetFileNameWithoutExtension(TestFolder).ToLower() == DSName.ToLower())
					{
						if (string.IsNullOrEmpty(Path.GetExtension(TestFolder)))
						{
							//Found a directory that has no extension
							MyName = Path.GetFileName(TestFolder);

							//Check the instrument class to determine the appropriate return type
							switch (instClass)
							{
								case "BrukerMALDI_Imaging":
									return RawDSTypes.BrukerImaging;
//									break;
								case "BrukerMALDI_Spot":
									return RawDSTypes.BrukerSpot;
//									break;
								default:
									return RawDSTypes.FolderNoExt;
//									break;
							}
						}
						else
						{
							//Directory name has an extension
							MyName = Path.GetFileName(TestFolder);
							return RawDSTypes.FolderExt;
						}
					}
				}

				//If we got to here, then the raw dataset wasn't found, so there was a problem
				MyName = "";

				return RawDSTypes.None;
			}	// End sub

			/// <summary>
			/// Disconnects a Bionet shared drive
			/// </summary>
			/// <param name="MyConn">Connection object for shared drive</param>
			/// <param name="ConnState">Return value specifying connection has been closed</param>
			private void DisconnectShare(ref ShareConnector MyConn, ref bool ConnState)
			{
				//Disconnects a shared drive
				MyConn.Disconnect();
				msg = "Bionet disconnected";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
				ConnState = false;
			}	// End sub

			/// <summary>
			/// Perform a single capture operation
			/// </summary>
			/// <param name="taskParams">Enum indicating status of task</param>
			/// <returns></returns>
			public EnumCloseOutType DoOperation(ITaskParams taskParams)
			{
				string dataset = taskParams.GetParam("Dataset");
				string sourceVol = taskParams.GetParam("Source_Vol");
				string sourcePath = taskParams.GetParam("Source_Path");
				string storageVol = taskParams.GetParam("Storage_Vol");
				string storagePath = taskParams.GetParam("Storage_Path");
				string storageVolExternal = taskParams.GetParam("Storage_Vol_External");
				string rawFName = "";
				RawDSTypes sourceType;
				string pwd = DecodePassword(m_Pwd);
				string msg;
				string tempVol;

				msg = "Started clsCaptureOps.DoOperation()";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

				// Setup destination based on client/server switch
				if (m_ClientServer)
				{
					tempVol = storageVolExternal;
				}
				else
				{
					tempVol = storageVol;
				}

				// Set up paths
				string sourceFolderPath;	// Instrument transfer directory
				string storageFolderPath = Path.Combine(tempVol, storagePath);	// Directory on storage server where dataset folder goes
				string datasetFolderPath;

				// If Storage_Folder_Name <> "", then use it in target folder path. Otherwise use dataset name
				if (taskParams.GetParam("Storage_Folder_Name") != "")
				{
					datasetFolderPath = Path.Combine(storageFolderPath, taskParams.GetParam("Storage_Folder_Name")); // HPLC run folder storage path
				}
				else
				{
					datasetFolderPath = Path.Combine(storageFolderPath, dataset);	// Dataset folder complete path
				}
				
				// Verify storage folder on storage server exists
				if (!ValidateFolderPath(storageFolderPath))
				{
					msg = "Storage folder '" + storageFolderPath + "' does not exist";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
					return EnumCloseOutType.CLOSEOUT_FAILED;
				}

				// Verify dataset folder path doesn't already exist
				if (ValidateFolderPath(datasetFolderPath))
				{
					// Folder exists, so take action specified in configuration
					if (!PerformDSExistsActions(datasetFolderPath)) return EnumCloseOutType.CLOSEOUT_FAILED;
				}

				// Determine if source dataset exists, and if it is a file or a folder
				sourceFolderPath = Path.Combine(sourceVol, sourcePath);
				// Connect to Bionet if necessary
				if (m_UseBioNet)
				{
					msg = "Bionet connection required";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

					m_ShareConnector = new ShareConnector(m_UserName, pwd);
					m_ShareConnector.Share = sourceFolderPath;
					if (m_ShareConnector.Connect())
					{
						msg = "Connected to Bionet";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
						m_Connected = true;
					}
					else
					{
						msg = "Error " + m_ShareConnector.ErrorMessage + " connecting to " + sourceFolderPath;
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
						return EnumCloseOutType.CLOSEOUT_FAILED;
					}
				}
				else
				{
					msg = "Bionet connection not required";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
				}

				//If Source_Folder_Name is non-blank, use it. Otherwise use dataset name
				if (taskParams.GetParam("Source_Folder_Name") != "")
				{
					sourceType = GetRawDSType(sourceFolderPath, taskParams.GetParam("Source_Folder_Name"), ref rawFName, taskParams.GetParam("Instrument_Class"));
				}
				else
				{
					sourceType = GetRawDSType(sourceFolderPath, dataset, ref rawFName, taskParams.GetParam("Instrument_Class"));
				}

				// Perform copy based on source type
				switch (sourceType)
				{
					case RawDSTypes.None:
						// No dataset file or folder found
						msg = "Dataset " + dataset + ": data file not found";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb,clsLogTools.LogLevels.ERROR,msg);
						return EnumCloseOutType.CLOSEOUT_FAILED;
//						break;
					case RawDSTypes.File:
						// Dataset found, and it's a single file
						// First, verify the file size is constant (indicates acquisition is actually finished)
						if (!VerifyConstantFileSize(Path.Combine(sourceFolderPath,rawFName),m_SleepInterval))
						{
							msg = "Dataset '" + dataset + "' not ready";
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb,clsLogTools.LogLevels.WARN,msg);
							if (m_Connected) DisconnectShare(ref m_ShareConnector, ref m_Connected);
							return EnumCloseOutType.CLOSEOUT_NOT_READY;
						}
						// Copy the file to the dataset folder
						try
						{
							// Make the dataset folder
							MakeFolderPath(datasetFolderPath);
							// Copy the raw spectra file
							File.Copy(Path.Combine(sourceFolderPath, rawFName), Path.Combine(datasetFolderPath, rawFName));
							msg = "Copied file " + Path.Combine(sourceFolderPath, rawFName) + " to " + Path.Combine(datasetFolderPath, rawFName);
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile,clsLogTools.LogLevels.INFO,msg);
							if (m_Connected) DisconnectShare(ref m_ShareConnector, ref m_Connected);
							return EnumCloseOutType.CLOSEOUT_SUCCESS;
						}
						catch (Exception ex) 
						{
							msg = "Copy exception for dataset " + dataset;
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb,clsLogTools.LogLevels.ERROR,msg,ex);
							if (m_Connected) DisconnectShare(ref m_ShareConnector, ref m_Connected);
							return EnumCloseOutType.CLOSEOUT_FAILED;
						}
//						break;
					case RawDSTypes.FolderExt:
						// Dataset found in a folder with an extension on the folder name
						// First, verify the folder size is constant (indicates acquisition is actually finished)
						if (!VerifyConstantFolderSize(Path.Combine(sourceFolderPath,rawFName),m_SleepInterval))
						{
							msg = "Dataset '" + dataset + "' not ready";
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb,clsLogTools.LogLevels.WARN,msg);
							if (m_Connected) DisconnectShare(ref m_ShareConnector, ref m_Connected);
							return EnumCloseOutType.CLOSEOUT_NOT_READY;
						}

						// Copy the dateset folder to the storage server
						try
						{
							// Make a dateaet folder
							MakeFolderPath(datasetFolderPath);
							// Copy the dataset folder
							clsFileTools.CopyDirectory(Path.Combine(sourceFolderPath,rawFName),Path.Combine(datasetFolderPath,rawFName));
							msg = "Copied folder " + Path.Combine(sourceFolderPath, rawFName) + " to " + Path.Combine(datasetFolderPath, rawFName);
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile,clsLogTools.LogLevels.INFO,msg);
							return EnumCloseOutType.CLOSEOUT_SUCCESS;
						}
						catch (Exception ex)
						{
							msg = "Copy exception for dataset " + dataset;
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb,clsLogTools.LogLevels.ERROR,msg,ex);
							return EnumCloseOutType.CLOSEOUT_FAILED;
						}
						finally
						{
							if (m_Connected) DisconnectShare(ref m_ShareConnector, ref m_Connected);
						}
//						break;
					case RawDSTypes.FolderNoExt:
						// Dataset found; it's a folder with no extension on the name
						// First, verify the folder size is constant (indicates acquisition is actually finished)
						if (!VerifyConstantFolderSize(Path.Combine(sourceFolderPath, rawFName), m_SleepInterval))
						{
							msg = "Dataset '" + dataset + "' not ready";
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, msg);
							if (m_Connected) DisconnectShare(ref m_ShareConnector, ref m_Connected);
							return EnumCloseOutType.CLOSEOUT_NOT_READY;
						}

						// Verify the folder doesn't contain a group of ".D" folders
						string[] folderList = Directory.GetDirectories(Path.Combine(sourceFolderPath, rawFName),"*.D");
						if (folderList.Length > 0)
						{
							msg = "Multiple scan folders found in dataset folder " + Path.Combine(sourceFolderPath, rawFName);
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
							return EnumCloseOutType.CLOSEOUT_FAILED;
						}

						// Copy the dataset folder to the storage server
						try
						{
							clsFileTools.CopyDirectory(Path.Combine(sourceFolderPath, rawFName), datasetFolderPath);
							msg = "Copied folder " + Path.Combine(sourceFolderPath, rawFName) + " to " + Path.Combine(datasetFolderPath, rawFName);
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.INFO, msg);
							return EnumCloseOutType.CLOSEOUT_SUCCESS;
						}
						catch (Exception ex)
						{
							msg = "Exception copying dataset folder " + Path.Combine(sourceFolderPath, rawFName);
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg, ex);
							return EnumCloseOutType.CLOSEOUT_FAILED;
						}
						finally
						{
							if (m_Connected) DisconnectShare(ref m_ShareConnector, ref m_Connected);
						}
//						break;
					case RawDSTypes.BrukerImaging:
						// Dataset found; it's a Bruker imaging folder
						// First, verify the folder size is constant (indicates acquisition is actually finished)
						if (!VerifyConstantFolderSize(Path.Combine(sourceFolderPath, rawFName), m_SleepInterval))
						{
							msg = "Dataset '" + dataset + "' not ready";
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, msg);
							if (m_Connected) DisconnectShare(ref m_ShareConnector, ref m_Connected);
							return EnumCloseOutType.CLOSEOUT_NOT_READY;
						}

						// Check to see if the folders have been zipped
						string[] zipFileList = Directory.GetFiles(Path.Combine(sourceFolderPath, rawFName), "*.zip");
						if (zipFileList.Length < 1)
						{
							// Data files haven't been zipped, so throw error
							msg = "No zip files found in dataset folder " + Path.Combine(sourceFolderPath, rawFName);
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
							return EnumCloseOutType.CLOSEOUT_FAILED;
						}

						// Make a dateaet folder
						MakeFolderPath(datasetFolderPath);

						// Copy only the files in the dataset folder to the storage server. Do not copy folders
						try
						{
							string[] fileList = Directory.GetFiles(Path.Combine(sourceFolderPath, rawFName));
							
							foreach (string fileToCopy in fileList)
							{
								FileInfo fi = new FileInfo(fileToCopy);
								fi.CopyTo(Path.Combine(datasetFolderPath,fi.Name));
							}
							msg = "Copied files in folder " + Path.Combine(sourceFolderPath, rawFName) + " to " + Path.Combine(datasetFolderPath, rawFName);
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.INFO, msg);
							return EnumCloseOutType.CLOSEOUT_SUCCESS;
						}
						catch (Exception ex)
						{
							msg = "Exception copying files in dataset folder " + Path.Combine(sourceFolderPath, rawFName);
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg, ex);
							return EnumCloseOutType.CLOSEOUT_FAILED;
						}
						finally
						{
							if (m_Connected) DisconnectShare(ref m_ShareConnector, ref m_Connected);
						}
//						break;
					case RawDSTypes.BrukerSpot:
						// Dataset found; it's a Bruker_Spot instrument type
						// First, verify the folder size is constant (indicates acquisition is actually finished)
						if (!VerifyConstantFolderSize(Path.Combine(sourceFolderPath, rawFName), m_SleepInterval))
						{
							msg = "Dataset '" + dataset + "' not ready";
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, msg);
							if (m_Connected) DisconnectShare(ref m_ShareConnector, ref m_Connected);
							return EnumCloseOutType.CLOSEOUT_NOT_READY;
						}

						// Verify the dataset folder contains just one data folder, unzipped
						string[] zipFiles = Directory.GetFiles(Path.Combine(sourceFolderPath, rawFName), "*.zip");
						string[] dataFolders = Directory.GetDirectories(Path.Combine(sourceFolderPath, rawFName));

						if (zipFiles.Length > 0)
						{
							msg = "Zip files found in dataset folder " + Path.Combine(sourceFolderPath, rawFName);
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
							return EnumCloseOutType.CLOSEOUT_FAILED;
						}
						else if (dataFolders.Length != 1)
						{
							msg = "Multiple data files found in dataset folder " + Path.Combine(sourceFolderPath, rawFName);
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
							return EnumCloseOutType.CLOSEOUT_FAILED;
						}

						// Copy the dataset folder to the storage server
						try
						{
							clsFileTools.CopyDirectory(Path.Combine(sourceFolderPath, rawFName), datasetFolderPath);
							msg = "Copied folder " + Path.Combine(sourceFolderPath, rawFName) + " to " + Path.Combine(datasetFolderPath, rawFName);
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.INFO, msg);
							return EnumCloseOutType.CLOSEOUT_SUCCESS;
						}
						catch (Exception ex)
						{
							msg = "Exception copying dataset folder " + Path.Combine(sourceFolderPath, rawFName);
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg, ex);
							return EnumCloseOutType.CLOSEOUT_FAILED;
						}
						finally
						{
							if (m_Connected) DisconnectShare(ref m_ShareConnector, ref m_Connected);
						}
//						break;
					default:
						msg = "Invalid dataset type found: " + sourceType.ToString();
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
						if (m_Connected) DisconnectShare(ref m_ShareConnector, ref m_Connected);
						return EnumCloseOutType.CLOSEOUT_FAILED;
//						break;
				}	// End switch
			}	// End sub

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
	}	// End class
}	// End namespace
