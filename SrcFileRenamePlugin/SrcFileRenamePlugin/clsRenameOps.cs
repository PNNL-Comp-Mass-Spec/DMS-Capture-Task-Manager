
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 11/17/2009
//
// Last modified 11/17/2009
//*********************************************************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using CaptureTaskManager;
using System.IO;
using PRISM.Files;

namespace SrcFileRenamePlugin
{
	class clsRenameOps
	{
		//*********************************************************************************************************
		// Class for performing rename operations
		//**********************************************************************************************************

		#region "Enums"
			protected enum RawDSTypes
			{
				None,
				File,
				FolderNoExt,
				FolderExt
			}
		#endregion

		#region "Class variables"
			protected IMgrParams m_MgrParams;
			protected string m_Msg = "";
			protected bool m_UseBioNet = false;
			protected string m_UserName = "";
			protected string m_Pwd = "";
			protected ShareConnector m_ShareConnector;
			protected bool m_Connected = false;
		#endregion

		#region "Constructors"
			/// <summary>
			/// Constructor
			/// </summary>
			/// <param name="MgrParams">Parameters for manager operation</param>
			/// <param name="UseBioNet">Flag to indicate if source instrument is on Bionet</param>
			public clsRenameOps(IMgrParams mgrParams, bool useBioNet)
			{
   			 m_MgrParams = mgrParams;
			    
   			 //Setup for BioNet use, if applicable
   			 m_UseBioNet = useBioNet;
   			 if (m_UseBioNet) 
				 {
       			  m_UserName = m_MgrParams.GetParam("bionetuser");
       			  m_Pwd = m_MgrParams.GetParam("bionetpwd");
   			 }
			}	// End sub
		#endregion

		#region "Methods"
			/// <summary>
			/// Perform a single rename operation
			/// </summary>
			/// <param name="taskParams">Enum indicating status of task</param>
			/// <returns></returns>
			public EnumCloseOutType DoOperation(ITaskParams taskParams)
			{
				string dataset = taskParams.GetParam("Dataset");
				string sourceVol = taskParams.GetParam("Source_Vol");
				string sourcePath = taskParams.GetParam("Source_Path");
				string rawFName = "";
				RawDSTypes sourceType;
				string pwd = DecodePassword(m_Pwd);
				string msg;
				string tempVol;

				msg = "Started clsRenameeOps.DoOperation()";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

				// Set up paths
				string sourceFolderPath;	// Instrument transfer directory

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

				sourceType = GetRawDSType(sourceFolderPath, dataset, ref rawFName);
				switch (sourceType)
				{
					case RawDSTypes.None:
						// No dataset file or folder found
						msg = "Dataset " + dataset + ": data file not found";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
						return EnumCloseOutType.CLOSEOUT_FAILED;
						break;
					case RawDSTypes.File:
						// Dataset found, and it's a single file. Rename the file
						try
						{
							if (RenameInstFile(Path.Combine(sourceFolderPath,rawFName)))
							{
								msg = "Renamed file " + Path.Combine(sourceFolderPath, rawFName);
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
								if (m_Connected) DisconnectShare(ref m_ShareConnector, ref m_Connected);
								return EnumCloseOutType.CLOSEOUT_SUCCESS;
							}
							else
							{
								msg = "Unable to rename file " + Path.Combine(sourceFolderPath, rawFName);
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
								if (m_Connected) DisconnectShare(ref m_ShareConnector, ref m_Connected);
								return EnumCloseOutType.CLOSEOUT_FAILED;
							}
						}
						catch (Exception ex)
						{
							msg = "Rename exception for dataset " + dataset;
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg, ex);
							if (m_Connected) DisconnectShare(ref m_ShareConnector, ref m_Connected);
							return EnumCloseOutType.CLOSEOUT_FAILED;
						}
						break;
					case RawDSTypes.FolderExt:
						// Dataset found in a folder with an extension on the folder name. Rename the folder
						try
						{
							if (RenameInstFolder(Path.Combine(sourceFolderPath, rawFName)))
							{
								msg = "Renamed folder " + Path.Combine(sourceFolderPath, rawFName);
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
								if (m_Connected) DisconnectShare(ref m_ShareConnector, ref m_Connected);
								return EnumCloseOutType.CLOSEOUT_SUCCESS;
							}
							else
							{
								msg = "Unable to rename folder " + Path.Combine(sourceFolderPath, rawFName);
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
								if (m_Connected) DisconnectShare(ref m_ShareConnector, ref m_Connected);
								return EnumCloseOutType.CLOSEOUT_FAILED;
							}
						}
						catch (Exception ex)
						{
							msg = "Rename exception for dataset " + dataset;
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg, ex);
							return EnumCloseOutType.CLOSEOUT_FAILED;
						}
						finally
						{
							if (m_Connected) DisconnectShare(ref m_ShareConnector, ref m_Connected);
						}
						break;
					case RawDSTypes.FolderNoExt:
						// Dataset found; it's a folder with no extension on the name. Rename the folder
						try
						{
							if (RenameInstFolder(Path.Combine(sourceFolderPath, rawFName)))
							{
								msg = "Renamed folder " + Path.Combine(sourceFolderPath, rawFName);
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
								if (m_Connected) DisconnectShare(ref m_ShareConnector, ref m_Connected);
								return EnumCloseOutType.CLOSEOUT_SUCCESS;
							}
							else
							{
								msg = "Unable to rename folder " + Path.Combine(sourceFolderPath, rawFName);
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
								if (m_Connected) DisconnectShare(ref m_ShareConnector, ref m_Connected);
								return EnumCloseOutType.CLOSEOUT_FAILED;
							}
						}
						catch (Exception ex)
						{
							msg = "Rename exception for dataset " + dataset;
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg, ex);
							return EnumCloseOutType.CLOSEOUT_FAILED;
						}
						finally
						{
							if (m_Connected) DisconnectShare(ref m_ShareConnector, ref m_Connected);
						}
						break;
					default:
						msg = "Invalid dataset type found: " + sourceType.ToString();
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
						if (m_Connected) DisconnectShare(ref m_ShareConnector, ref m_Connected);
						return EnumCloseOutType.CLOSEOUT_FAILED;
						break;
				}	// End switch
			}	// End sub

			/// <summary>
			/// Prefixes specified folder name with "x_"
			/// </summary>
			/// <param name="DSPath">Full path specifying folder to be renamed</param>
			/// <returns>TRUE for success, FALSE for failure</returns>
			private bool RenameInstFolder(string InstFolderPath)
			{
				//Rename dataset folder on instrument
				try
				{
					DirectoryInfo di = new DirectoryInfo(InstFolderPath);
					string n = Path.Combine(di.Parent.FullName, "x_" + di.Name);
					di.MoveTo(n);
					m_Msg = "Renamed directory " + InstFolderPath;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.INFO, m_Msg);
					return true;
				}
				catch (Exception ex)
				{
					m_Msg = "Error renaming directory " + InstFolderPath;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg, ex);
					return false;
				}
			}	// End sub

			/// <summary>
			/// Prefixes specified file name with "x_"
			/// </summary>
			/// <param name="DSPath">Full path specifying file to be renamed</param>
			/// <returns>TRUE for success, FALSE for failure</returns>
			private bool RenameInstFile(string InstFilePath)
			{
				//Rename dataset folder on instrument
				try
				{
					FileInfo fi = new FileInfo(InstFilePath);
					string n = Path.Combine(fi.DirectoryName, "x_" + fi.Name);
					fi.MoveTo(n);
					m_Msg = "Renamed file " + InstFilePath;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.INFO, m_Msg);
					return true;
				}
				catch (Exception ex)
				{
					m_Msg = "Error renaming file " + InstFilePath;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg, ex);
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
			/// Determines if raw dataset exists as a file or folder
			/// </summary>
			/// <param name="InstFolder">Full path to instrument transfer folder</param>
			/// <param name="DSName">Dataset name</param>
			/// <param name="MyName">Return value for full name of file or folder found, if any</param>
			/// <returns>Enum specifying type of file/folder found, if any</returns>
			private RawDSTypes GetRawDSType(string InstFolder, string DSName, ref string MyName)
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
							return RawDSTypes.FolderNoExt;
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
				m_Msg = "Bionet disconnected";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_Msg);
				ConnState = false;
			}	// End sub
		#endregion
	}	// End class
}	// End namespace
