
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 10/06/2009
//
// Last modified 10/06/2009
//						05/04/2010 (DAC) - Added Matt Monroe MSFileScanner for quality checks
//*********************************************************************************************************
using System;
using CaptureTaskManager;
using MSFileInfoScanner;
using System.IO;

namespace DatasetQualityPlugin
{
	public class clsPluginMain : clsToolRunnerBase
	{
		//*********************************************************************************************************
		// Main class for plugin
		//**********************************************************************************************************

		#region "Constants"
			const string MS_FILE_SCANNER_DS_INFO_SP = "CacheDatasetInfoXML";
		#endregion

		#region "Module variables"
			clsMSFileInfoScanner m_MsFileScanner;
			string m_Msg;
			bool m_ErrOccurred = false;
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
			/// Runs the dataset quality step tool
			/// </summary>
			/// <returns>Enum indicating success or failure</returns>
			public override clsToolReturnData RunTool()
			{
				string msg;

				msg = "Starting DatasetQualityPlugin.clsPluginMain.RunTool()";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

				// Perform base class operations, if any
				clsToolReturnData retData = base.RunTool();
				if (retData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED) return retData;

				string dataset = m_TaskParams.GetParam("Dataset");

				// Store the version info in the database
				if (!StoreToolVersionInfo())
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false");
					retData.CloseoutMsg = "Error determining tool version info";
					retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
					return retData;
				}

				msg = "Performing quality checks for dataset '" + dataset + "'";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.INFO, msg);

				//msg = "Dataset Quality tool is not presently active";
				//clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, msg);
				//retData.EvalCode = EnumEvalCode.EVAL_CODE_NOT_EVALUATED;
				//retData.EvalMsg = msg;
				//retData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;

				retData = RunMsFileInfoScanner();

				msg = "Completed clsPluginMain.RunTool()";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

				return retData;
			}	// End sub

			/// <summary>
			/// Initializes the dataset quality tool
			/// </summary>
			/// <param name="mgrParams">Parameters for manager operation</param>
			/// <param name="taskParams">Parameters for the assigned task</param>
			/// <param name="statusTools">Tools for status reporting</param>
			public override void Setup(IMgrParams mgrParams, ITaskParams taskParams, IStatusFile statusTools)
			{
				string msg = "Starting clsPluginMain.Setup()";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

				base.Setup(mgrParams, taskParams, statusTools);

				// Initialize MSFileScanner class
				m_MsFileScanner = new clsMSFileInfoScanner();
				m_MsFileScanner.ErrorEvent += new clsMSFileInfoScanner.ErrorEventEventHandler(m_MsFileScanner_ErrorEvent);
				m_MsFileScanner.MessageEvent += new clsMSFileInfoScanner.MessageEventEventHandler(m_MsFileScanner_MessageEvent);

				msg = "Completed clsPluginMain.Setup()";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
			}	// End sub

			/// <summary>
			/// Runs the MS_File_Info_Scanner tool
			/// </summary>
			/// <returns></returns>
			private clsToolReturnData RunMsFileInfoScanner()
			{
				clsToolReturnData result = new clsToolReturnData();
				string sourceFolder;

				// Always use client perspective for the source folder (allows MSFileInfoScanner to run from any CTM)
				sourceFolder = m_TaskParams.GetParam("Storage_Vol_External");

				// Set up the rest of the paths
				sourceFolder = Path.Combine(sourceFolder,m_TaskParams.GetParam("Storage_Path"));
				sourceFolder = Path.Combine(sourceFolder, m_TaskParams.GetParam("Folder"));
				string outputFolder = Path.Combine(sourceFolder, "QC");
				bool bSkipPlots = false;

				// Set up the params for the MS file scanner
				m_MsFileScanner.DSInfoDBPostingEnabled = false;
				m_MsFileScanner.SaveTICAndBPIPlots = bool.Parse(m_TaskParams.GetParam("SaveTICAndBPIPlots"));
				m_MsFileScanner.SaveLCMS2DPlots = bool.Parse(m_TaskParams.GetParam("SaveLCMS2DPlots"));
				m_MsFileScanner.ComputeOverallQualityScores = bool.Parse(m_TaskParams.GetParam("ComputeOverallQualityScores"));
				m_MsFileScanner.CreateDatasetInfoFile = bool.Parse(m_TaskParams.GetParam("CreateDatasetInfoFile"));
				m_MsFileScanner.LCMS2DPlotMZResolution = float.Parse(m_TaskParams.GetParam("LCMS2DPlotMZResolution"));
				m_MsFileScanner.LCMS2DPlotMaxPointsToPlot = int.Parse(m_TaskParams.GetParam("LCMS2DPlotMaxPointsToPlot"));
				m_MsFileScanner.LCMS2DPlotMinPointsPerSpectrum = int.Parse(m_TaskParams.GetParam("LCMS2DPlotMinPointsPerSpectrum"));
				m_MsFileScanner.LCMS2DPlotMinIntensity = float.Parse(m_TaskParams.GetParam("LCMS2DPlotMinIntensity"));
				m_MsFileScanner.LCMS2DOverviewPlotDivisor = int.Parse(m_TaskParams.GetParam("LCMS2DOverviewPlotDivisor"));

				// Get the input file name
				string sFileOrFolderName = GetDataFileOrFolderName(sourceFolder, out bSkipPlots);
				if (sFileOrFolderName == "Invalid File Type")
				{
					// DS quality test not implemented for this file type
					result.CloseoutMsg = "";
					result.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
					result.EvalMsg = "Dataset quality test not implemented for data type " + m_TaskParams.GetParam("rawdatatype");
					result.EvalCode = EnumEvalCode.EVAL_CODE_NOT_EVALUATED;
					return result;
				}

				if (string.IsNullOrEmpty(sFileOrFolderName))
				{
					// There was a problem with getting the file name; Details reported by called method
					result.CloseoutMsg = m_Msg;
					result.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
					return result;
				}

				if (bSkipPlots)
				{
					// Do not create any plots
					m_MsFileScanner.SaveTICAndBPIPlots = false;
					m_MsFileScanner.SaveLCMS2DPlots = false;
				}

				// Make the output folder
				if (!Directory.Exists(outputFolder))
				{
					try
					{
						Directory.CreateDirectory(outputFolder);
						m_Msg = "clsPluginMain.RunMsFileInfoScanner: Created output folder " + outputFolder;
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_Msg);
					}
					catch (Exception ex)
					{
						m_Msg = "clsPluginMain.RunMsFileInfoScanner: Exception creating output folder " + outputFolder;
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg, ex);
						result.CloseoutMsg = "Exception creating output folder " + outputFolder;
						result.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
						return result;
					}
				}
				
				// Call the file scanner DLL
				clsMSFileInfoScanner.eMSFileScannerErrorCodes errorCode;
				bool success;

				m_ErrOccurred = false;
				m_Msg = string.Empty;

				success = m_MsFileScanner.ProcessMSFileOrFolder(Path.Combine(sourceFolder, sFileOrFolderName), outputFolder);
				if (!success)
				{
					// Either a bad result code was returned, or an error event was received
					if (string.IsNullOrEmpty(m_Msg))
						m_Msg = "ProcessMSFileOrFolder returned false";

					result.CloseoutMsg = "Job " + m_TaskParams.GetParam("Job") + ", Step " + m_TaskParams.GetParam("Step") + ": " + m_Msg;
					result.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
				}
				else
				{
                    int iPostCount = 0;

                    while (iPostCount <= 2)
                    {
                        success = m_MsFileScanner.PostDatasetInfoUseDatasetID(int.Parse(m_TaskParams.GetParam("Dataset_ID")),
                                                    m_MgrParams.GetParam("connectionstring"), MS_FILE_SCANNER_DS_INFO_SP);

                        if (success)
                            break;
                        else
                        {
                            // If the error message contains the text "timeout expired" then try again, up to 2 times
                            if (!m_Msg.ToLower().Contains("timeout expired"))                            
                                break;
                        }

                        iPostCount += 1;
                    }

					if (success)
					{
						errorCode = clsMSFileInfoScanner.eMSFileScannerErrorCodes.NoError;
					}
					else
					{
						errorCode = m_MsFileScanner.ErrorCode;
						m_Msg = "clsPluginMain.RunMsFileInfoScanner: Error running info scanner. Message = " +
										m_MsFileScanner.GetErrorMessage() + " Result code = " + ((int)m_MsFileScanner.ErrorCode).ToString();
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg);
					}

					if ((errorCode != clsMSFileInfoScanner.eMSFileScannerErrorCodes.NoError) || m_ErrOccurred)
					{
						// Either a bad result code was returned, or an error event was received
						result.CloseoutMsg = "Job " + m_TaskParams.GetParam("Job") + ", Step " + m_TaskParams.GetParam("Step") +
									": MSFileInfoScanner error.";
						result.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
					}
					else
					{
						// Everything went wonderfully
						result.CloseoutMsg = "Job " + m_TaskParams.GetParam("Job") + ", Step " + m_TaskParams.GetParam("Step") +
									" completed successfully";
						result.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
					}
				}

				return result;
			}	// End sub

			/// <summary>
			/// Returns the file or folder name for specified dataset based on dataset type
			/// </summary>
			/// <returns>Data file or folder name</returns>
			private string GetDataFileOrFolderName(string inputFolder, out bool bSkipPlots)
			{
				string dataset = m_TaskParams.GetParam("Dataset");
				string sFileOrFolderName;
				bSkipPlots = false;

				// Get the expected file name based on the dataset type
				switch (m_TaskParams.GetParam("rawdatatype"))
				{
					case "dot_raw_files":
						sFileOrFolderName = dataset + ".raw";
						break;
					case "zipped_s_folders":
						sFileOrFolderName = "analysis.baf";
						break;
					case "bruker_ft":
						string sInstrumentClass = m_TaskParams.GetParam("Instrument_Class");

						if (sInstrumentClass == "Bruker_Amazon_Ion_Trap")
							sFileOrFolderName = dataset + ".d" + "\\" + "analysis.yep";
						else
							sFileOrFolderName = dataset + ".d" + "\\" + "analysis.baf";
						break;
					case "dot_uimf_files":
						sFileOrFolderName = dataset + ".uimf";
						break;
					case "sciex_wiff_files":
						sFileOrFolderName = dataset + ".wiff";
						bSkipPlots = false;
						break;
					case "dot_d_folders":
						sFileOrFolderName = dataset + ".d";
						bSkipPlots = true;
						break;
					default:
						m_Msg = "clsPluginMain.GetDataFileOrFolderName: Data type " + m_TaskParams.GetParam("rawdatatype") +
									" not recognized";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, m_Msg);
						m_Msg = "Data type " + m_TaskParams.GetParam("rawdatatype") + " not recognized";
						return "Invalid File Type";
				}

				// Test to verify the file (or folder) exists
				string sFileOrFolderPath = Path.Combine(inputFolder, sFileOrFolderName);

				if (!File.Exists(sFileOrFolderPath) && !Directory.Exists(sFileOrFolderPath))
				{
					m_Msg = "clsPluginMain.GetDataFileOrFolderName: File " + sFileOrFolderPath + " not found";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg);
					m_Msg = "File " + sFileOrFolderPath + " not found";
					sFileOrFolderName = string.Empty;
				}

				return sFileOrFolderName;
			}	// End sub

			/// <summary>
			/// Stores the tool version info in the database
			/// </summary>
			/// <remarks></remarks>
			protected bool StoreToolVersionInfo()
			{

				string strToolVersionInfo = string.Empty;
				System.IO.FileInfo ioAppFileInfo = new System.IO.FileInfo(System.Reflection.Assembly.GetExecutingAssembly().Location);
				bool bSuccess;

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info");

				// Lookup the version of the Capture tool plugin
				string strPluginPath = System.IO.Path.Combine(ioAppFileInfo.DirectoryName, "DatasetQualityPlugin.dll");
				bSuccess = base.StoreToolVersionInfoOneFile(ref strToolVersionInfo, strPluginPath);
				if (!bSuccess)
					return false;

				// Lookup the version of the Capture tool plugin
				string strMSFileInfoScanner = System.IO.Path.Combine(ioAppFileInfo.DirectoryName, "MSFileInfoScanner.dll");
				bSuccess = base.StoreToolVersionInfoOneFile(ref strToolVersionInfo, strMSFileInfoScanner);
				if (!bSuccess)
					return false;

				// Lookup the version of the Capture tool plugin
				string strUIMFLibraryPath = System.IO.Path.Combine(ioAppFileInfo.DirectoryName, "UIMFLibrary.dll");
				bSuccess = base.StoreToolVersionInfoOneFile(ref strToolVersionInfo, strUIMFLibraryPath);
				if (!bSuccess)
					return false;

				// Store path to CaptureToolPlugin.dll in ioToolFiles
				System.Collections.Generic.List<System.IO.FileInfo> ioToolFiles = new System.Collections.Generic.List<System.IO.FileInfo>();
				ioToolFiles.Add(new System.IO.FileInfo(strPluginPath));
				ioToolFiles.Add(new System.IO.FileInfo(strMSFileInfoScanner));

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

		#endregion

		#region "Event handlers"
			/// <summary>
			/// Handles message event from MS file scanner
			/// </summary>
			/// <param name="Message">Event message</param>
			void m_MsFileScanner_MessageEvent(string Message)
			{
				m_Msg = "clsPluginMain.RunMsFileInfoScanner: Message from MSFileInfoScanner = " + Message;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_Msg);
			}	// End sub

			/// <summary>
			/// Handles error event from MS file scanner
			/// </summary>
			/// <param name="Message">Error message</param>
			void m_MsFileScanner_ErrorEvent(string Message)
			{
				m_ErrOccurred = true;
				m_Msg = "clsPluginMain.RunMsFileInfoScanner: Error running MSFileInfoScanner. Error message = " + Message;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg);
			}	// End sub
		#endregion
	}	// End class
}	// End namespace
