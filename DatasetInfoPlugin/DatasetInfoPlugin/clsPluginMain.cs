
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 10/06/2009
//
// Last modified 10/06/2009
//               05/04/2010 dac - Added Matt Monroe MSFileScanner for quality checks
//               09/17/2012 mem - Moved from the DatasetQuality plugin to the Dataset Info plugin
//*********************************************************************************************************
using System;
using CaptureTaskManager;
using System.IO;

namespace DatasetInfoPlugin
{

	public class clsPluginMain : clsToolRunnerBase
	{
		//*********************************************************************************************************
		// Main class for plugin
		//**********************************************************************************************************

		#region "Constants"
		const string MS_FILE_SCANNER_DS_INFO_SP = "CacheDatasetInfoXML";
		const string UNKNOWN_FILE_TYPE = "Unknown File Type";
		const string INVALID_FILE_TYPE = "Invalid File Type";
		#endregion

		#region "Class-wide variables"
		MSFileInfoScannerInterfaces.iMSFileInfoScanner m_MsFileScanner;
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

			msg = "Starting DatasetInfoPlugin.clsPluginMain.RunTool()";
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

			retData = RunMsFileInfoScanner();

			msg = "Completed clsPluginMain.RunTool()";
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

			return retData;
		}	// End sub


		private MSFileInfoScannerInterfaces.iMSFileInfoScanner LoadMSFileInfoScanner(string strMSFileInfoScannerDLLPath)
		{
			const string MsDataFileReaderClass = "MSFileInfoScanner.clsMSFileInfoScanner";

			MSFileInfoScannerInterfaces.iMSFileInfoScanner objMSFileInfoScanner = null;
			string msg;

			try
			{
				if (!System.IO.File.Exists(strMSFileInfoScannerDLLPath))
				{
					msg = "DLL not found: " + strMSFileInfoScannerDLLPath;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
				}
				else
				{
					object obj = null;
					obj = LoadObject(MsDataFileReaderClass, strMSFileInfoScannerDLLPath);
					if (obj != null)
					{
						objMSFileInfoScanner = (MSFileInfoScannerInterfaces.iMSFileInfoScanner)obj;
						msg = "Loaded MSFileInfoScanner from " + strMSFileInfoScannerDLLPath;
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
					}

				}

			}
			catch (Exception ex)
			{
				msg = "Exception loading class " + MsDataFileReaderClass + ": " + ex.Message;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
			}

			return objMSFileInfoScanner;
		}

		private object LoadObject(string className, string strDLLFilePath)
		{
			object obj = null;
			try
			{
				// Dynamically load the specified class from strDLLFilePath
				System.Reflection.Assembly assem;
				assem = System.Reflection.Assembly.LoadFrom(strDLLFilePath);
				Type dllType = assem.GetType(className, false, true);
				obj = Activator.CreateInstance(dllType);
			}
			catch (Exception ex)
			{
				string msg = "Exception loading DLL " + strDLLFilePath + ": " + ex.Message;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
			}
			return obj;
		}

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

			string strMSFileInfoScannerPath = GetMSFileInfoScannerDLLPath();
			if (string.IsNullOrEmpty(strMSFileInfoScannerPath))
				throw new System.NotSupportedException("Manager parameter 'MSFileInfoScannerDir' is not defined");

			if (!System.IO.File.Exists(strMSFileInfoScannerPath))
			{
				throw new System.IO.FileNotFoundException("File Not Found: " + strMSFileInfoScannerPath);
			}

			// Initialize the MSFileScanner class
			m_MsFileScanner = LoadMSFileInfoScanner(strMSFileInfoScannerPath);

			m_MsFileScanner.ErrorEvent += new MSFileInfoScannerInterfaces.iMSFileInfoScanner.ErrorEventEventHandler(m_MsFileScanner_ErrorEvent);
			m_MsFileScanner.MessageEvent += new MSFileInfoScannerInterfaces.iMSFileInfoScanner.MessageEventEventHandler(m_MsFileScanner_MessageEvent);

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
			sourceFolder = Path.Combine(sourceFolder, m_TaskParams.GetParam("Storage_Path"));
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
			string rawDataTypeName = "???";
			string sFileOrFolderName = GetDataFileOrFolderName(sourceFolder, out bSkipPlots, out rawDataTypeName);

			if (sFileOrFolderName == UNKNOWN_FILE_TYPE)
			{
				// Raw_Data_Type not recognized
				result.CloseoutMsg = m_Msg;
				result.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
				return result;
			}

			if (sFileOrFolderName == INVALID_FILE_TYPE)
			{
				// DS quality test not implemented for this file type
				result.CloseoutMsg = "";
				result.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
				result.EvalMsg = "Dataset quality test not implemented for data type " + rawDataTypeName;
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
			MSFileInfoScannerInterfaces.iMSFileInfoScanner.eMSFileScannerErrorCodes errorCode;
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
					errorCode = MSFileInfoScannerInterfaces.iMSFileInfoScanner.eMSFileScannerErrorCodes.NoError;
				}
				else
				{
					errorCode = m_MsFileScanner.ErrorCode;
					m_Msg = "clsPluginMain.RunMsFileInfoScanner: Error running info scanner. Message = " +
									m_MsFileScanner.GetErrorMessage() + " Result code = " + ((int)m_MsFileScanner.ErrorCode).ToString();
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg);
				}

				if ((errorCode != MSFileInfoScannerInterfaces.iMSFileInfoScanner.eMSFileScannerErrorCodes.NoError) || m_ErrOccurred)
				{
					// Either a bad result code was returned, or an error event was received
					result.CloseoutMsg = "MSFileInfoScanner error";
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
		/// <returns>Data file or folder name; empty string if not found</returns>
		/// <remarks>Will return UNKNOWN_FILE_TYPE or INVALID_FILE_TYPE in special circumstances</remarks>
		private string GetDataFileOrFolderName(string inputFolder, out bool bSkipPlots, out string rawDataTypeName)
		{
			string dataset = m_TaskParams.GetParam("Dataset");
			string sFileOrFolderName;
			bool bIsFile = true;
			bSkipPlots = false;

			rawDataTypeName = m_TaskParams.GetParam("rawdatatype", "UnknownRawDataType");
			clsInstrumentClassInfo.eRawDataType rawDataType = clsInstrumentClassInfo.GetRawDataType(rawDataTypeName);
			if (rawDataType == clsInstrumentClassInfo.eRawDataType.Unknown)
			{
				m_Msg = "RawDataType not recognized: " + rawDataTypeName;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg);
				return UNKNOWN_FILE_TYPE;
			}

			// Get the expected file name based on the dataset type
			switch (rawDataType)
			{
				case clsInstrumentClassInfo.eRawDataType.ThermoRawFile:
					// LTQ_2, LTQ_4, etc.
					// LTQ_Orb_1, LTQ_Orb_2, etc.
					// VOrbiETD01, VOrbiETD02, etc.
					// TSQ_3
					// Thermo_GC_MS_01
					sFileOrFolderName = dataset + clsInstrumentClassInfo.DOT_RAW_EXTENSION;
					bIsFile = true;
					break;
				case clsInstrumentClassInfo.eRawDataType.ZippedSFolders:
					// 9T_FTICR, 11T_FTICR_B, and 12T_FTICR
					sFileOrFolderName = "analysis.baf";
					bIsFile = true;
					break;
				case clsInstrumentClassInfo.eRawDataType.BrukerFTFolder:
					// 12T_FTICR_B, 15T_FTICR, 9T_FTICR_B
					// Also, Bruker_FT_IonTrap01, which is Bruker_Amazon_Ion_Trap
					string sInstrumentClass = m_TaskParams.GetParam("Instrument_Class");

					if (sInstrumentClass == "Bruker_Amazon_Ion_Trap")
						sFileOrFolderName = dataset + clsInstrumentClassInfo.DOT_D_EXTENSION + "\\" + "analysis.yep";
					else
						sFileOrFolderName = dataset + clsInstrumentClassInfo.DOT_D_EXTENSION + "\\" + "analysis.baf";

					bIsFile = true;
					break;
				case clsInstrumentClassInfo.eRawDataType.UIMF:
					// IMS_TOF_2, IMS_TOF_3, IMS_TOF_4, IMS_TOF_5, IMS_TOF_6, etc.
					sFileOrFolderName = dataset + clsInstrumentClassInfo.DOT_UIMF_EXTENSION;
					bIsFile = true;
					break;
				case clsInstrumentClassInfo.eRawDataType.AgilentQStarWiffFile:
					// QTrap01
					sFileOrFolderName = dataset + clsInstrumentClassInfo.DOT_WIFF_EXTENSION;
					bIsFile = true;
					bSkipPlots = false;
					break;
				case clsInstrumentClassInfo.eRawDataType.AgilentDFolder:
					// Agilent_GC_MS_01, AgQTOF03, AgQTOF04
					sFileOrFolderName = dataset + clsInstrumentClassInfo.DOT_D_EXTENSION;
					bIsFile = false;
					bSkipPlots = false;
					break;

				case clsInstrumentClassInfo.eRawDataType.BrukerMALDIImaging:
					// bruker_maldi_imaging: 12T_FTICR_Imaging, 15T_FTICR_Imaging, and BrukerTOF_Imaging_01
					// Find the name of the first zip file
					bSkipPlots = true;
					System.IO.DirectoryInfo diFolder = new System.IO.DirectoryInfo(inputFolder);
					System.IO.FileInfo[] fiFiles;
					fiFiles = diFolder.GetFiles("0_R*.zip");

					if (fiFiles != null && fiFiles.Length > 0)
					{
						sFileOrFolderName = fiFiles[0].Name;
						bIsFile = true;
					}
					else
					{
						m_Msg = "Did not find any 0_R*.zip files in the dataset folder";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "clsPluginMain.GetDataFileOrFolderName: " + m_Msg);
						return INVALID_FILE_TYPE;
					}
					break;

				case clsInstrumentClassInfo.eRawDataType.BrukerTOFBaf:
					sFileOrFolderName = dataset + clsInstrumentClassInfo.DOT_D_EXTENSION;
					bIsFile = false;
					bSkipPlots = false;
					break;

				default:
					// Other instruments; do not process
					// dot_wiff_files: Agilent_TOF2
					// bruker_maldi_spot: BrukerTOF_01
					m_Msg = "Data type " + rawDataType + " not recognized";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "clsPluginMain.GetDataFileOrFolderName: " + m_Msg);
					return INVALID_FILE_TYPE;
			}

			// Test to verify the file (or folder) exists
			string sFileOrFolderPath = Path.Combine(inputFolder, sFileOrFolderName);

			if (bIsFile && !File.Exists(sFileOrFolderPath))
			{
				// File not found; look for alternate extensions
				System.Collections.Generic.List<string> lstAlternateExtensions = new System.Collections.Generic.List<string>();
				bool bAlternateFound = false;
				lstAlternateExtensions.Add("mgf");
				lstAlternateExtensions.Add("mzXML");
				lstAlternateExtensions.Add("mzML");

				foreach (string altExtension in lstAlternateExtensions)
				{
					string dataFileNamePathAlt = System.IO.Path.ChangeExtension(sFileOrFolderPath, altExtension);
					if (File.Exists(dataFileNamePathAlt))
					{
						m_Msg = "Data file not found, but ." + altExtension + " file exists";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, m_Msg);
						bAlternateFound = true;
						sFileOrFolderPath = INVALID_FILE_TYPE;
						sFileOrFolderName = INVALID_FILE_TYPE;
						rawDataTypeName = altExtension + " file";
						break;
					}
				}

				if (!bAlternateFound)
				{
					m_Msg = "clsPluginMain.GetDataFileOrFolderName: File " + sFileOrFolderPath + " not found";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg);
					m_Msg = "File " + sFileOrFolderPath + " not found";
					sFileOrFolderName = string.Empty;
				}
			}

			if (!bIsFile && !Directory.Exists(sFileOrFolderPath))
			{
				m_Msg = "clsPluginMain.GetDataFileOrFolderName: Folder " + sFileOrFolderPath + " not found";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg);
				m_Msg = "Folder " + sFileOrFolderPath + " not found";
				sFileOrFolderName = string.Empty;
			}

			return sFileOrFolderName;

		}	// End sub

		/// <summary>
		/// Construct the full path to the MSFileInfoScanner.DLL
		/// </summary>
		/// <returns></returns>
		protected string GetMSFileInfoScannerDLLPath()
		{
			string strMSFileInfoScannerFolder = m_MgrParams.GetParam("MSFileInfoScannerDir", string.Empty);
			if (string.IsNullOrEmpty(strMSFileInfoScannerFolder))
				return string.Empty;
			else
				return System.IO.Path.Combine(strMSFileInfoScannerFolder, "MSFileInfoScanner.dll");
		}

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
			string strPluginPath = System.IO.Path.Combine(ioAppFileInfo.DirectoryName, "DatasetInfoPlugin.dll");
			bSuccess = base.StoreToolVersionInfoOneFile(ref strToolVersionInfo, strPluginPath);
			if (!bSuccess)
				return false;

			// Lookup the version of the MSFileInfoScanner DLL
			string strMSFileInfoScannerPath = GetMSFileInfoScannerDLLPath();
			if (!string.IsNullOrEmpty(strMSFileInfoScannerPath))
			{
				bSuccess = base.StoreToolVersionInfoOneFile(ref strToolVersionInfo, strMSFileInfoScannerPath);
				if (!bSuccess)
					return false;
			}

			// Lookup the version of the UIMFLibrary DLL
			string strUIMFLibraryPath = System.IO.Path.Combine(ioAppFileInfo.DirectoryName, "UIMFLibrary.dll");
			bSuccess = base.StoreToolVersionInfoOneFile(ref strToolVersionInfo, strUIMFLibraryPath);
			if (!bSuccess)
				return false;

			// Store path to CaptureToolPlugin.dll and MSFileInfoScanner.dll in ioToolFiles
			System.Collections.Generic.List<System.IO.FileInfo> ioToolFiles = new System.Collections.Generic.List<System.IO.FileInfo>();
			ioToolFiles.Add(new System.IO.FileInfo(strPluginPath));

			if (!string.IsNullOrEmpty(strMSFileInfoScannerPath))
				ioToolFiles.Add(new System.IO.FileInfo(strMSFileInfoScannerPath));

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

