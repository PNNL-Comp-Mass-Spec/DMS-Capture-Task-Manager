
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
		/// Runs the dataset info step tool
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

			// Store the version info in the database
			if (!StoreToolVersionInfo())
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false");
				retData.CloseoutMsg = "Error determining tool version info";
				retData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
				return retData;
			}

			msg = "Running DatasetInfo on dataset '" + m_Dataset + "'";
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
		/// Initializes the dataset info tool
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
			m_MsFileScanner.SaveTICAndBPIPlots = m_TaskParams.GetParam("SaveTICAndBPIPlots", false);
			m_MsFileScanner.SaveLCMS2DPlots = m_TaskParams.GetParam("SaveLCMS2DPlots", false);
			m_MsFileScanner.ComputeOverallQualityScores = m_TaskParams.GetParam("ComputeOverallQualityScores", false);
			m_MsFileScanner.CreateDatasetInfoFile = m_TaskParams.GetParam("CreateDatasetInfoFile", false);

			m_MsFileScanner.LCMS2DPlotMZResolution = m_TaskParams.GetParam("LCMS2DPlotMZResolution", (float)0.4);
			m_MsFileScanner.LCMS2DPlotMaxPointsToPlot = m_TaskParams.GetParam("LCMS2DPlotMaxPointsToPlot", 500000);
			m_MsFileScanner.LCMS2DPlotMinPointsPerSpectrum = m_TaskParams.GetParam("LCMS2DPlotMinPointsPerSpectrum", 2);
			m_MsFileScanner.LCMS2DPlotMinIntensity = m_TaskParams.GetParam("LCMS2DPlotMinIntensity", (float)0);
			m_MsFileScanner.LCMS2DOverviewPlotDivisor = m_TaskParams.GetParam("LCMS2DOverviewPlotDivisor", 10);

			// Get the input file name
			clsInstrumentClassInfo.eRawDataType rawDataType = clsInstrumentClassInfo.eRawDataType.Unknown;
			clsInstrumentClassInfo.eInstrumentClass instrumentClass = clsInstrumentClassInfo.eInstrumentClass.Unknown;
			bool bBrukerDotDBaf;
			string sFileOrFolderName = GetDataFileOrFolderName(sourceFolder, out bSkipPlots, out rawDataType, out instrumentClass, out bBrukerDotDBaf);

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
				result.CloseoutMsg = string.Empty;
				result.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
				result.EvalMsg = "Dataset info test not implemented for data type " + clsInstrumentClassInfo.GetRawDataTypeName(rawDataType) + ", instrument class " + clsInstrumentClassInfo.GetInstrumentClassName(instrumentClass);
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

				if (bBrukerDotDBaf)
				{
					// 12T_FTICR_B datasets (with .D folders and analysis.baf and/or fid files) sometimes work with MSFileInfoscanner, and sometimes don't
					// The problem is that ProteoWizard doesn't support certain forms of these datasets
					// In particular, small datasets (lasting just a few seconds) don't work
					
					result.CloseoutMsg = string.Empty;
					result.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
					result.EvalMsg = "MSFileInfoScanner error for data type " + clsInstrumentClassInfo.GetRawDataTypeName(rawDataType) + ", instrument class " + clsInstrumentClassInfo.GetInstrumentClassName(instrumentClass);
					result.EvalCode = EnumEvalCode.EVAL_CODE_NOT_EVALUATED;
					return result;

				}
				else
				{

					if (string.IsNullOrEmpty(m_Msg))
						m_Msg = "ProcessMSFileOrFolder returned false";

					result.CloseoutMsg = "Job " + m_Job + ", Step " + m_TaskParams.GetParam("Step") + ": " + m_Msg;
					result.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
				}
			}
			else
			{
				int iPostCount = 0;
				int iDatasetID;
				string connectionString = m_MgrParams.GetParam("connectionstring");

				iDatasetID = m_TaskParams.GetParam("Dataset_ID", 0);

				while (iPostCount <= 2)
				{
					success = m_MsFileScanner.PostDatasetInfoUseDatasetID(iDatasetID, connectionString, MS_FILE_SCANNER_DS_INFO_SP);

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

				bool bFailJob = false;

				if ((errorCode != MSFileInfoScannerInterfaces.iMSFileInfoScanner.eMSFileScannerErrorCodes.NoError) || m_ErrOccurred)
				{

				}

				if (bFailJob)
				{
					// Either a bad result code was returned, or an error event was received
					result.CloseoutMsg = "MSFileInfoScanner error";
					result.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
				}
				else
				{
					// Everything went wonderfully
					result.CloseoutMsg = string.Empty;
					result.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
				}
			}

			return result;
		}	// End sub

		/// <summary>
		/// Looks for a zip file matching "0_R*X*.zip"
		/// </summary>
		/// <param name="diDatasetFolder">Dataset folder</param>
		/// <returns>Returns the file name if found, otherwise an empty string</returns>
		private string CheckForBrukerImagingZipFiles(System.IO.DirectoryInfo diDatasetFolder)
		{
			
			System.IO.FileInfo[] fiFiles;
			fiFiles = diDatasetFolder.GetFiles("0_R*X*.zip");

			if (fiFiles != null && fiFiles.Length > 0)
			{
				return fiFiles[0].Name;
			}
			else
			{
				return string.Empty;
			}
			 
		}

		/// <summary>
		/// Returns the file or folder name for specified dataset based on dataset type
		/// </summary>
		/// <returns>Data file or folder name; empty string if not found</returns>
		/// <remarks>Will return UNKNOWN_FILE_TYPE or INVALID_FILE_TYPE in special circumstances</remarks>
		private string GetDataFileOrFolderName(string inputFolder, out bool bSkipPlots, out clsInstrumentClassInfo.eRawDataType rawDataType, out clsInstrumentClassInfo.eInstrumentClass instrumentClass, out bool bBrukerDotDBaf)
		{
			string sFileOrFolderName;
			bool bIsFile = true;

			bSkipPlots = false;
			instrumentClass = clsInstrumentClassInfo.eInstrumentClass.Unknown;
			rawDataType = clsInstrumentClassInfo.eRawDataType.Unknown;
			bBrukerDotDBaf = false;

			// Determine the Instrument Class and RawDataType
			string instClassName = m_TaskParams.GetParam("Instrument_Class");
			string rawDataTypeName = m_TaskParams.GetParam("rawdatatype", "UnknownRawDataType");

			instrumentClass = clsInstrumentClassInfo.GetInstrumentClass(instClassName);
			if (instrumentClass == clsInstrumentClassInfo.eInstrumentClass.Unknown)
			{
				m_Msg = "Instrument class not recognized: " + instClassName;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg);
				return UNKNOWN_FILE_TYPE;
			}

			rawDataType = clsInstrumentClassInfo.GetRawDataType(rawDataTypeName);
			if (rawDataType == clsInstrumentClassInfo.eRawDataType.Unknown)
			{
				m_Msg = "RawDataType not recognized: " + rawDataTypeName;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg);
				return UNKNOWN_FILE_TYPE;
			}

			System.IO.DirectoryInfo diDatasetFolder = new System.IO.DirectoryInfo(inputFolder);

			// Get the expected file name based on the dataset type
			switch (rawDataType)
			{
				case clsInstrumentClassInfo.eRawDataType.ThermoRawFile:
					// LTQ_2, LTQ_4, etc.
					// LTQ_Orb_1, LTQ_Orb_2, etc.
					// VOrbiETD01, VOrbiETD02, etc.
					// TSQ_3
					// Thermo_GC_MS_01
					sFileOrFolderName = m_Dataset + clsInstrumentClassInfo.DOT_RAW_EXTENSION;
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
					// 12T_FTICR_Imaging and 15T_FTICR_Imaging datasets with instrument class BrukerMALDI_Imaging_V2 will also have bruker_ft format; however, instead of an analysis.baf file, they might have a .mcf file
					
					bIsFile = true;
					if (instrumentClass == clsInstrumentClassInfo.eInstrumentClass.Bruker_Amazon_Ion_Trap)
					{
						sFileOrFolderName = System.IO.Path.Combine(m_Dataset + clsInstrumentClassInfo.DOT_D_EXTENSION, "analysis.yep");
					}
					else
					{
						sFileOrFolderName = System.IO.Path.Combine(m_Dataset + clsInstrumentClassInfo.DOT_D_EXTENSION, "analysis.baf");
						bBrukerDotDBaf = true;
					}

					if (!File.Exists(Path.Combine(diDatasetFolder.FullName, sFileOrFolderName)))
						sFileOrFolderName = CheckForBrukerImagingZipFiles(diDatasetFolder);
					
					break;

				case clsInstrumentClassInfo.eRawDataType.UIMF:
					// IMS_TOF_2, IMS_TOF_3, IMS_TOF_4, IMS_TOF_5, IMS_TOF_6, etc.
					sFileOrFolderName = m_Dataset + clsInstrumentClassInfo.DOT_UIMF_EXTENSION;
					bIsFile = true;
					break;
			
				case clsInstrumentClassInfo.eRawDataType.SciexWiffFile:
					// QTrap01
					sFileOrFolderName = m_Dataset + clsInstrumentClassInfo.DOT_WIFF_EXTENSION;
					bIsFile = true;
					break;

				case clsInstrumentClassInfo.eRawDataType.AgilentDFolder:
					// Agilent_GC_MS_01, AgQTOF03, AgQTOF04, PrepHPLC1
					sFileOrFolderName = m_Dataset + clsInstrumentClassInfo.DOT_D_EXTENSION;
					bIsFile = false;

					if (instrumentClass == clsInstrumentClassInfo.eInstrumentClass.PrepHPLC)
					{
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Skipping MSFileInfoScanner since PrepHPLC dataset");
						return INVALID_FILE_TYPE;
					}

					break;

				case clsInstrumentClassInfo.eRawDataType.BrukerMALDIImaging:
					// bruker_maldi_imaging: 12T_FTICR_Imaging, 15T_FTICR_Imaging, and BrukerTOF_Imaging_01
					// Find the name of the first zip file

					sFileOrFolderName = CheckForBrukerImagingZipFiles(diDatasetFolder);
					bSkipPlots = true;
					bIsFile = true;

					if (string.IsNullOrEmpty(sFileOrFolderName))
					{
						m_Msg = "Did not find any 0_R*.zip files in the dataset folder";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "clsPluginMain.GetDataFileOrFolderName: " + m_Msg);
						return INVALID_FILE_TYPE;
					}
					
					break;

				case clsInstrumentClassInfo.eRawDataType.BrukerTOFBaf:
					sFileOrFolderName = m_Dataset + clsInstrumentClassInfo.DOT_D_EXTENSION;
					bIsFile = false;
					break;

				default:
					// Other instruments; do not process
					// dot_wiff_files (AgilentQStarWiffFile): AgTOF02
					// bruker_maldi_spot (BrukerMALDISpot): BrukerTOF_01
					m_Msg = "Data type " + rawDataType + " not recognized";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "clsPluginMain.GetDataFileOrFolderName: " + m_Msg);
					return INVALID_FILE_TYPE;
			}

			// Test to verify the file (or folder) exists
			string sFileOrFolderPath = Path.Combine(diDatasetFolder.FullName, sFileOrFolderName);

			if (bIsFile && !File.Exists(sFileOrFolderPath))
			{

				// File not found; look for alternate extensions
				System.Collections.Generic.List<string> lstAlternateExtensions = new System.Collections.Generic.List<string>();
				bool bAlternateFound = false;
				string dataFileNamePathAlt;

				lstAlternateExtensions.Add("mgf");
				lstAlternateExtensions.Add("mzXML");
				lstAlternateExtensions.Add("mzML");

				foreach (string altExtension in lstAlternateExtensions)
				{
					dataFileNamePathAlt = System.IO.Path.ChangeExtension(sFileOrFolderPath, altExtension);
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
					DirectoryInfo diDotDFolder = new DirectoryInfo(Path.Combine(diDatasetFolder.FullName, m_Dataset + clsInstrumentClassInfo.DOT_D_EXTENSION));

					if (diDotDFolder.Exists)
					{
						// Look for a .mcf file in the .D folder

						string mcfFileName = string.Empty;
						Int64 mcfFileSizeBytes = 0;

						foreach (FileInfo fiFile in diDotDFolder.GetFiles("*.mcf"))
						{
							// Determine the largest .mcf file
							if (fiFile.Length > mcfFileSizeBytes)
							{
								mcfFileSizeBytes = fiFile.Length;
								mcfFileName = fiFile.Name;
								bAlternateFound = true;
								sFileOrFolderName = diDotDFolder.Name;
							}
						}

					}
					else
					{
						// Look for any .D folder; operator may have placed the wrong .D folder in this dataset folder
						var diDotDFolders = diDatasetFolder.GetDirectories("*.d");
						if (diDotDFolders.Length > 0)
						{
							m_Msg = "Dataset folder has a misnamed .D folder inside it.  Found " + diDotDFolders[0].Name + " but expecting " + m_Dataset + clsInstrumentClassInfo.DOT_D_EXTENSION;
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg);
							return string.Empty;
						}
					}
				}

				if (!bAlternateFound)
				{
					m_Msg = "clsPluginMain.GetDataFileOrFolderName: File " + sFileOrFolderPath + " not found";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg);
					m_Msg = "File " + sFileOrFolderPath + " not found";
					return string.Empty;
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

