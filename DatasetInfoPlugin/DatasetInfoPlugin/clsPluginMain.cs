
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
using System.Reflection;
using CaptureTaskManager;
using System.IO;
using MSFileInfoScannerInterfaces;

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

	    private const bool IGNORE_BRUKER_BAF_ERRORS = false;
		#endregion

		#region "Class-wide variables"
		iMSFileInfoScanner m_MsFileScanner;
		string m_Msg;
		bool m_ErrOccurred;
		#endregion

		#region "Methods"
		/// <summary>
		/// Runs the dataset info step tool
		/// </summary>
		/// <returns>Enum indicating success or failure</returns>
		public override clsToolReturnData RunTool()
		{
		    var msg = "Starting DatasetInfoPlugin.clsPluginMain.RunTool()";
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

			// Perform base class operations, if any
			var retData = base.RunTool();
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
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);

			retData = RunMsFileInfoScanner();

			msg = "Completed clsPluginMain.RunTool()";
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

			return retData;
		}	// End sub


		private iMSFileInfoScanner LoadMSFileInfoScanner(string strMSFileInfoScannerDLLPath)
		{
			const string MsDataFileReaderClass = "MSFileInfoScanner.clsMSFileInfoScanner";

			iMSFileInfoScanner objMSFileInfoScanner = null;
			string msg;

			try
			{
				if (!File.Exists(strMSFileInfoScannerDLLPath))
				{
					msg = "DLL not found: " + strMSFileInfoScannerDLLPath;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
				}
				else
				{
				    var obj = LoadObject(MsDataFileReaderClass, strMSFileInfoScannerDLLPath);
				    if (obj != null)
					{
						objMSFileInfoScanner = (iMSFileInfoScanner)obj;
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
			    var assem = Assembly.LoadFrom(strDLLFilePath);
				var dllType = assem.GetType(className, false, true);
				obj = Activator.CreateInstance(dllType);
			}
			catch (Exception ex)
			{
				var msg = "Exception loading DLL " + strDLLFilePath + ": " + ex.Message;
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
			var msg = "Starting clsPluginMain.Setup()";
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

			base.Setup(mgrParams, taskParams, statusTools);

			var strMSFileInfoScannerPath = GetMSFileInfoScannerDLLPath();
			if (string.IsNullOrEmpty(strMSFileInfoScannerPath))
				throw new NotSupportedException("Manager parameter 'MSFileInfoScannerDir' is not defined");

			if (!File.Exists(strMSFileInfoScannerPath))
			{
				throw new FileNotFoundException("File Not Found: " + strMSFileInfoScannerPath);
			}

			// Initialize the MSFileScanner class
			m_MsFileScanner = LoadMSFileInfoScanner(strMSFileInfoScannerPath);

			m_MsFileScanner.ErrorEvent += m_MsFileScanner_ErrorEvent;
			m_MsFileScanner.MessageEvent += m_MsFileScanner_MessageEvent;

			msg = "Completed clsPluginMain.Setup()";
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
		}

		/// <summary>
		/// Runs the MS_File_Info_Scanner tool
		/// </summary>
		/// <returns></returns>
		private clsToolReturnData RunMsFileInfoScanner()
		{
			var result = new clsToolReturnData();

			// Always use client perspective for the source folder (allows MSFileInfoScanner to run from any CTM)
			var sourceFolder = m_TaskParams.GetParam("Storage_Vol_External");

			// Set up the rest of the paths
			sourceFolder = Path.Combine(sourceFolder, m_TaskParams.GetParam("Storage_Path"));
			sourceFolder = Path.Combine(sourceFolder, m_TaskParams.GetParam("Folder"));
			var outputFolder = Path.Combine(sourceFolder, "QC");
			bool bSkipPlots;

			// Set up the params for the MS file scanner
			m_MsFileScanner.DSInfoDBPostingEnabled = false;
			m_MsFileScanner.SaveTICAndBPIPlots = m_TaskParams.GetParam("SaveTICAndBPIPlots", false);
			m_MsFileScanner.SaveLCMS2DPlots = m_TaskParams.GetParam("SaveLCMS2DPlots", false);
			m_MsFileScanner.ComputeOverallQualityScores = m_TaskParams.GetParam("ComputeOverallQualityScores", false);
			m_MsFileScanner.CreateDatasetInfoFile = m_TaskParams.GetParam("CreateDatasetInfoFile", false);
		    
			m_MsFileScanner.LCMS2DPlotMZResolution = m_TaskParams.GetParam("LCMS2DPlotMZResolution", clsLCMSDataPlotterOptions.DEFAULT_MZ_RESOLUTION);
            m_MsFileScanner.LCMS2DPlotMaxPointsToPlot = m_TaskParams.GetParam("LCMS2DPlotMaxPointsToPlot", clsLCMSDataPlotterOptions.DEFAULT_MAX_POINTS_TO_PLOT);
			m_MsFileScanner.LCMS2DPlotMinPointsPerSpectrum = m_TaskParams.GetParam("LCMS2DPlotMinPointsPerSpectrum", clsLCMSDataPlotterOptions.DEFAULT_MIN_POINTS_PER_SPECTRUM);
			m_MsFileScanner.LCMS2DPlotMinIntensity = m_TaskParams.GetParam("LCMS2DPlotMinIntensity", (float)0);
            m_MsFileScanner.LCMS2DOverviewPlotDivisor = m_TaskParams.GetParam("LCMS2DOverviewPlotDivisor", clsLCMSDataPlotterOptions.DEFAULT_LCMS2D_OVERVIEW_PLOT_DIVISOR);

			m_MsFileScanner.CheckCentroidingStatus = true;

			// Get the input file name
			clsInstrumentClassInfo.eRawDataType rawDataType;
			clsInstrumentClassInfo.eInstrumentClass instrumentClass;
			bool bBrukerDotDBaf;
			var sFileOrFolderName = GetDataFileOrFolderName(sourceFolder, out bSkipPlots, out rawDataType, out instrumentClass, out bBrukerDotDBaf);

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
					string msg = "clsPluginMain.RunMsFileInfoScanner: Created output folder " + outputFolder;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
				}
				catch (Exception ex)
				{
                    string msg = "clsPluginMain.RunMsFileInfoScanner: Exception creating output folder " + outputFolder;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
					result.CloseoutMsg = "Exception creating output folder " + outputFolder;
					result.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
					return result;
				}
			}

			// Call the file scanner DLL

		    m_ErrOccurred = false;
			m_Msg = string.Empty;

			var success = m_MsFileScanner.ProcessMSFileOrFolder(Path.Combine(sourceFolder, sFileOrFolderName), outputFolder);

		    if (m_ErrOccurred)
		        success = false;

			if (!success)
			{
				// Either a bad result code was returned, or an error event was received

				if (bBrukerDotDBaf && IGNORE_BRUKER_BAF_ERRORS)
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
			    
                if (string.IsNullOrEmpty(m_Msg))
			        m_Msg = "ProcessMSFileOrFolder returned false";

			    result.CloseoutMsg = m_Msg;
			    result.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return result;
			}

		    var iPostCount = 0;
		    var connectionString = m_MgrParams.GetParam("connectionstring");

		    var iDatasetID = m_TaskParams.GetParam("Dataset_ID", 0);

		    while (iPostCount <= 2)
		    {
		        success = m_MsFileScanner.PostDatasetInfoUseDatasetID(iDatasetID, connectionString, MS_FILE_SCANNER_DS_INFO_SP);

		        if (success)
		            break;
				    
		        // If the error message contains the text "timeout expired" then try again, up to 2 times
		        if (!m_Msg.ToLower().Contains("timeout expired"))
		            break;

		        iPostCount += 1;
		    }

		    iMSFileInfoScanner.eMSFileScannerErrorCodes errorCode;
		    if (success)
		    {
		        errorCode = iMSFileInfoScanner.eMSFileScannerErrorCodes.NoError;
		    }
		    else
		    {
		        errorCode = m_MsFileScanner.ErrorCode;
		        m_Msg = "Error running info scanner. Message = " +
		                m_MsFileScanner.GetErrorMessage() + " Result code = " + ((int)m_MsFileScanner.ErrorCode);
		        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Msg);
		    }

		    if (errorCode == iMSFileInfoScanner.eMSFileScannerErrorCodes.NoError)
		    {
		        // Everything went wonderfully
		        result.CloseoutMsg = string.Empty;
		        result.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
		    }
		    else
		    {
		        // Either a bad result code was returned, or an error event was received
		        result.CloseoutMsg = "MSFileInfoScanner error";
		        result.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
		    }

		    return result;

		}

		/// <summary>
		/// Looks for a zip file matching "0_R*X*.zip"
		/// </summary>
		/// <param name="diDatasetFolder">Dataset folder</param>
		/// <returns>Returns the file name if found, otherwise an empty string</returns>
		private string CheckForBrukerImagingZipFiles(DirectoryInfo diDatasetFolder)
		{
		    var fiFiles = diDatasetFolder.GetFiles("0_R*X*.zip");

		    if (fiFiles.Length > 0)
			{
				return fiFiles[0].Name;
			}
		    
            return string.Empty;
		}

	    /// <summary>
		/// Returns the file or folder name for specified dataset based on dataset type
		/// </summary>
		/// <returns>Data file or folder name; empty string if not found</returns>
		/// <remarks>Will return UNKNOWN_FILE_TYPE or INVALID_FILE_TYPE in special circumstances</remarks>
		private string GetDataFileOrFolderName(string inputFolder, out bool bSkipPlots, out clsInstrumentClassInfo.eRawDataType rawDataType, out clsInstrumentClassInfo.eInstrumentClass instrumentClass, out bool bBrukerDotDBaf)
		{
			string sFileOrFolderName;
			bool bIsFile;

			bSkipPlots = false;
	        rawDataType = clsInstrumentClassInfo.eRawDataType.Unknown;
			bBrukerDotDBaf = false;

			// Determine the Instrument Class and RawDataType
			var instClassName = m_TaskParams.GetParam("Instrument_Class");
			var rawDataTypeName = m_TaskParams.GetParam("rawdatatype", "UnknownRawDataType");

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

			var diDatasetFolder = new DirectoryInfo(inputFolder);

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
						sFileOrFolderName = Path.Combine(m_Dataset + clsInstrumentClassInfo.DOT_D_EXTENSION, "analysis.yep");
					}
					else
					{
						sFileOrFolderName = Path.Combine(m_Dataset + clsInstrumentClassInfo.DOT_D_EXTENSION, "analysis.baf");
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
			var sFileOrFolderPath = Path.Combine(diDatasetFolder.FullName, sFileOrFolderName);

			if (bIsFile && !File.Exists(sFileOrFolderPath))
			{

				// File not found; look for alternate extensions
				var lstAlternateExtensions = new System.Collections.Generic.List<string>();
				var bAlternateFound = false;

			    lstAlternateExtensions.Add("mgf");
				lstAlternateExtensions.Add("mzXML");
				lstAlternateExtensions.Add("mzML");

				foreach (var altExtension in lstAlternateExtensions)
				{
				    var dataFileNamePathAlt = Path.ChangeExtension(sFileOrFolderPath, altExtension);
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
					var diDotDFolder = new DirectoryInfo(Path.Combine(diDatasetFolder.FullName, m_Dataset + clsInstrumentClassInfo.DOT_D_EXTENSION));

					if (diDotDFolder.Exists)
					{
						// Look for a .mcf file in the .D folder

					    Int64 mcfFileSizeBytes = 0;

						foreach (var fiFile in diDotDFolder.GetFiles("*.mcf"))
						{
							// Determine the largest .mcf file
							if (fiFile.Length > mcfFileSizeBytes)
							{
								mcfFileSizeBytes = fiFile.Length;
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
            var strMSFileInfoScannerFolder = m_MgrParams.GetParam("MSFileInfoScannerDir", string.Empty);
			if (string.IsNullOrEmpty(strMSFileInfoScannerFolder))
				return string.Empty;
		    
            return Path.Combine(strMSFileInfoScannerFolder, "MSFileInfoScanner.dll");
		}

		/// <summary>
		/// Stores the tool version info in the database
		/// </summary>
		/// <remarks></remarks>
		protected bool StoreToolVersionInfo()
		{

			var strToolVersionInfo = string.Empty;
			var fiExecutingAssembly = new FileInfo(Assembly.GetExecutingAssembly().Location);
		    if (fiExecutingAssembly.DirectoryName == null)
		    {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "FileInfo object for the executing assembly has a null value for DirectoryName");				
		        return false;
		    }

		    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info");

			// Lookup the version of the Capture tool plugin
			var strPluginPath = Path.Combine(fiExecutingAssembly.DirectoryName, "DatasetInfoPlugin.dll");
			var bSuccess = base.StoreToolVersionInfoOneFile(ref strToolVersionInfo, strPluginPath);
			if (!bSuccess)
				return false;

			// Lookup the version of the MSFileInfoScanner DLL
			var strMSFileInfoScannerPath = GetMSFileInfoScannerDLLPath();
			if (!string.IsNullOrEmpty(strMSFileInfoScannerPath))
			{
				bSuccess = base.StoreToolVersionInfoOneFile(ref strToolVersionInfo, strMSFileInfoScannerPath);
				if (!bSuccess)
					return false;
			}

			// Lookup the version of the UIMFLibrary DLL
			var strUIMFLibraryPath = Path.Combine(fiExecutingAssembly.DirectoryName, "UIMFLibrary.dll");
			bSuccess = base.StoreToolVersionInfoOneFile(ref strToolVersionInfo, strUIMFLibraryPath);
			if (!bSuccess)
				return false;

			// Store path to CaptureToolPlugin.dll and MSFileInfoScanner.dll in ioToolFiles
			var ioToolFiles = new System.Collections.Generic.List<FileInfo>
			{
			    new FileInfo(strPluginPath)
			};

		    if (!string.IsNullOrEmpty(strMSFileInfoScannerPath))
				ioToolFiles.Add(new FileInfo(strMSFileInfoScannerPath));

			try
			{
				return base.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, false);
			}
			catch (Exception ex)
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
			m_Msg = "Message from MSFileInfoScanner = " + Message;
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_Msg);
		}

		/// <summary>
		/// Handles error event from MS file scanner
		/// </summary>
        /// <param name="message">Error message</param>
		void m_MsFileScanner_ErrorEvent(string message)
		{
            var errorMsg = "clsPluginMain.RunMsFileInfoScanner, Error running MSFileInfoScanner: " + message;

            if (message.StartsWith("Error using ProteoWizard reader"))
            {
                // This is not always a critical error; log it as a warning
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, errorMsg);
            }
            else
            {
                m_ErrOccurred = true;
                m_Msg = "Error running MSFileInfoScanner: " + message;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMsg);
            }
			
		}

		#endregion
	}

}	// End namespace

