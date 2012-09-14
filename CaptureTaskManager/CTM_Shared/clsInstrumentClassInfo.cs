using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CaptureTaskManager
{
	public class clsInstrumentClassInfo
	{
		#region "Raw Data Type Constants"

		// Note: All of the RAW_DATA_TYPE constants need to be all lowercase
		//
		//Agilent ion trap data, Agilent TOF data
		public const string RAW_DATA_TYPE_DOT_D_FOLDERS = "dot_d_folders";
		//FTICR data, including instrument 3T_FTICR, 7T_FTICR, 9T_FTICR, 11T_FTICR, 11T_FTICR_B, and 12T_FTICR 
		public const string RAW_DATA_TYPE_ZIPPED_S_FOLDERS = "zipped_s_folders";
		//Micromass QTOF data
		public const string RAW_DATA_TYPE_DOT_RAW_FOLDER = "dot_raw_folder";
		//Finnigan ion trap/LTQ-FT data
		public const string RAW_DATA_TYPE_DOT_RAW_FILES = "dot_raw_files";
		//Agilent/QSTAR TOF data
		public const string RAW_DATA_TYPE_DOT_WIFF_FILES = "dot_wiff_files";
		//IMS_UIMF (IMS_Agilent_TOF in DMS)
		public const string RAW_DATA_TYPE_DOT_UIMF_FILES = "dot_uimf_files";
		//mzXML
		public const string RAW_DATA_TYPE_DOT_MZXML_FILES = "dot_mzxml_files";
		//mzML
		public const string RAW_DATA_TYPE_DOT_MZML_FILES = "dot_mzml_files";

		// 12T datasets acquired prior to 7/16/2010 use a Bruker data station and have an analysis.baf file, 0.ser folder, and a XMASS_Method.m subfolder with file apexAcquisition.method
		// Datasets will have an instrument name of 12T_FTICR and raw_data_type of "zipped_s_folders"

		// 12T datasets acquired after 9/1/2010 use the Agilent data station, and thus have a .D folder
		// Datasets will have an instrument name of 12T_FTICR_B and raw_data_type of "bruker_ft"
		// 15T datasets also have raw_data_type "bruker_ft"
		// Inside the .D folder is the analysis.baf file; there is also .m subfolder that has a apexAcquisition.method file

		public const string RAW_DATA_TYPE_BRUKER_FT_FOLDER = "bruker_ft";
		// The following is used by BrukerTOF_01 (e.g. Bruker TOF_TOF)
		// Folder has a .EMF file and a single sub-folder that has an acqu file and fid file

		public const string RAW_DATA_TYPE_BRUKER_MALDI_SPOT = "bruker_maldi_spot";
		// The following is used by instruments 9T_FTICR_Imaging and BrukerTOF_Imaging_01
		// Series of zipped subfolders, with names like 0_R00X329.zip; subfolders inside the .Zip files have fid files

		public const string RAW_DATA_TYPE_BRUKER_MALDI_IMAGING = "bruker_maldi_imaging";
		// The following is used by instrument Maxis_01
		// Inside the .D folder is the analysis.baf file; there is also .m subfolder that has a microTOFQMaxAcquisition.method file; there is not a ser or fid file

		public const string RAW_DATA_TYPE_BRUKER_TOF_BAF_FOLDER = "bruker_tof_baf";
		#endregion

		#region "File Extension Constants"
		public const string DOT_WIFF_EXTENSION = ".wiff";
		public const string DOT_D_EXTENSION = ".d";
		public const string DOT_RAW_EXTENSION = ".raw";
		public const string DOT_UIMF_EXTENSION = ".uimf";
		public const string DOT_MZXML_EXTENSION = ".mzxml";
		public const string DOT_MZML_EXTENSION = ".mzml";
		public const string DOT_MGF_EXTENSION = ".mgf";
		public const string DOT_CDF_EXTENSION = ".cdf";
		#endregion

		#region "Instrument Class Constants"
		public const string INST_CLASS_FINNIGAN_ION_TRAP = "finnigan_ion_trap";
		public const string INST_CLASS_LTQ_FT = "ltq_ft";
		public const string INST_CLASS_TRIPLE_QUAD = "triple_quad";
		public const string INST_CLASS_THERMO_EXACTIVE = "thermo_exactive";
		public const string INST_CLASS_AGILENT_ION_TRAP = "agilent_ion_trap";
		public const string INST_CLASS_AGILENT_TOF = "agilent_tof";
		public const string INST_CLASS_AGILENT_TOF_V2 = "agilent_tof_v2";
		public const string INST_CLASS_BRUKER_AMAZON_ION_TRAP = "bruker_amazon_ion_trap";
		public const string INST_CLASS_BRUKERFT_BAF = "brukerft_baf";
		public const string INST_CLASS_BRUKERFTMS = "brukerftms";
		public const string INST_CLASS_BRUKERMALDI_IMAGING = "brukermaldi_imaging";
		public const string INST_CLASS_BRUKERMALDI_SPOT = "brukermaldi_spot";
		public const string INST_CLASS_BRUKERTOF_BAF = "brukertof_baf";
		public const string INST_CLASS_DATA_FOLDERS = "data_folders";
		public const string INST_CLASS_FINNIGAN_FTICR = "finnigan_fticr";
		public const string INST_CLASS_IMS_AGILENT_TOF = "ims_agilent_tof";
		public const string INST_CLASS_MICROMASS_QTOF = "micromass_qtof";
		public const string INST_CLASS_QSTAR_QTOF = "qstar_qtof";
		public const string INST_CLASS_SCIEX_QTRAP = "sciex_qtrap";
		public const string INST_CLASS_SCIEX_TRIPLETOF = "sciex_tripletof";
		#endregion

		#region "Enums"

		public enum eRawDataType
		{
			Unknown = 0,
			ThermoRawFile = 1,
			UIMF = 2,
			mzXML = 3,
			mzML = 4,
			AgilentDFolder = 5,			// Agilent ion trap data, Agilent TOF data
			AgilentQStarWiffFile = 6,
			MicromassRawFolder = 7,		// Micromass QTOF data
			ZippedSFolders = 8,			// FTICR data, including instrument 3T_FTICR, 7T_FTICR, 9T_FTICR, 11T_FTICR, 11T_FTICR_B, and 12T_FTICR 
			BrukerFTFolder = 9,			// .D folder is the analysis.baf file; there is also .m subfolder that has a apexAcquisition.method file
			BrukerMALDISpot = 10,		// has a .EMF file and a single sub-folder that has an acqu file and fid file
			BrukerMALDIImaging = 11,	// Series of zipped subfolders, with names like 0_R00X329.zip; subfolders inside the .Zip files have fid files
			BrukerTOFBaf = 12			// Used by Maxis01; Inside the .D folder is the analysis.baf file; there is also .m subfolder that has a microTOFQMaxAcquisition.method file; there is not a ser or fid file
		}

		public enum eInstrumentClass
		{
			Unknown = 0,
			Finnigan_Ion_Trap = 1,		// LCQ_C1, LTQ_1, LTQ_ETD_1
			LTQ_FT = 2,					// LTQ_FT1, LTQ_Orb_1, VOrbiETD01, VOrbi05
			Triple_Quad = 3,			// TSQ_1, Thermo_GC_MS_01, Agilent_QQQ_04
			Thermo_Exactive = 4,		// Exact01, Exact02, ...
			Agilent_Ion_Trap = 5,		// Agilent_XCT1, Agilent_GC_MS_01
			Agilent_TOF = 6,			// AgTOF01, 
			Agilent_TOF_V2 = 7,			// AgQTOF03, AgQTOF04
			Bruker_Amazon_Ion_Trap = 8,	// Bruker_FT_IonTrap01
			BrukerFT_BAF = 9,			// 9T_FTICR_B, 12T_FTICR_B, 15T_FTICR, 
			BRUKERFTMS = 10,			// 9T_FTICR, 11T_FTICR_B, 12T_FTICR		(last used in 2010)
			BrukerMALDI_Imaging = 11,	// 12T_FTICR_Imaging, 15T_FTICR_Imaging
			BrukerMALDI_Spot = 12,		// BrukerTOF_01
			BrukerTOF_BAF = 13,			// Maxis_01
			Data_Folders = 14,			// Folders of data
			Finnigan_FTICR = 15,		// 3T_FTICR, 7T_FTICR, 11T_FTICR		(last used in 2007)
			IMS_Agilent_TOF = 16,		// IMS02_AgTOF06, IMS04_AgTOF05, IMS05_AgQTOF04
			Micromass_QTOF = 17,		// QTOF_MM1								(last used in 2007)
			QStar_QTOF = 18,			// QTOF_1322							(last used in 2003)
			Sciex_QTrap = 19,			// QTrap01
			Sciex_TripleTOF = 20		// WashU_TripleTOF5600
		}

		#endregion

		public static eInstrumentClass GetInstrumentClass(string sInstrumentClassName)
		{
			if (string.IsNullOrEmpty(sInstrumentClassName))
			{
				return eInstrumentClass.Unknown;
			}

			eInstrumentClass instrumentClass = eInstrumentClass.Unknown;
			try
			{
				// Convert the instrument class name text to the num name using case-insensitive conversion
				instrumentClass = (eInstrumentClass)Enum.Parse(typeof(eInstrumentClass), sInstrumentClassName, true);
			}
			catch
			{
				// Ignore errors; leave instrumentClass as Unknown
			}

			return instrumentClass;
		}

		public static string GetInstrumentClassName(eInstrumentClass instrumentClass)
		{
			return instrumentClass.ToString();
		}

		public static eRawDataType GetRawDataType(string sRawDataTypeName)
		{

			if (string.IsNullOrEmpty(sRawDataTypeName))
			{
				return eRawDataType.Unknown;
			}

			switch (sRawDataTypeName.ToLower())
			{
				case RAW_DATA_TYPE_DOT_D_FOLDERS:
					return eRawDataType.AgilentDFolder;
				case RAW_DATA_TYPE_ZIPPED_S_FOLDERS:
					return eRawDataType.ZippedSFolders;
				case RAW_DATA_TYPE_DOT_RAW_FOLDER:
					return eRawDataType.MicromassRawFolder;
				case RAW_DATA_TYPE_DOT_RAW_FILES:
					return eRawDataType.ThermoRawFile;
				case RAW_DATA_TYPE_DOT_WIFF_FILES:
					return eRawDataType.AgilentQStarWiffFile;
				case RAW_DATA_TYPE_DOT_UIMF_FILES:
					return eRawDataType.UIMF;
				case RAW_DATA_TYPE_DOT_MZXML_FILES:
					return eRawDataType.mzXML;
				case RAW_DATA_TYPE_DOT_MZML_FILES:
					return eRawDataType.mzML;
				case RAW_DATA_TYPE_BRUKER_FT_FOLDER:
					return eRawDataType.BrukerFTFolder;
				case RAW_DATA_TYPE_BRUKER_MALDI_SPOT:
					return eRawDataType.BrukerMALDISpot;
				case RAW_DATA_TYPE_BRUKER_MALDI_IMAGING:
					return eRawDataType.BrukerMALDIImaging;
				case RAW_DATA_TYPE_BRUKER_TOF_BAF_FOLDER:
					return eRawDataType.BrukerTOFBaf;
				default:
					return eRawDataType.Unknown;
			}

		}

		public static string GetRawDataTypeName(eRawDataType rawDataType)
		{
			
		switch (rawDataType)
		{
			case eRawDataType.AgilentDFolder:
				return RAW_DATA_TYPE_DOT_D_FOLDERS;
			case eRawDataType.ZippedSFolders:
				return RAW_DATA_TYPE_ZIPPED_S_FOLDERS;
			case eRawDataType.MicromassRawFolder:
				return RAW_DATA_TYPE_DOT_RAW_FOLDER;
			case eRawDataType.ThermoRawFile:
				return RAW_DATA_TYPE_DOT_RAW_FILES;
			case eRawDataType.AgilentQStarWiffFile:
				return RAW_DATA_TYPE_DOT_WIFF_FILES;
			case eRawDataType.UIMF:
				return RAW_DATA_TYPE_DOT_UIMF_FILES;
			case eRawDataType.mzXML:
				return RAW_DATA_TYPE_DOT_MZXML_FILES;
			case eRawDataType.mzML:
				return RAW_DATA_TYPE_DOT_MZML_FILES;
			case eRawDataType.BrukerFTFolder:
				return RAW_DATA_TYPE_BRUKER_FT_FOLDER;
			case eRawDataType.BrukerMALDISpot:
				return RAW_DATA_TYPE_BRUKER_MALDI_SPOT;
			case eRawDataType.BrukerMALDIImaging:
				return RAW_DATA_TYPE_BRUKER_MALDI_IMAGING;
			case eRawDataType.BrukerTOFBaf:
				return RAW_DATA_TYPE_BRUKER_TOF_BAF_FOLDER;
			default:
				return "Unknown";
		}
		}

	}
}
