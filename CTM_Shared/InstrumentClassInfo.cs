using System;

// ReSharper disable UnusedMember.Global
namespace CaptureTaskManager
{
    // Used in the DatasetIntegrityPlugin and elsewhere
    public static class InstrumentClassInfo
    {
        // Ignore Spelling: Bruker, Illumina, Micromass, Sciex, Shimadzu, Synapt
        // Ignore Spelling: acqu, baf, fid, maldi, qgd, ser, tdf, tims, tof, uimf, wiff

        #region "Raw Data Type Constants"

        // Note: All of the RAW_DATA_TYPE constants need to be all lowercase

        // Agilent ion trap data, Agilent TOF data
        public const string RAW_DATA_TYPE_DOT_D_FOLDERS = "dot_d_folders";

        // FTICR data, including instrument 3T_FTICR, 7T_FTICR, 9T_FTICR, 11T_FTICR, 11T_FTICR_B, and 12T_FTICR
        public const string RAW_DATA_TYPE_ZIPPED_S_FOLDERS = "zipped_s_folders";

        // Waters QTOF data (Micromass)
        // Waters Synapt data
        public const string RAW_DATA_TYPE_DOT_RAW_FOLDER = "dot_raw_folder";

        // Thermo ion trap/LTQ-FT data
        public const string RAW_DATA_TYPE_DOT_RAW_FILES = "dot_raw_files";

        // Agilent/QSTAR TOF data
        public const string RAW_DATA_TYPE_DOT_WIFF_FILES = "dot_wiff_files";

        // Sciex QTrap data
        public const string RAW_DATA_TYPE_SCIEX_WIFF_FILES = "sciex_wiff_files";

        // IMS_UIMF (IMS_Agilent_TOF_UIMF and IMS_Agilent_TOF_DotD in DMS)
        public const string RAW_DATA_TYPE_DOT_UIMF_FILES = "dot_uimf_files";

        // mzXML
        public const string RAW_DATA_TYPE_DOT_MZXML_FILES = "dot_mzxml_files";

        // mzML
        public const string RAW_DATA_TYPE_DOT_MZML_FILES = "dot_mzml_files";

        // Shimadzu GC data
        public const string RAW_DATA_TYPE_DOT_QGD_FILES = "dot_qgd_files";

        // 12T datasets acquired prior to 7/16/2010 use a Bruker data station and have an analysis.baf file, 0.ser directory, and a XMASS_Method.m subdirectory with file apexAcquisition.method
        // Datasets will have an instrument name of 12T_FTICR and raw_data_type of "zipped_s_folders"

        // 12T datasets acquired after 9/1/2010 use the Agilent data station, and thus have a .D directory
        // Datasets will have an instrument name of 12T_FTICR_B and raw_data_type of "bruker_ft"
        // 15T datasets also have raw_data_type "bruker_ft"
        // Inside the .D directory is the analysis.baf file; there is also .m subdirectory that has a apexAcquisition.method file
        public const string RAW_DATA_TYPE_BRUKER_FT_FOLDER = "bruker_ft";

        // The following is used by BrukerTOF_01 (e.g. Bruker TOF_TOF)
        // Directory has a .EMF file and a single subdirectory that has an acqu file and fid file
        public const string RAW_DATA_TYPE_BRUKER_MALDI_SPOT = "bruker_maldi_spot";

        // The following is used by instruments 9T_FTICR_Imaging and BrukerTOF_Imaging_01
        // Series of zipped subdirectories, with names like 0_R00X329.zip; subdirectories inside the .Zip files have fid files
        public const string RAW_DATA_TYPE_BRUKER_MALDI_IMAGING = "bruker_maldi_imaging";

        // The following is used by instrument Maxis_01
        // Inside the .D directory is the analysis.baf file; there is also .m subdirectory that has a microTOFQMaxAcquisition.method file; there is not a ser or fid file
        public const string RAW_DATA_TYPE_BRUKER_TOF_BAF_DIRECTORY = "bruker_tof_baf";

        // The following is used by Bruker timsTOF instruments
        // Inside the .D directory is the analysis.tdf file; there is also .m subdirectory that has a microTOFQImpacTemAcquisition.method file; there is not a ser or fid file
        public const string RAW_DATA_TYPE_BRUKER_TOF_TDF_DIRECTORY = "bruker_tof_tdf";

        // The following is used by instrument External_Illumina
        public const string RAW_DATA_TYPE_ILLUMINA_FOLDER = "illumina_folder";

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
        public const string DOT_TXT_GZ_EXTENSION = ".txt.gz";
        public const string DOT_QGD_EXTENSION = ".qgd";

        #endregion

        #region "Enums"

        public enum RawDataType
        {
            Unknown = 0,

            /// <summary>
            /// Thermo .raw file
            /// </summary>
            ThermoRawFile = 1,

            /// <summary>
            /// SQLite database used by PNNL IMS instruments
            /// </summary>
            UIMF = 2,

            /// <summary>
            /// XML file, older format
            /// </summary>
            mzXML = 3,

            /// <summary>
            /// XML file, newer format
            /// </summary>
            mzML = 4,

            /// <summary>
            /// Agilent ion trap data, Agilent TOF data
            /// </summary>
            AgilentDFolder = 5,

            /// <summary>
            /// QStar .wiff file
            /// </summary>
            AgilentQStarWiffFile = 6,

            /// <summary>
            /// Waters Synapt
            /// </summary>
            WatersRawFolder = 7,

            /// <summary>
            /// FTICR data, including instrument 3T_FTICR, 7T_FTICR, 9T_FTICR, 11T_FTICR, 11T_FTICR_B, and 12T_FTICR
            /// </summary>
            ZippedSFolders = 8,

            /// <summary>
            /// .D directory has a analysis.baf file
            /// There is also .m subdirectory that has a apexAcquisition.method file
            /// </summary>
            BrukerFTFolder = 9,

            /// <summary>
            /// Has a .EMF file and a single subdirectory that has an acqu file and fid file
            /// </summary>
            BrukerMALDISpot = 10,

            /// <summary>
            /// Directory with .jpg files and .D subdirectories
            /// </summary>
            BrukerMALDIImaging = 11,

            /// <summary>
            /// Series of zipped subdirectories, with names like 0_R00X329.zip; subdirectories inside the .Zip files have fid files
            /// </summary>
            BrukerTOFBaf = 12,

            /// <summary>
            /// Used by Maxis01; Inside the .D directory is the analysis.baf file
            /// There is also .m subdirectory that has a microTOFQMaxAcquisition.method file
            /// There is not a ser or fid file
            /// </summary>
            SciexWiffFile = 13,

            /// <summary>
            /// Illumina sequencing data
            /// </summary>
            IlluminaFolder = 14,

            /// <summary>
            /// Shimadzu GC file with extension .qgd
            /// </summary>
            ShimadzuQGDFile = 15,

            /// <summary>
            /// Used by Bruker timsTOF; Instead the .D directory is the analysis.tdf file
            /// </summary>
            BrukerTOFTdf = 16
        }

        public enum InstrumentClass
        {
            Unknown = 0,
            Finnigan_Ion_Trap = 1,          // LCQ_C1, LTQ_1, LTQ_ETD_1
            LTQ_FT = 2,                     // LTQ_FT1, LTQ_Orb_1, VOrbiETD01, VOrbi05, QExact01, QExactHF03
            Triple_Quad = 3,                // TSQ_1, Thermo_GC_MS_01
            Thermo_Exactive = 4,            // Exact01, Exact02, ...
            Agilent_Ion_Trap = 5,           // Agilent_XCT1, Agilent_GC_01, Agilent_GC_MS_01, BSF_GCMS01
            Agilent_TOF = 6,                // AgTOF01, AgTOF02                     (last used in 2012)
            Agilent_TOF_V2 = 7,             // AgQTOF03, AgQTOF04, AgTOF05, Agilent_QQQ_04
            Bruker_Amazon_Ion_Trap = 8,     // Bruker_FT_IonTrap01                  (last used in 2012)
            BrukerFT_BAF = 9,               // 9T_FTICR_B, 12T_FTICR_B, 15T_FTICR,
            BrukerFTMS = 10,                // 9T_FTICR, 11T_FTICR_B, 12T_FTICR     (last used in 2010)
            BrukerMALDI_Imaging = 11,       // 9T_FTICR_Imaging                     (last used in 2012)
            BrukerMALDI_Spot = 12,          // BrukerTOF_01
            BrukerTOF_BAF = 13,             // Maxis_01
            Data_Folders = 14,              // Directories of data
            Finnigan_FTICR = 15,            // 3T_FTICR, 7T_FTICR, 11T_FTICR        (last used in 2007)
            IMS_Agilent_TOF_UIMF = 16,      // Data is acquired natively as UIMF files: IMS02_AgTOF06, IMS04_AgTOF05, IMS05_AgQTOF04, IMS07_AgTOF04
            Waters_TOF = 17,                // QTOF_MM1, External_Waters_TOF
            QStar_QTOF = 18,                // QTOF_1322                            (last used in 2003)
            Sciex_QTrap = 19,               // QTrap01
            Sciex_TripleTOF = 20,           // WashU_TripleTOF5600                  (last used in 2012)
            PrepHPLC = 21,                  // PrepHPLC1, PrepHPLC2
            BrukerMALDI_Imaging_V2 = 22,    // 12T_FTICR_Imaging, 15T_FTICR_Imaging
            Illumina_Sequencer = 23,        // External_Illumina
            GC_QExactive = 24,              // GCQE01
            Waters_IMS = 25,                // SynaptG2_01
            Shimadzu_GC = 26,               // Shimadzu_GC_MS_01
            BrukerTOF_TDF = 27,             // Bruker_timsTOF
            FT_Booster_Data = 28,           // 21T_Booster
            IMS_Agilent_TOF_DotD = 29       // Data is acquired natively as .D directories: IMS08, IMS09, IMS10, IMS11
        }

        #endregion

        public static InstrumentClass GetInstrumentClass(string sInstrumentClassName)
        {
            if (string.IsNullOrEmpty(sInstrumentClassName))
            {
                return InstrumentClass.Unknown;
            }

            var instrumentClass = InstrumentClass.Unknown;
            try
            {
                // Convert the instrument class name text to the enum name using case-insensitive conversion
                instrumentClass = (InstrumentClass)Enum.Parse(typeof(InstrumentClass), sInstrumentClassName, true);
            }
            catch
            {
                // Ignore errors; leave instrumentClass as Unknown
            }

            return instrumentClass;
        }

        public static string GetInstrumentClassName(InstrumentClass instrumentClass)
        {
            return instrumentClass.ToString();
        }

        public static RawDataType GetRawDataType(string rawDataTypeName)
        {
            if (string.IsNullOrEmpty(rawDataTypeName))
            {
                return RawDataType.Unknown;
            }

            return rawDataTypeName.ToLower() switch
            {
                RAW_DATA_TYPE_DOT_D_FOLDERS => RawDataType.AgilentDFolder,
                RAW_DATA_TYPE_ZIPPED_S_FOLDERS => RawDataType.ZippedSFolders,
                RAW_DATA_TYPE_DOT_RAW_FOLDER => RawDataType.WatersRawFolder,
                RAW_DATA_TYPE_DOT_RAW_FILES => RawDataType.ThermoRawFile,
                RAW_DATA_TYPE_DOT_WIFF_FILES => RawDataType.AgilentQStarWiffFile,
                RAW_DATA_TYPE_SCIEX_WIFF_FILES => RawDataType.SciexWiffFile,
                RAW_DATA_TYPE_DOT_UIMF_FILES => RawDataType.UIMF,
                RAW_DATA_TYPE_DOT_MZXML_FILES => RawDataType.mzXML,
                RAW_DATA_TYPE_DOT_MZML_FILES => RawDataType.mzML,
                RAW_DATA_TYPE_BRUKER_FT_FOLDER => RawDataType.BrukerFTFolder,
                RAW_DATA_TYPE_BRUKER_MALDI_SPOT => RawDataType.BrukerMALDISpot,
                RAW_DATA_TYPE_BRUKER_MALDI_IMAGING => RawDataType.BrukerMALDIImaging,
                RAW_DATA_TYPE_BRUKER_TOF_BAF_DIRECTORY => RawDataType.BrukerTOFBaf,
                RAW_DATA_TYPE_BRUKER_TOF_TDF_DIRECTORY => RawDataType.BrukerTOFTdf,
                RAW_DATA_TYPE_ILLUMINA_FOLDER => RawDataType.IlluminaFolder,
                RAW_DATA_TYPE_DOT_QGD_FILES => RawDataType.ShimadzuQGDFile,
                _ => RawDataType.Unknown
            };
        }

        public static string GetRawDataTypeName(RawDataType rawDataType)
        {
            return rawDataType switch
            {
                RawDataType.AgilentDFolder => RAW_DATA_TYPE_DOT_D_FOLDERS,
                RawDataType.ZippedSFolders => RAW_DATA_TYPE_ZIPPED_S_FOLDERS,
                RawDataType.WatersRawFolder => RAW_DATA_TYPE_DOT_RAW_FOLDER,
                RawDataType.ThermoRawFile => RAW_DATA_TYPE_DOT_RAW_FILES,
                RawDataType.AgilentQStarWiffFile => RAW_DATA_TYPE_DOT_WIFF_FILES,
                RawDataType.SciexWiffFile => RAW_DATA_TYPE_SCIEX_WIFF_FILES,
                RawDataType.UIMF => RAW_DATA_TYPE_DOT_UIMF_FILES,
                RawDataType.mzXML => RAW_DATA_TYPE_DOT_MZXML_FILES,
                RawDataType.mzML => RAW_DATA_TYPE_DOT_MZML_FILES,
                RawDataType.BrukerFTFolder => RAW_DATA_TYPE_BRUKER_FT_FOLDER,
                RawDataType.BrukerMALDISpot => RAW_DATA_TYPE_BRUKER_MALDI_SPOT,
                RawDataType.BrukerMALDIImaging => RAW_DATA_TYPE_BRUKER_MALDI_IMAGING,
                RawDataType.BrukerTOFBaf => RAW_DATA_TYPE_BRUKER_TOF_BAF_DIRECTORY,
                RawDataType.BrukerTOFTdf => RAW_DATA_TYPE_BRUKER_TOF_TDF_DIRECTORY,
                RawDataType.IlluminaFolder => RAW_DATA_TYPE_ILLUMINA_FOLDER,
                _ => "Unknown"
            };
        }
    }
}
