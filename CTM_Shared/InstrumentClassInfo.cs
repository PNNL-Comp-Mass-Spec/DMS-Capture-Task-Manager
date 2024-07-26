using System;

// ReSharper disable UnusedMember.Global
namespace CaptureTaskManager
{
    // Used in the DatasetIntegrityPlugin and elsewhere
    public static class InstrumentClassInfo
    {
        // Ignore Spelling: Bruker, Illumina, LCMethod, Micromass, Sciex, Shimadzu, Synapt, LCMSNET
        // Ignore Spelling: acqu, baf, cdf, fid, gz, maldi, mgf, MzML, MzXML, qgd, ser, tdf, tims, tof, uimf, wiff

        // Note: the RAW_DATA_TYPE constants need to be all lowercase

        // Agilent ion trap data, Agilent TOF data
        public const string DATA_FORMAT_DOT_D_FOLDERS = "dot_d_folders";

        // FTICR data, including instrument 3T_FTICR, 7T_FTICR, 9T_FTICR, 11T_FTICR, 11T_FTICR_B, and 12T_FTICR
        public const string DATA_FORMAT_ZIPPED_S_FOLDERS = "zipped_s_folders";

        // Waters QTOF data (Micromass)
        // Waters Synapt data
        public const string DATA_FORMAT_DOT_RAW_FOLDER = "dot_raw_folder";

        // Thermo ion trap/LTQ-FT data
        public const string DATA_FORMAT_DOT_RAW_FILES = "dot_raw_files";

        // Agilent/QSTAR TOF data
        public const string DATA_FORMAT_DOT_WIFF_FILES = "dot_wiff_files";

        // Sciex QTrap data
        public const string DATA_FORMAT_SCIEX_WIFF_FILES = "sciex_wiff_files";

        // IMS_UIMF (IMS_Agilent_TOF_UIMF and IMS_Agilent_TOF_DotD in DMS)
        public const string DATA_FORMAT_DOT_UIMF_FILES = "dot_uimf_files";

        // mzXML
        public const string DATA_FORMAT_DOT_MZXML_FILES = "dot_mzxml_files";

        // mzML
        public const string DATA_FORMAT_DOT_MZML_FILES = "dot_mzml_files";

        // Shimadzu GC data
        public const string DATA_FORMAT_DOT_QGD_FILES = "dot_qgd_files";

        // 12T datasets acquired prior to 7/16/2010 use a Bruker data station and have an analysis.baf file, 0.ser directory, and a XMASS_Method.m subdirectory with file apexAcquisition.method
        // Datasets will have an instrument name of 12T_FTICR and data_format of "zipped_s_folders"

        // 12T datasets acquired after 9/1/2010 use the Agilent data station, and thus have a .D directory
        // Datasets will have an instrument name of 12T_FTICR_B and data_format of "bruker_ft"
        // 15T datasets also have data_format "bruker_ft"
        // Inside the .D directory is the analysis.baf file; there is also .m subdirectory that has a apexAcquisition.method file
        public const string DATA_FORMAT_BRUKER_FT_FOLDER = "bruker_ft";

        // The following is used by BrukerTOF_01 (e.g. Bruker TOF_TOF)
        // Directory has a .EMF file and a single subdirectory that has an acqu file and fid file
        public const string DATA_FORMAT_BRUKER_MALDI_SPOT = "bruker_maldi_spot";

        // The following is used by instruments 9T_FTICR_Imaging and BrukerTOF_Imaging_01
        // Series of zipped subdirectories, with names like 0_R00X329.zip; subdirectories inside the .Zip files have fid files
        public const string DATA_FORMAT_BRUKER_MALDI_IMAGING = "bruker_maldi_imaging";

        // The following is used by instrument Maxis_01
        // Inside the .D directory is the analysis.baf file; there is also .m subdirectory that has a microTOFQMaxAcquisition.method file; there is not a ser or fid file
        public const string DATA_FORMAT_BRUKER_TOF_BAF_DIRECTORY = "bruker_tof_baf";

        // The following is used by Bruker timsTOF instruments
        // Inside the .D directory is the analysis.tdf file; there is also .m subdirectory that has a microTOFQImpacTemAcquisition.method file; there is not a ser or fid file
        public const string DATA_FORMAT_BRUKER_TOF_TDF_DIRECTORY = "bruker_tof_tdf";

        // The following is used by Bruker timsTOF instruments with MALDI imaging capability
        // Inside the .D directory is the analysis.tsf file; there is also .m subdirectory that has a microTOFQImpacTemAcquisition.method file; there is not a ser or fid file
        public const string DATA_FORMAT_BRUKER_TOF_TSF_DIRECTORY = "bruker_tof_tsf";

        // The following is used by instrument External_Illumina
        public const string DATA_FORMAT_ILLUMINA_FOLDER = "illumina_folder";

        // The following is used by LCMSNet LCs with no available pump method/pressure data
        public const string DATA_FORMAT_LCMSNET_LCMETHOD = "lcmsnet_lcmethod";

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
        public const string DOT_LCMETHOD_EXTENSION = ".lcmethod";

        public static InstrumentClass GetInstrumentClass(string instrumentClassName)
        {
            if (string.IsNullOrEmpty(instrumentClassName))
            {
                return InstrumentClass.Unknown;
            }

            var instrumentClass = InstrumentClass.Unknown;
            try
            {
                // Convert the instrument class name text to the enum name using case-insensitive conversion
                instrumentClass = (InstrumentClass)Enum.Parse(typeof(InstrumentClass), instrumentClassName, true);
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

        public static DataFormat GetDataFormat(string rawDataTypeName)
        {
            if (string.IsNullOrEmpty(rawDataTypeName))
            {
                return DataFormat.Unknown;
            }

            return rawDataTypeName.ToLower() switch
            {
                DATA_FORMAT_DOT_D_FOLDERS => DataFormat.AgilentDFolder,
                DATA_FORMAT_ZIPPED_S_FOLDERS => DataFormat.ZippedSFolders,
                DATA_FORMAT_DOT_RAW_FOLDER => DataFormat.WatersRawFolder,
                DATA_FORMAT_DOT_RAW_FILES => DataFormat.ThermoRawFile,
                DATA_FORMAT_DOT_WIFF_FILES => DataFormat.AgilentQStarWiffFile,
                DATA_FORMAT_SCIEX_WIFF_FILES => DataFormat.SciexWiffFile,
                DATA_FORMAT_DOT_UIMF_FILES => DataFormat.UIMF,
                DATA_FORMAT_DOT_MZXML_FILES => DataFormat.mzXML,
                DATA_FORMAT_DOT_MZML_FILES => DataFormat.mzML,
                DATA_FORMAT_BRUKER_FT_FOLDER => DataFormat.BrukerFTFolder,
                DATA_FORMAT_BRUKER_MALDI_SPOT => DataFormat.BrukerMALDISpot,
                DATA_FORMAT_BRUKER_MALDI_IMAGING => DataFormat.BrukerMALDIImaging,
                DATA_FORMAT_BRUKER_TOF_BAF_DIRECTORY => DataFormat.BrukerTOFBaf,
                DATA_FORMAT_BRUKER_TOF_TDF_DIRECTORY => DataFormat.BrukerTOFTdf,
                DATA_FORMAT_BRUKER_TOF_TSF_DIRECTORY => DataFormat.BrukerTOFTsf,
                DATA_FORMAT_ILLUMINA_FOLDER => DataFormat.IlluminaFolder,
                DATA_FORMAT_DOT_QGD_FILES => DataFormat.ShimadzuQGDFile,
                DATA_FORMAT_LCMSNET_LCMETHOD => DataFormat.LCMSNetLCMethod,
                _ => DataFormat.Unknown
            };
        }

        public static string GetDataFormatName(DataFormat rawDataType)
        {
            return rawDataType switch
            {
                DataFormat.AgilentDFolder => DATA_FORMAT_DOT_D_FOLDERS,
                DataFormat.ZippedSFolders => DATA_FORMAT_ZIPPED_S_FOLDERS,
                DataFormat.WatersRawFolder => DATA_FORMAT_DOT_RAW_FOLDER,
                DataFormat.ThermoRawFile => DATA_FORMAT_DOT_RAW_FILES,
                DataFormat.AgilentQStarWiffFile => DATA_FORMAT_DOT_WIFF_FILES,
                DataFormat.SciexWiffFile => DATA_FORMAT_SCIEX_WIFF_FILES,
                DataFormat.UIMF => DATA_FORMAT_DOT_UIMF_FILES,
                DataFormat.mzXML => DATA_FORMAT_DOT_MZXML_FILES,
                DataFormat.mzML => DATA_FORMAT_DOT_MZML_FILES,
                DataFormat.BrukerFTFolder => DATA_FORMAT_BRUKER_FT_FOLDER,
                DataFormat.BrukerMALDISpot => DATA_FORMAT_BRUKER_MALDI_SPOT,
                DataFormat.BrukerMALDIImaging => DATA_FORMAT_BRUKER_MALDI_IMAGING,
                DataFormat.BrukerTOFBaf => DATA_FORMAT_BRUKER_TOF_BAF_DIRECTORY,
                DataFormat.BrukerTOFTdf => DATA_FORMAT_BRUKER_TOF_TDF_DIRECTORY,
                DataFormat.BrukerTOFTsf => DATA_FORMAT_BRUKER_TOF_TSF_DIRECTORY,
                DataFormat.IlluminaFolder => DATA_FORMAT_ILLUMINA_FOLDER,
                DataFormat.LCMSNetLCMethod => DATA_FORMAT_LCMSNET_LCMETHOD,
                _ => "Unknown"
            };
        }
    }
}
