namespace CaptureTaskManager;

public enum DataFormat
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
    /// Used by Bruker timsTOF; Inside the .D directory is the analysis.tdf file
    /// </summary>
    BrukerTOFTdf = 16,

    /// <summary>
    /// Used by LCMSNet LCs with no available pump method/pressure data
    /// </summary>
    LCMSNetLCMethod = 17,

    /// <summary>
    /// Used by Bruker timsTOF Imaging; directory with .jpg and .mis files, and .D directory with an analysis.tsf file
    /// </summary>
    BrukerTOFTsf = 18
}
