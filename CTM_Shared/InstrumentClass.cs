namespace CaptureTaskManager;

public enum InstrumentClass
{
    Unknown = 0,
    Finnigan_Ion_Trap = 1,          // LCQ_C1, LTQ_1, LTQ_ETD_1
    LTQ_FT = 2,                     // LTQ_FT1, LTQ_Orb_1, VOrbiETD01, VOrbi05, QExact01, QExactHF03, Lumos01, Ascend01, Astral01
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
    IMS_Agilent_TOF_DotD = 29,      // Data is acquired natively as .D directories: IMS08, IMS09, IMS10, IMS11
    Thermo_SII_LC = 30,             // Thermo LC data files from SII (Standard Instrument Integration) for Xcalibur
    Waters_Acquity_LC = 31,         // Waters Acquity LC data files from MassLynx
    LCMSNet_LC = 32                 // LCMSNet LC with no available pump method/pressure data
}
