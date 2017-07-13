using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using PRISM;

namespace Pacifica.Core
{
    public class Upload : clsEventNotifier, IUpload
    {
        /// <summary>
        /// EUS Operator ID to use when operator ID is unknown
        /// </summary>
        /// <remarks>43428 is "Monroe, Matthew"</remarks>
        public const int DEFAULT_EUS_OPERATOR_ID = 43428;

        /// <summary>
        /// EUS Proposal ID to use when the proposal ID is unknown
        /// </summary>
        /// <remarks>
        /// Proposal ID 17797 is "Development of High Throughput Proteomic Production Operations"
        /// It is a string because it may contain suffix letters
        /// </remarks>
        public const string DEFAULT_EUS_PROPOSAL_ID = "17797";

        /// <summary>
        /// EUS Instrument ID to use when the EUS Instrument ID is unknown
        /// </summary>
        public const int UNKNOWN_INSTRUMENT_EUS_INSTRUMENT_ID = 34127;

        /// <summary>
        /// DMS Instrument Name corresponding to EUS Instrument ID 34127
        /// </summary>
        public const string UNKNOWN_INSTRUMENT_NAME = "VOrbiETD04";

        private readonly Configuration mPacificaConfig;

        public class EUSInfo
        {
            /// <summary>
            /// EUS instrument ID
            /// </summary>
            public int EUSInstrumentID;

            /// <summary>
            /// EUS proposal number
            /// </summary>
            public string EUSProposalID;

            /// <summary>
            /// EUS ID of the instrument operator (for datasets) or the data package owner (for Data Packages)
            /// </summary>
            public int EUSUploaderID;

            public void Clear()
            {
                EUSInstrumentID = 0;
                EUSProposalID = string.Empty;
                EUSUploaderID = 0;
            }

            public override string ToString()
            {
                return "EUSInstrumentID " + EUSInstrumentID + ", Uploader " + EUSUploaderID + ", Proposal " + EUSProposalID;
            }
        }

        public class UploadMetadata
        {
            public int DatasetID;               // 0 for data packages
            public int DataPackageID;
            public string SubFolder;
            public string DatasetName;          // Only used for datasets; not Data Packages
            public string DateCodeString;       // Only used for datasets; not Data Packages
            public string DMSInstrumentName;    // Originally only used by datasets. Used by Data Packages starting in July 2017 since required by policy
            public string CampaignName;
            public int CampaignID;
            public int EUSInstrumentID;         // Originally only used by datasets. Used by Data Packages starting in July 2017 since required by policy
            public string EUSProposalID;        // Originally only used by datasets. Used by Data Packages starting in October 2016 since required by policy
            public string ExperimentName;
            public int ExperimentID;
            public string OrganismName;
            public int OrganismID;
            public int NCBITaxonomyID;
            public string AcquisitionTime;
            public int AcquisitionLengthMin;
            public int NumberOfScans;
            public string SeparationType;
            public string DatasetType;
            public int RequestedRunID;
            public List<int> UserOfRecordList;

            /// <summary>
            /// Instrument Operator EUS ID for datasets
            /// Data Package Owner for data packages
            /// </summary>
            /// <remarks>DEFAULT_EUS_OPERATOR_ID if unknown</remarks>
            public int EUSOperatorID;

            public void Clear()
            {
                DatasetID = 0;
                DataPackageID = 0;
                SubFolder = string.Empty;
                DatasetName = string.Empty;
                DateCodeString = string.Empty;
                DMSInstrumentName = string.Empty;
                CampaignName = string.Empty;
                CampaignID = 0;
                EUSInstrumentID = 0;
                EUSProposalID = string.Empty;
                ExperimentName = string.Empty;
                ExperimentID = 0;
                OrganismName = string.Empty;
                OrganismID = 0;
                NCBITaxonomyID = 0;
                AcquisitionTime = string.Empty;
                AcquisitionLengthMin = 0;
                NumberOfScans = 0;
                SeparationType = string.Empty;
                DatasetType = string.Empty;
                RequestedRunID = 0;
                UserOfRecordList = new List<int>();
                EUSOperatorID = DEFAULT_EUS_OPERATOR_ID;

            }

            public override string ToString()
            {
                if (DatasetID == 0 && DataPackageID > 0)
                    return "Data package " + DataPackageID;

                return "Dataset " + DatasetID + ", on instrument " + DMSInstrumentName + ": " + DatasetName;
            }
        }

        #region Auto-Properties

        /// <summary>
        /// Error message
        /// </summary>
        public string ErrorMessage { get; private set; }

        /// <summary>
        /// The metadata.txt file will be copied to the Transfer Folder if the folder path is not empty
        /// Example: \\proto-4\DMS3_Xfer\QC_Shew_16_01_125ng_CID-STD_newCol-1_5Apr17_Frodo_16-11-08
        /// </summary>
        public string TransferFolderPath { get; set; }

        /// <summary>
        /// Capture Task DB job number for the DatasetArchive or ArchiveUpdate Job
        /// </summary>
        /// <remarks>
        /// The metadata.txt file name will include the JobNumber text in the name, for example MyEMSL_metadata_CaptureJob_12345.txt
        /// For DataPackages we store DataPackageId in JobNumber</remarks>
        public string JobNumber { get; set; }

        /// <summary>
        /// When true, upload to ingestdmsdev.my.emsl.pnl.gov instead of ingestdms.my.emsl.pnl.gov
        /// </summary>
        public bool UseTestInstance { get; set; }

        #endregion

        #region Private Members

        #endregion

        #region Constructor

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="config">Pacifica configuration</param>
        /// <remarks>TransferFolderPath and JobNumber will be empty</remarks>
        public Upload(Configuration config) : this(config, string.Empty, string.Empty)
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="config">Pacifica config</param>
        /// <param name="transferFolderPath">
        /// Transfer folder path for this dataset,
        /// for example \\proto-4\DMS3_Xfer\SysVirol_IFT001_Pool_17_B_10x_27Aug13_Tiger_13-07-36
        /// </param>
        /// <param name="jobNumber">DMS Data Capture job number</param>
        /// <remarks>The metadata.txt file will be copied to the transfer folder</remarks>
        public Upload(Configuration config, string transferFolderPath, string jobNumber)
        {

            mPacificaConfig = config;

            // Note that EasyHttp is a static class with a static event
            // Be careful about instantiating this class (Upload) multiple times
            EasyHttp.StatusUpdate += EasyHttp_StatusUpdate;

            ErrorMessage = string.Empty;
            TransferFolderPath = transferFolderPath;
            JobNumber = jobNumber;
        }

        #endregion

        #region Events and Handlers

        public event UploadCompletedEventHandler UploadCompleted;
        public event StatusUpdateEventHandler StatusUpdate;

        void EasyHttp_StatusUpdate(object sender, StatusEventArgs e)
        {
            StatusUpdate?.Invoke(this, e);
        }

        private void RaiseUploadCompleted(string serverResponse)
        {
            UploadCompleted?.Invoke(this, new UploadCompletedEventArgs(serverResponse));
        }

        #endregion

        #region IUpload Members

        /// <summary>
        /// Update the files and data tracked by metadataObject to MyEMSL
        /// </summary>
        /// <param name="metadataObject"></param>
        /// <param name="statusURI">Status URL</param>
        /// <returns>True if successfully uploaded, false if an error</returns>
        public bool StartUpload(List<Dictionary<string, object>> metadataObject, out string statusURI)
        {
            const EasyHttp.eDebugMode debugMode = EasyHttp.eDebugMode.DebugDisabled;

            return StartUpload(metadataObject, debugMode, out statusURI);
        }

        /// <summary>
        /// Update the files and data tracked by metadataObject to MyEMSL
        /// </summary>
        /// <param name="metadataObject"></param>
        /// <param name="debugMode">
        /// Set to eDebugMode.CreateTarLocal to authenticate with MyEMSL, then create a .tar file locally instead of actually uploading it
        /// Set to eDebugMode.MyEMSLOfflineMode to create the .tar file locally without contacting MyEMSL</param>
        /// <param name="statusURI">Status URL</param>
        /// <returns>True if successfully uploaded, false if an error</returns>
        public bool StartUpload(
            List<Dictionary<string, object>> metadataObject,
            EasyHttp.eDebugMode debugMode,
            out string statusURI)
        {

            statusURI = string.Empty;
            ErrorMessage = string.Empty;

            if (!File.Exists(Configuration.CLIENT_CERT_FILEPATH))
            {
                OnError("Authentication failure in StartUpload; cert file not found at " + Configuration.CLIENT_CERT_FILEPATH);
                return false;
            }

            var fileList = Utilities.GetFileListFromMetadataObject(metadataObject);

            // Grab the list of files from the top-level "file" object
            // Keys in this dictionary are the source file path; values are metadata about the file
            var fileListObject = new SortedDictionary<string, FileInfoObject>();

            // This is a list of dictionary objects
            // Dictionary keys will be sha1Hash, destinationDirectory, and fileName
            // var newFileObj = new List<Dictionary<string, string>>();

            foreach (var file in fileList)
            {

                var fiObj = new FileInfoObject(file.AbsoluteLocalPath, file.RelativeDestinationDirectory, file.Sha1HashHex);

                fileListObject.Add(file.AbsoluteLocalPath, fiObj);
            }

            // Optionally use the test instance
            mPacificaConfig.UseTestInstance = UseTestInstance;

            var mdJson = Utilities.ObjectToJson(metadataObject);

            // Create the metadata.txt file
            var metadataFilePath = Path.GetTempFileName();
            var mdTextFile = new FileInfo(metadataFilePath);
            using (var sw = mdTextFile.CreateText())
            {
                sw.Write(mdJson);
            }

            try
            {
                // Copy the Metadata.txt file to the transfer folder, renaming it when we copy it.
                // Example path: \\proto-4\DMS3_Xfer\QC_Shew_16_01_125ng_CID-STD_newCol-1_5Apr17_Frodo_16-11-08\MyEMSL_metadata_CaptureJob_2836788.txt
                if (!string.IsNullOrWhiteSpace(TransferFolderPath))
                {
                    var fiTargetFile = new FileInfo(Path.Combine(TransferFolderPath, Utilities.GetMetadataFilenameForJob(JobNumber)));
                    if (fiTargetFile.Directory != null && !fiTargetFile.Directory.Exists)
                        fiTargetFile.Directory.Create();

                    mdTextFile.CopyTo(fiTargetFile.FullName, true);
                }

            }
            // ReSharper disable once EmptyGeneralCatchClause
            catch
            {
                // Ignore errors here
            }


            if (fileList.Count == 0)
            {
                OnDebugEvent("File list is empty in StartUpload; nothing to do");
                RaiseUploadCompleted(string.Empty);
                return true;
            }

            var location = "upload";
            var serverUri = "https://ServerIsOffline/dummy_page?test";

            if (debugMode == EasyHttp.eDebugMode.MyEMSLOfflineMode)
            {
                OnDebugEvent("StartUpload is creating the .tar file locally");
            }
            else
            {
                serverUri = mPacificaConfig.IngestServerUri;

                // URL for posting the virtual .tar file to
                // Typically: https://ingestdms.my.emsl.pnl.gov/upload
                var storageUrl = serverUri + "/" + location;

                OnDebugEvent("StartUpload is sending file to " + storageUrl);
            }

            var responseData = EasyHttp.SendFileListToIngester(
                mPacificaConfig, location, serverUri, fileListObject, mdTextFile.FullName, debugMode);

            if (debugMode != EasyHttp.eDebugMode.DebugDisabled)
            {
                // A .tar file was created locally; it was not sent to the server
                return false;
            }

            var responseJSON = Utilities.JsonToObject(responseData);

            var transactionID = Convert.ToInt32(responseJSON["job_id"].ToString());

            statusURI = mPacificaConfig.IngestServerUri + "/get_state?job_id=" + transactionID;

            var success = false;

            try
            {
                string statusResult;
                if (responseData.Contains("state"))
                {
                    // We already have a valid server response
                    statusResult = responseData;
                }
                else
                {
                    statusResult = EasyHttp.Send(mPacificaConfig, statusURI, out HttpStatusCode responseStatusCode);
                }

                var statusJSON = Utilities.JsonToObject(statusResult);

                var state = statusJSON["state"].ToString().ToLower();

                if (state == "ok")
                {
                    success = true;
                    RaiseUploadCompleted(statusURI);
                }
                else if (state == "failed")
                {
                    OnError("Upload failed during ingest process");
                    RaiseUploadCompleted(statusResult);
                }
                else if (state.Contains("error"))
                {
                    OnError("Ingester Backend is offline or having issues");
                    RaiseUploadCompleted(statusResult);
                }
                else
                {
                    OnError("Unrecognized ingest state: " + statusJSON["state"]);
                }

            }
            catch (Exception ex)
            {
                OnError("Exception examining the MyEMSL response string: " + ex.Message, ex);
            }

            try
            {
                // Delete the local temporary file
                mdTextFile.Delete();
            }
            // ReSharper disable once EmptyGeneralCatchClause
            catch
            {
                // Ignore errors here
            }

            return success;
        }

        public string GenerateSha1Hash(string fullFilePath)
        {
            return Utilities.GenerateSha1Hash(fullFilePath);
        }

        #endregion

        #region Member Methods

        /// <summary>
        /// Create the metadata object with the upload details, including the files to upload
        /// </summary>
        /// <param name="uploadMetadata">Upload metadata</param>
        /// <param name="lstUnmatchedFiles">Files to upload</param>
        /// <param name="eusInfo">Output parameter: EUS instrument ID, proposal ID, and uploader ID</param>
        /// <returns>
        /// Dictionary of the information to translate to JSON;
        /// Keys are key names; values are either strings or dictionary objects or even a list of dictionary objects
        /// </returns>
        public static List<Dictionary<string, object>> CreatePacificaMetadataObject(
            UploadMetadata uploadMetadata,
            List<FileInfoObject> lstUnmatchedFiles,
            out EUSInfo eusInfo)
        {
            eusInfo = new EUSInfo();
            eusInfo.Clear();

            // new metadata object is just a list of dictionary entries
            var metadataObject = new List<Dictionary<string, object>>();

            if (uploadMetadata.DMSInstrumentName.IndexOf("LCQ", StringComparison.OrdinalIgnoreCase) >= 0 &&
                uploadMetadata.EUSInstrumentID <= 0)
            {
                uploadMetadata.EUSInstrumentID = 1163;
            }

            if (uploadMetadata.DMSInstrumentName == "Exact02" &&
                uploadMetadata.EUSInstrumentID <= 0)
            {
                uploadMetadata.EUSInstrumentID = 34111;
            }

            if (uploadMetadata.DMSInstrumentName == "IMS07_AgTOF04" &&
                uploadMetadata.EUSInstrumentID <= 0)
            {
                uploadMetadata.EUSInstrumentID = 34155;
            }

            var eusInstrumentID = GetEUSInstrumentID(uploadMetadata.EUSInstrumentID, UNKNOWN_INSTRUMENT_EUS_INSTRUMENT_ID);

            // fill out Transaction Key/Value pairs
            if (uploadMetadata.DatasetID > 0)
            {
                metadataObject.Add(new Dictionary<string, object> {
                    { "destinationTable", "TransactionKeyValue" },
                    { "key", "omics.dms.instrument" },
                    { "value", uploadMetadata.DMSInstrumentName }
                });
                metadataObject.Add(new Dictionary<string, object> {
                    { "destinationTable", "TransactionKeyValue" },
                    { "key", "omics.dms.instrument_id" },
                    { "value", eusInstrumentID }
                });
                metadataObject.Add(new Dictionary<string, object> {
                    { "destinationTable", "TransactionKeyValue" },
                    { "key", "omics.dms.date_code" },
                    { "value", uploadMetadata.DateCodeString }
                });
                metadataObject.Add(new Dictionary<string, object> {
                    { "destinationTable", "TransactionKeyValue" },
                    { "key", "omics.dms.dataset" },
                    { "value", uploadMetadata.DatasetName }
                });
                metadataObject.Add(new Dictionary<string, object> {
                    { "destinationTable", "TransactionKeyValue" },
                    { "key", "omics.dms.campaign_name" },
                    { "value", uploadMetadata.CampaignName }
                });
                metadataObject.Add(new Dictionary<string, object> {
                    { "destinationTable", "TransactionKeyValue" },
                    { "key", "omics.dms.experiment_name" },
                    { "value", uploadMetadata.ExperimentName }
                });
                metadataObject.Add(new Dictionary<string, object> {
                    { "destinationTable", "TransactionKeyValue" },
                    { "key", "omics.dms.dataset_name" },
                    { "value", uploadMetadata.DatasetName }
                });
                metadataObject.Add(new Dictionary<string, object> {
                    { "destinationTable", "TransactionKeyValue" },
                    { "key", "omics.dms.campaign_id" },
                    { "value", uploadMetadata.CampaignID.ToString(CultureInfo.InvariantCulture) }
                });
                metadataObject.Add(new Dictionary<string, object> {
                    { "destinationTable", "TransactionKeyValue" },
                    { "key", "omics.dms.experiment_id" },
                    { "value", uploadMetadata.ExperimentID.ToString(CultureInfo.InvariantCulture) }
                });
                metadataObject.Add(new Dictionary<string, object> {
                    { "destinationTable", "TransactionKeyValue" },
                    { "key", "omics.dms.dataset_id" },
                    { "value", uploadMetadata.DatasetID.ToString(CultureInfo.InvariantCulture) }
                });
                if (!string.IsNullOrEmpty(uploadMetadata.OrganismName))
                {
                    metadataObject.Add(new Dictionary<string, object> {
                        { "destinationTable", "TransactionKeyValue" },
                        { "key", "organism_name" },
                        { "value", uploadMetadata.OrganismName }
                    });
                }
                if (uploadMetadata.OrganismID != 0)
                {
                    metadataObject.Add(new Dictionary<string, object> {
                        { "destinationTable", "TransactionKeyValue" },
                        { "key", "omics.dms.organism_id" },
                        { "value", uploadMetadata.OrganismID.ToString(CultureInfo.InvariantCulture) }
                    });
                }
                if (uploadMetadata.NCBITaxonomyID != 0)
                {
                    metadataObject.Add(new Dictionary<string, object> {
                        { "destinationTable", "TransactionKeyValue" },
                        { "key", "ncbi_taxonomy_id" },
                        { "value", uploadMetadata.NCBITaxonomyID.ToString(CultureInfo.InvariantCulture) }
                    });
                }
                if (!string.IsNullOrEmpty(uploadMetadata.SeparationType))
                {
                    metadataObject.Add(new Dictionary<string, object> {
                        { "destinationTable", "TransactionKeyValue" },
                        { "key", "omics.dms.separation_type" },
                        { "value", uploadMetadata.SeparationType }
                    });
                }
                if (!string.IsNullOrEmpty(uploadMetadata.DatasetType))
                {
                    metadataObject.Add(new Dictionary<string, object> {
                        { "destinationTable", "TransactionKeyValue" },
                        { "key", "omics.dms.dataset_type" },
                        { "value", uploadMetadata.DatasetType }
                    });
                }
                metadataObject.Add(new Dictionary<string, object> {
                    { "destinationTable", "TransactionKeyValue" },
                    { "key", "omics.dms.run_acquisition_length_min" },
                    { "value", uploadMetadata.AcquisitionLengthMin }
                });
                if (uploadMetadata.UserOfRecordList.Count > 0)
                {
                    foreach (var userId in uploadMetadata.UserOfRecordList)
                    {
                        metadataObject.Add(new Dictionary<string, object> {
                            { "destinationTable", "TransactionKeyValue" },
                            { "key", "User of Record" },
                            { "value", userId.ToString(CultureInfo.InvariantCulture) }
                        });
                        metadataObject.Add(new Dictionary<string, object> {
                            { "destinationTable", "TransactionKeyValue" },
                            { "key", "user_of_record" },
                            { "value", userId.ToString(CultureInfo.InvariantCulture) }
                        });
                    }
                }
            }
            else if (uploadMetadata.DataPackageID > 0)
            {

                metadataObject.Add(new Dictionary<string, object> {
                    { "destinationTable", "TransactionKeyValue" },
                    { "key", "omics.dms.instrument" },
                    { "value", uploadMetadata.DMSInstrumentName }
                });

                metadataObject.Add(new Dictionary<string, object> {
                    { "destinationTable", "TransactionKeyValue" },
                    { "key", "omics.dms.instrument_id" },
                    { "value", eusInstrumentID }
                });

                metadataObject.Add(new Dictionary<string, object> {
                    { "destinationTable", "TransactionKeyValue" },
                    { "key", "omics.dms.datapackage_id" },
                    { "value", uploadMetadata.DataPackageID.ToString(CultureInfo.InvariantCulture) }
                });
            }
            else
            {
                // ReSharper disable once NotResolvedInText
                throw new ArgumentOutOfRangeException("Must define a DatasetID or a DataPackageID; cannot create the metadata object");
            }

            // now fill in the required metadata
            if (uploadMetadata.DatasetID > 0)
            {
                metadataObject.Add(new Dictionary<string, object> {
                    { "destinationTable", "Transactions.instrument" },
                    { "value", eusInstrumentID }
                });
                eusInfo.EUSInstrumentID = eusInstrumentID;
            }

            string EUSProposalID;
            if (string.IsNullOrWhiteSpace(uploadMetadata.EUSProposalID))
            {
                // This dataset or data package does not have an EUS_Proposal_ID
                EUSProposalID = DEFAULT_EUS_PROPOSAL_ID;
            }
            else
            {
                EUSProposalID = uploadMetadata.EUSProposalID;
            }
            eusInfo.EUSProposalID = EUSProposalID;

            metadataObject.Add(new Dictionary<string, object> {
                { "destinationTable", "Transactions.proposal" },
                { "value", EUSProposalID }
            });

            int EUSUploaderID;

            // For datasets, eusOperatorID is the instrument operator EUS ID
            // For data packages, it is the EUS ID of the data package owner
            if (uploadMetadata.EUSOperatorID == 0)
            {
                // This should have already been flagged in upstream code
                // But if we reach this function and it is still 0, we will use the default operator ID
                EUSUploaderID = DEFAULT_EUS_OPERATOR_ID;
            }
            else
            {
                EUSUploaderID = uploadMetadata.EUSOperatorID;
            }
            eusInfo.EUSUploaderID = EUSUploaderID;

            metadataObject.Add(new Dictionary<string, object> {
                { "destinationTable", "Transactions.submitter" },
                { "value", EUSUploaderID.ToString() }
            });

            // now mix in the list of file objects
            foreach (var f in lstUnmatchedFiles)
            {
                // The subdir path must be "data/" or of the form "data/SubDirectory"
                // "data/" is required for files at the root dataset level because the root of the tar file
                // has a metadata.txt file and we would have a conflict if the dataset folder root
                // also had a file named metadata.txt

                // The ingest system will trim out the leading "data/" when storing the SubDir in the system

                // Note the inconsistent requirements; files in the root dataset level must have "data/"
                // while files in subdirectories should have a SubDir that does _not_ end in a forward slash

                string subdirString;

                if (string.IsNullOrWhiteSpace(f.RelativeDestinationDirectory))
                    subdirString = "data/";
                else
                    subdirString = "data/" + f.RelativeDestinationDirectory.Trim('/');

                if (subdirString.Contains("//"))
                {
                    throw new Exception("File path should not have two forward slashes: " + subdirString);
                }

                metadataObject.Add(new Dictionary<string, object> {
                    { "destinationTable", "Files" },
                    { "name", f.FileName },
                    { "absolutelocalpath", f.AbsoluteLocalPath},
                    { "subdir", subdirString },
                    { "size", f.FileSizeInBytes.ToString() },
                    { "hashsum", f.Sha1HashHex },
                    { "mimetype", "application/octet-stream" },
                    { "hashtype", "sha1" },
                    { "ctime", f.CreationTime.ToUniversalTime().ToString("s") },
                    { "mtime", f.LastWriteTime.ToUniversalTime().ToString("s") }
                });
            }

            return metadataObject;

        }

        /// <summary>
        /// Return a string description of the EUS info encoded by metadataObject
        /// </summary>
        /// <param name="metadataObject"></param>
        /// <returns></returns>
        public static string GetMetadataObjectDescription(List<Dictionary<string, object>> metadataObject)
        {

            var metadataList = new List<string>();
            var fileCount = 0;

            var kvLookup = new Dictionary<string, object>
            {
                {"omics.dms.dataset_id", "Dataset_ID"},
                {"omics.dms.datapackage_id", "DataPackage_ID"},
                {"omics.dms.instrument", "DMS_Instrument"},
                {"omics.dms.instrument_id", "EUS_Instrument_ID"}
            };

            var transactionValueLookup = new Dictionary<string, string>
            {
                {"Transactions.proposal", "EUS_Proposal_ID"},
                {"Transactions.submitter", "EUS_User_ID"},
                {"Transactions.instrument", "EUS_Instrument_ID"}
            };

            var matchedKeys = new SortedSet<string>();

            foreach (var item in metadataObject)
            {
                if (!GetDictionaryValue(item, "destinationTable", out string tableName))
                    continue;

                if (tableName == "TransactionKeyValue")
                {
                    if (GetDictionaryValue(item, "key", out string keyName))
                    {
                        if (GetDictionaryValue(item, "value", out string keyValue))
                        {
                            if (GetDictionaryValue(kvLookup, keyName, out string valueDescription))
                            {
                                metadataList.Add(valueDescription + "=" + keyValue);
                                matchedKeys.Add(valueDescription);
                            }

                        }
                    }

                }
                else if (tableName == "Files")
                {
                    if (item.TryGetValue("size", out _))
                    {
                        fileCount += 1;
                    }

                }
                else
                {
                    if (transactionValueLookup.TryGetValue(tableName, out var valueDescription))
                    {
                        if (matchedKeys.Contains(valueDescription))
                        {
                            // This item has already been added (typically EUS_Instrument_ID)
                            continue;
                        }

                        // Include the value for this item in the description
                        if (GetDictionaryValue(item, "value", out string keyValue))
                        {
                            metadataList.Add(valueDescription + "=" + keyValue);
                            matchedKeys.Add(valueDescription);
                        }
                    }
                }
            }

            return string.Join("; ", metadataList) + "; FileCount=" + fileCount;

        }

        private static bool GetDictionaryValue(IReadOnlyDictionary<string, object> eusInfoMapObject, string keyName, out string matchedValue)
        {
            if (eusInfoMapObject.TryGetValue(keyName, out object value))
            {
                matchedValue = value as string;
                if (matchedValue != null)
                    return true;
            }

            matchedValue = string.Empty;
            return false;
        }

        private static bool GetMetadataGroupValue(IEnumerable<Dictionary<string, string>> groupObjects, string groupKeyName, out string matchedValue)
        {
            foreach (var groupDefinition in groupObjects)
            {

                if (!groupDefinition.TryGetValue("type", out string candidateKeyName))
                    continue;

                if (!string.Equals(candidateKeyName, groupKeyName))
                    continue;

                // Found the desired dictionary entry
                if (!groupDefinition.TryGetValue("name", out string value))
                    continue;

                matchedValue = value;
                return true;
            }

            matchedValue = string.Empty;
            return false;
        }

        /// <summary>
        /// Return the EUS instrument ID, falling back to instrumentIdIfUnknown if eusInstrumentId is empty
        /// </summary>
        /// <param name="eusInstrumentId"></param>
        /// <param name="instrumentIdIfUnknown"></param>
        /// <returns></returns>
        private static int GetEUSInstrumentID(int eusInstrumentId, int instrumentIdIfUnknown)
        {
            return eusInstrumentId <= 0 ? instrumentIdIfUnknown : eusInstrumentId;
        }

        private void OnError(string errorMessage, Exception ex = null)
        {
            ErrorMessage = errorMessage;
            OnErrorEvent(errorMessage, ex);

        }

        #endregion
    }

}
