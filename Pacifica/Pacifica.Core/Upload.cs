using System;
using System.Collections.Generic;
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
        /// <remarks>34127 is VOrbiETD04</remarks>
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
            /// <remarks>Aka EUSSubmitterId</remarks>
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

            EasyHttp.MyEMSLOffline += EasyHttp_MyEMSLOffline;

            EasyHttp.ErrorEvent += OnErrorEvent;

            ErrorMessage = string.Empty;
            TransferFolderPath = transferFolderPath;
            JobNumber = jobNumber;
        }

        #endregion

        #region Events and Handlers

        public event MessageEventHandler MyEMSLOffline;
        public event UploadCompletedEventHandler UploadCompleted;
        public event StatusUpdateEventHandler StatusUpdate;

        private void EasyHttp_MyEMSLOffline(object sender, MessageEventArgs e)
        {
            MyEMSLOffline?.Invoke(this, e);
        }

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

            var certificateFilePath = EasyHttp.ResolveCertFile(mPacificaConfig, "StartUpload", out var errorMessage);

            if (string.IsNullOrWhiteSpace(certificateFilePath))
            {
                OnError(errorMessage);
                return false;
            }

            var fileList = Utilities.GetFileListFromMetadataObject(metadataObject);

            // Grab the list of files from the top-level "file" object
            // Keys in this dictionary are the source file path (Windows paths); values are metadata about the file
            var fileListObject = new SortedDictionary<string, FileInfoObject>();

            foreach (var file in fileList)
            {
                var fio = new FileInfoObject(file.AbsoluteLocalPath, file.RelativeDestinationDirectory, file.Sha1HashHex);
                fileListObject.Add(file.AbsoluteLocalPath, fio);
            }

            // Optionally use the test instance
            mPacificaConfig.UseTestInstance = UseTestInstance;

            var jsonMetadata = Utilities.ObjectToJson(metadataObject);

            // Create the metadata.txt file
            var metadataFilePath = Path.GetTempFileName();
            var metadataFile = new FileInfo(metadataFilePath);
            using (var metadataWriter = metadataFile.CreateText())
            {
                metadataWriter.Write(jsonMetadata);
            }

            try
            {
                // Copy the Metadata.txt file to the transfer folder, renaming it when we copy it.
                // Example path: \\proto-4\DMS3_Xfer\QC_Shew_16_01_125ng_CID-STD_newCol-1_5Apr17_Frodo_16-11-08\MyEMSL_metadata_CaptureJob_2836788.txt
                if (!string.IsNullOrWhiteSpace(TransferFolderPath))
                {
                    var targetFile = new FileInfo(Path.Combine(TransferFolderPath, Utilities.GetMetadataFilenameForJob(JobNumber)));
                    if (targetFile.Directory != null && !targetFile.Directory.Exists)
                        targetFile.Directory.Create();

                    metadataFile.CopyTo(targetFile.FullName, true);
                }

            }
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
                mPacificaConfig, location, serverUri, fileListObject, metadataFile.FullName, debugMode);

            if (debugMode != EasyHttp.eDebugMode.DebugDisabled)
            {
                // A .tar file was created locally; it was not sent to the server
                return false;
            }

            try
            {

                var responseJSON = Utilities.JsonToObject(responseData);

                var transactionID = Convert.ToInt32(responseJSON["job_id"].ToString());

                statusURI = mPacificaConfig.IngestServerUri + "/get_state?job_id=" + transactionID;
            }
            catch (Exception ex)
            {
                OnError("Error converting the response data to a JSON object", ex);

                // Delete the local temporary file
                Utilities.DeleteFileIgnoreErrors(metadataFile);
                return false;
            }

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
                    statusResult = EasyHttp.SendViaThreadStart(mPacificaConfig, statusURI, out _);
                }

                if (EasyHttp.IsResponseError(statusResult))
                {
                    OnError("Error checking status: " + statusResult);
                    return false;
                }

                Dictionary<string, object> statusJSON;

                try
                {
                    statusJSON = Utilities.JsonToObject(statusResult);
                }
                catch (Exception)
                {
                    OnError("Unable to parse response into JSON: " + statusResult);
                    return false;
                }

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

            // Delete the local temporary file
            Utilities.DeleteFileIgnoreErrors(metadataFile);

            return success;
        }

        public string GenerateSha1Hash(string fullFilePath)
        {
            return Utilities.GenerateSha1Hash(fullFilePath);
        }

        #endregion

        #region Member Methods

        private static void AppendKVMetadata(ICollection<Dictionary<string, object>> metadataObject, string keyName, int value)
        {
            metadataObject.Add(new Dictionary<string, object> {
                { "destinationTable", "TransactionKeyValue" },
                { "key", keyName },
                { "value", value }
            });
        }

        private static void AppendKVMetadata(ICollection<Dictionary<string, object>> metadataObject, string keyName, string value)
        {
            metadataObject.Add(new Dictionary<string, object> {
                { "destinationTable", "TransactionKeyValue" },
                { "key", keyName },
                { "value", value }
            });
        }

        private static void AppendTransactionMetadata(ICollection<Dictionary<string, object>> metadataObject, string columnName, int value)
        {
            // Example destination table name:
            //  Transactions.instrument
            metadataObject.Add(new Dictionary<string, object> {
                { "destinationTable", "Transactions." + columnName },
                { "value", value }
            });
        }

        private static void AppendTransactionMetadata(ICollection<Dictionary<string, object>> metadataObject, string columnName, string value)
        {
            // Example destination table names:
            //  Transactions.proposal
            //  Transactions.submitter
            metadataObject.Add(new Dictionary<string, object> {
                { "destinationTable", "Transactions." + columnName },
                { "value", value }
            });
        }

        /// <summary>
        /// Create the metadata object with the upload details, including the files to upload
        /// </summary>
        /// <param name="uploadMetadata">Upload metadata</param>
        /// <param name="filesToUpload">Files to upload</param>
        /// <param name="eusInfo">Output parameter: EUS instrument ID, proposal ID, and uploader ID</param>
        /// <returns>
        /// Dictionary of the information to translate to JSON;
        /// Keys are key names; values are either strings or dictionary objects or even a list of dictionary objects
        /// </returns>
        public static List<Dictionary<string, object>> CreatePacificaMetadataObject(
            UploadMetadata uploadMetadata,
            List<FileInfoObject> filesToUpload,
            out EUSInfo eusInfo)
        {
            eusInfo = new EUSInfo();
            eusInfo.Clear();

            // new metadata object is just a list of dictionary entries
            var metadataObject = new List<Dictionary<string, object>>();

            if (uploadMetadata.EUSInstrumentID <= 0)
            {
                // Possibly override EUSInstrument ID
                if (uploadMetadata.DMSInstrumentName.IndexOf("LCQ", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    uploadMetadata.EUSInstrumentID = 1163;
                }

                if (string.Equals(uploadMetadata.DMSInstrumentName, "Exact02", StringComparison.OrdinalIgnoreCase))
                {
                    uploadMetadata.EUSInstrumentID = 34111;
                }

                if (string.Equals(uploadMetadata.DMSInstrumentName, "IMS07_AgTOF04", StringComparison.OrdinalIgnoreCase))
                {
                    uploadMetadata.EUSInstrumentID = 34155;
                }
            }

            // Now that EUS instrument ID is defined, store it and lookup other EUS info
            eusInfo.EUSInstrumentID = GetEUSInstrumentID(uploadMetadata.EUSInstrumentID, UNKNOWN_INSTRUMENT_EUS_INSTRUMENT_ID);
            eusInfo.EUSProposalID = GetEUSProposalID(uploadMetadata.EUSProposalID, DEFAULT_EUS_PROPOSAL_ID);
            eusInfo.EUSUploaderID = GetEUSSubmitterID(uploadMetadata.EUSOperatorID, DEFAULT_EUS_OPERATOR_ID);

            // Fill out Transaction Key/Value pairs
            if (uploadMetadata.DatasetID > 0)
            {
                AppendKVMetadata(metadataObject, "omics.dms.instrument", uploadMetadata.DMSInstrumentName);
                AppendKVMetadata(metadataObject, "omics.dms.instrument_id", eusInfo.EUSInstrumentID);
                AppendKVMetadata(metadataObject, "omics.dms.date_code", uploadMetadata.DateCodeString);
                AppendKVMetadata(metadataObject, "omics.dms.dataset", uploadMetadata.DatasetName);
                AppendKVMetadata(metadataObject, "omics.dms.campaign_name", uploadMetadata.CampaignName);
                AppendKVMetadata(metadataObject, "omics.dms.experiment_name", uploadMetadata.ExperimentName);
                AppendKVMetadata(metadataObject, "omics.dms.dataset_name", uploadMetadata.DatasetName);
                AppendKVMetadata(metadataObject, "omics.dms.campaign_id", uploadMetadata.CampaignID.ToString());
                AppendKVMetadata(metadataObject, "omics.dms.experiment_id", uploadMetadata.ExperimentID.ToString());
                AppendKVMetadata(metadataObject, "omics.dms.dataset_id", uploadMetadata.DatasetID.ToString());

                if (!string.IsNullOrEmpty(uploadMetadata.OrganismName))
                {
                    AppendKVMetadata(metadataObject, "organism_name", uploadMetadata.OrganismName);
                }

                if (uploadMetadata.OrganismID != 0)
                {
                    AppendKVMetadata(metadataObject, "omics.dms.organism_id", uploadMetadata.OrganismID.ToString());
                }

                if (uploadMetadata.NCBITaxonomyID != 0)
                {
                    AppendKVMetadata(metadataObject, "ncbi_taxonomy_id", uploadMetadata.NCBITaxonomyID.ToString());
                }

                if (!string.IsNullOrEmpty(uploadMetadata.SeparationType))
                {
                    AppendKVMetadata(metadataObject, "omics.dms.separation_type", uploadMetadata.SeparationType);
                }

                if (!string.IsNullOrEmpty(uploadMetadata.DatasetType))
                {
                    AppendKVMetadata(metadataObject, "omics.dms.dataset_type", uploadMetadata.DatasetType);
                }

                AppendKVMetadata(metadataObject, "omics.dms.run_acquisition_length_min", uploadMetadata.AcquisitionLengthMin);

                if (uploadMetadata.UserOfRecordList.Count > 0)
                {
                    foreach (var userId in uploadMetadata.UserOfRecordList)
                    {
                        AppendKVMetadata(metadataObject, "User of Record", userId.ToString());
                        AppendKVMetadata(metadataObject, "user_of_record", userId.ToString());
                    }
                }
            }
            else if (uploadMetadata.DataPackageID > 0)
            {

                AppendKVMetadata(metadataObject, "omics.dms.instrument", uploadMetadata.DMSInstrumentName);

                AppendKVMetadata(metadataObject, "omics.dms.instrument_id", eusInfo.EUSInstrumentID);

                AppendKVMetadata(metadataObject, "omics.dms.datapackage_id", uploadMetadata.DataPackageID.ToString());
            }
            else
            {
                throw new Exception("Must define a non-zero DatasetID or a DataPackageID; cannot create the metadata object");
            }

            // Append the required metadata
            AppendTransactionMetadata(metadataObject, "instrument", eusInfo.EUSInstrumentID);
            AppendTransactionMetadata(metadataObject, "proposal", eusInfo.EUSProposalID);
            AppendTransactionMetadata(metadataObject, "submitter", eusInfo.EUSUploaderID);

            // Append the files
            foreach (var file in filesToUpload)
            {
                // The subdir path must be "data/" or of the form "data/SubDirectory"
                // "data/" is required for files at the root dataset level because the root of the tar file
                // has a metadata.txt file and we would have a conflict if the dataset folder root
                // also had a file named metadata.txt

                // The ingest system will trim out the leading "data/" when storing the SubDir in the system

                // Note the inconsistent requirements; files in the root dataset level must have "data/"
                // while files in subdirectories should have a SubDir that does _not_ end in a forward slash
                // It is likely that this discrepancy has been fixed in the backend python code on the ingest server

                string subdirString;

                if (string.IsNullOrWhiteSpace(file.RelativeDestinationDirectory))
                    subdirString = "data/";
                else
                    subdirString = "data/" + file.RelativeDestinationDirectory.Trim('/');

                if (subdirString.Contains("//"))
                {
                    throw new Exception("File path should not have two forward slashes: " + subdirString);
                }

                metadataObject.Add(new Dictionary<string, object> {
                    { "destinationTable", "Files" },
                    { "name", file.FileName },
                    { "absolutelocalpath", file.AbsoluteLocalPath},
                    { "subdir", subdirString },
                    { "size", file.FileSizeInBytes.ToString() },
                    { "hashsum", file.Sha1HashHex },
                    { "mimetype", "application/octet-stream" },
                    { "hashtype", "sha1" },
                    { "ctime", file.CreationTime.ToUniversalTime().ToString("s") },
                    { "mtime", file.LastWriteTime.ToUniversalTime().ToString("s") }
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
                if (!GetDictionaryValue(item, "destinationTable", out var tableName))
                    continue;

                switch (tableName)
                {
                    case "TransactionKeyValue":
                    {
                        if (!GetDictionaryValue(item, "key", out var keyName)) continue;
                        if (!GetDictionaryValue(item, "value", out var keyValue)) continue;
                        if (!GetDictionaryValue(kvLookup, keyName, out var valueDescription)) continue;

                        metadataList.Add(valueDescription + "=" + keyValue);
                        matchedKeys.Add(valueDescription);
                        break;
                    }
                    case "Files":
                        if (item.TryGetValue("size", out _))
                        {
                            fileCount += 1;
                        }
                        break;
                    default:
                    {
                        if (!transactionValueLookup.TryGetValue(tableName, out var valueDescription)) continue;

                        if (matchedKeys.Contains(valueDescription))
                        {
                            // This item has already been added (typically EUS_Instrument_ID)
                            continue;
                        }

                        // Include the value for this item in the description
                        if (!GetDictionaryValue(item, "value", out var keyValue)) continue;

                        metadataList.Add(valueDescription + "=" + keyValue);
                        matchedKeys.Add(valueDescription);
                        break;
                    }
                }
            }

            return string.Join("; ", metadataList) + "; FileCount=" + fileCount;

        }

        private static bool GetDictionaryValue(IReadOnlyDictionary<string, object> eusInfoMapObject, string keyName, out string matchedValue)
        {
            if (eusInfoMapObject.TryGetValue(keyName, out var value))
            {
                matchedValue = value as string;
                if (matchedValue != null)
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

        private static string GetEUSProposalID(string eusProposalId, string eusProposalIdIfUnknown)
        {
            return string.IsNullOrWhiteSpace(eusProposalId) ? eusProposalIdIfUnknown : eusProposalId;
        }

        private static int GetEUSSubmitterID(int eusOperatorId, int eusOperatorIdIfUnknown)
        {
            // For datasets, eusOperatorID is the instrument operator EUS ID
            // For data packages, it is the EUS ID of the data package owner
            return eusOperatorId == 0 ? eusOperatorIdIfUnknown : eusOperatorId;
        }

        private void OnError(string errorMessage, Exception ex = null)
        {
            ErrorMessage = errorMessage;
            OnErrorEvent(errorMessage, ex);

        }

        #endregion
    }

}
