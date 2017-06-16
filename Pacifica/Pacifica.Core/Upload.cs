using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Net;

namespace Pacifica.Core
{
    public class Upload : IUpload
    {
        /// <summary>
        /// Default EUS ID is "Monroe, Matthew"
        /// </summary>
        public const int DEFAULT_EUS_OPERATOR_ID = 43428;

        // Proposal ID 17797 is "Development of High Throughput Proteomic Production Operations"
        // It is a string because it may contain suffix letters
        public const string DEFAULT_EUS_PROPOSAL_ID = "17797";

        // VOrbiETD04 is 34127
        private const string UNKNOWN_INSTRUMENT_EUS_INSTRUMENT_ID = "34127";

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
            public string DMSInstrumentName;    // Only used for datasets; not Data Packages
            public string CampaignName;
            public int CampaignID;
            public string EUSInstrumentID;      // Only used for datasets; not Data Packages
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
                EUSInstrumentID = string.Empty;
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

        public string ErrorMessage
        { get; private set; }

        /// <summary>
        /// The metadata.txt file will be copied to the Transfer Folder if the folder path is not empty
        /// </summary>
        public string TransferFolderPath
        { get; set; }

        /// <summary>
        /// The metadata.txt file name will include the JobNumber text in the name, for example MyEMSL_metadata_CaptureJob_12345.txt
        /// </summary>
        public string JobNumber
        { get; set; }

        /// <summary>
        /// When true, upload to test3.my.emsl.pnl.gov instead of ingest.my.emsl.pnl.gov
        /// </summary>
        public bool UseTestInstance { get; set; }

        #endregion

        #region Private Members

        #endregion

        #region Constructor

        /// <summary>
        /// Constructor
        /// </summary>
        /// <remarks>TransferFolderPath and JobNumber will be empty</remarks>
        public Upload()
            : this(string.Empty, string.Empty)
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="transferFolderPath">Transfer folder path for this dataset, for example \\proto-4\DMS3_Xfer\SysVirol_IFT001_Pool_17_B_10x_27Aug13_Tiger_13-07-36</param>
        /// <param name="jobNumber">DMS Data Capture job number</param>
        /// <remarks>The metadata.txt file will be copied to the transfer folder</remarks>
        public Upload(string transferFolderPath, string jobNumber)
        {

            // Note that EasyHttp is a static class with a static event
            // Be careful about instantiating this class (Upload) multiple times
            EasyHttp.StatusUpdate += EasyHttp_StatusUpdate;

            ErrorMessage = string.Empty;
            TransferFolderPath = transferFolderPath;
            JobNumber = jobNumber;
        }

        #endregion

        #region Events and Handlers

        public event MessageEventHandler DebugEvent;
        public event MessageEventHandler ErrorEvent;
        public event UploadCompletedEventHandler UploadCompleted;
        public event StatusUpdateEventHandler StatusUpdate;

        private void RaiseDebugEvent(string callingFunction, string currentTask)
        {
            DebugEvent?.Invoke(this, new MessageEventArgs(callingFunction, currentTask));
        }

        private void RaiseErrorEvent(string callingFunction, string errorMessage)
        {
            ErrorEvent?.Invoke(this, new MessageEventArgs(callingFunction, errorMessage));
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

        public bool StartUpload(List<Dictionary<string, object>> metadataObject, out string statusURL)
        {
            NetworkCredential loginCredentials = null;
            const EasyHttp.eDebugMode debugMode = EasyHttp.eDebugMode.DebugDisabled;

            // ReSharper disable once ExpressionIsAlwaysNull
            return StartUpload(metadataObject, loginCredentials, debugMode, out statusURL);
        }


        public bool StartUpload(List<Dictionary<string, object>> metadataObject, EasyHttp.eDebugMode debugMode, out string statusURL)
        {
            NetworkCredential loginCredentials = null;

            // ReSharper disable once ExpressionIsAlwaysNull
            return StartUpload(metadataObject, loginCredentials, debugMode, out statusURL);
        }

        public bool StartUpload(List<Dictionary<string, object>> metadataObject, NetworkCredential loginCredentials, out string statusURL)
        {
            const EasyHttp.eDebugMode debugMode = EasyHttp.eDebugMode.DebugDisabled;
            return StartUpload(metadataObject, loginCredentials, debugMode, out statusURL);
        }


        /// <summary>
        ///
        /// </summary>
        /// <param name="metadataObject"></param>
        /// <param name="loginCredentials"></param>
        /// <param name="debugMode">
        /// Set to eDebugMode.CreateTarLocal to authenticate with MyEMSL, then create a .tar file locally instead of actually uploading it
        /// Set to eDebugMode.MyEMSLOfflineMode to create the .tar file locally without contacting MyEMSL</param>
        /// <param name="statusURL"></param>
        /// <returns>True if successfully uploaded, false if an error</returns>
        public bool StartUpload(
            List<Dictionary<string, object>> metadataObject,
            NetworkCredential loginCredentials,
            EasyHttp.eDebugMode debugMode,
            out string statusURL)
        {

            statusURL = string.Empty;
            ErrorMessage = string.Empty;

            if (!File.Exists(Configuration.CLIENT_CERT_FILEPATH))
            {
                ErrorMessage = "Authentication failure; cert file not found at " + Configuration.CLIENT_CERT_FILEPATH;
                RaiseErrorEvent("StartUpload", ErrorMessage);
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

                // newFileObj.Add(fiObj.SerializeToDictionaryObject());

            }


            Configuration.UseTestInstance = UseTestInstance;

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
                // Copy the Metadata.txt file to the transfer folder
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
                RaiseDebugEvent("ProcessMetadata", "File list is empty; nothing to do");
                RaiseUploadCompleted(string.Empty);
                return true;
            }

            var location = "upload";
            var serverUri = "https://ServerIsOffline/dummy_page?test";

            if (debugMode == EasyHttp.eDebugMode.MyEMSLOfflineMode)
            {
                RaiseDebugEvent("ProcessMetadata", "Creating .tar file locally");
            }
            else
            {
                serverUri = Configuration.IngestServerUri;

                var storageUrl = serverUri + "/" + location;

                RaiseDebugEvent("ProcessMetadata", "Sending file to " + storageUrl);
            }

            var responseData = EasyHttp.SendFileListToIngester(
                location, serverUri, fileListObject, mdTextFile.FullName, debugMode);

            if (debugMode != EasyHttp.eDebugMode.DebugDisabled)
            {
                // A .tar file was created locally; it was not sent to the server
                return false;
            }

            var responseJSON = Utilities.JsonToObject(responseData);

            var transactionID = Convert.ToInt32(responseJSON["job_id"].ToString());

            statusURL = Configuration.IngestServerUri + "/get_state?job_id=" + transactionID;

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
                    statusResult = EasyHttp.Send(statusURL, out HttpStatusCode responseStatusCode);
                }

                var statusJSON = Utilities.JsonToObject(statusResult);


                if (statusJSON["state"].ToString().ToLower() == "ok")
                {
                    success = true;
                    RaiseUploadCompleted(statusURL);
                }
                else if (statusJSON["state"].ToString().ToLower() == "failed")
                {
                    RaiseErrorEvent("StartUpload", "Upload failed during ingest process");
                    RaiseUploadCompleted(statusResult);
                    success = false;
                }
                else if (statusJSON["state"].ToString().ToLower().Contains("error"))
                {
                    RaiseErrorEvent("StartUpload", "Ingester Backend is offline or having issues");
                    RaiseUploadCompleted(statusResult);
                    success = false;
                }

            }
            catch (Exception ex)
            {
                OnError("StartUpload", "Exception examining the MyEMSL response string: " + ex.Message);
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
                    { "value", uploadMetadata.DatasetName }
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
                // Could associate an EUS Instrument ID here
                // private const string DATA_PACKAGE_EUS_INSTRUMENT_ID = "40000";
                // string eusInstrumentID = GetEUSInstrumentID(uploadMetadata.EUSInstrumentID, DATA_PACKAGE_EUS_INSTRUMENT_ID);
                // groupObject.Add(new Dictionary<string, string> { { "name", eusInstrumentID }, { "type", "omics.dms.instrument_id" } });

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
                eusInfo.EUSInstrumentID = StringToInt(eusInstrumentID, 0);
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
                var subdirString = "data".Trim('/') + "/" + f.RelativeDestinationDirectory.Trim('/');
                metadataObject.Add(new Dictionary<string, object> {
                    { "destinationTable", "Files" },
                    { "name", f.FileName },
                    { "absolutelocalpath", f.AbsoluteLocalPath},
                    { "subdir", subdirString.Trim('/') },
                    { "size", f.FileSizeInBytes.ToString() },
                    { "hashsum", f.Sha1HashHex },
                    { "mimetype", "application/octet-stream" },
                    { "hashtype", "sha1" },
                    { "ctime", f.CreationTime.ToUniversalTime().ToString("s") },
                    { "mtime", f.CreationTime.ToUniversalTime().ToString("s") }
                });
            }

            return metadataObject;

        }


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
        public static Dictionary<string, object> CreateMetadataObject(
            UploadMetadata uploadMetadata,
            List<FileInfoObject> lstUnmatchedFiles,
            out EUSInfo eusInfo)
        {
            var metadataObject = new Dictionary<string, object>();
            var groupObject = new List<Dictionary<string, string>>();

            // Lookup the EUS_Instrument_ID
            // If empty, use 34127, which is VOrbiETD04
            var eusInstrumentID = GetEUSInstrumentID(uploadMetadata.EUSInstrumentID, UNKNOWN_INSTRUMENT_EUS_INSTRUMENT_ID);

            eusInfo = new EUSInfo();
            eusInfo.Clear();

            // Set up the MyEMSL tagging information

            if (uploadMetadata.DatasetID > 0)
            {
                groupObject.Add(new Dictionary<string, string> {
                    { "name", uploadMetadata.DMSInstrumentName },
                    { "type", "omics.dms.instrument" } });

                groupObject.Add(new Dictionary<string, string> {
                    { "name", eusInstrumentID },
                    { "type", "omics.dms.instrument_id" } });

                groupObject.Add(new Dictionary<string, string> {
                    { "name", uploadMetadata.DateCodeString },
                    { "type", "omics.dms.date_code" } });

                groupObject.Add(new Dictionary<string, string> {
                    { "name", uploadMetadata.DatasetName },
                    { "type", "omics.dms.dataset" } });

                groupObject.Add(new Dictionary<string, string> {
                    { "name", uploadMetadata.DatasetID.ToString(CultureInfo.InvariantCulture) },
                    { "type", "omics.dms.dataset_id" } });
            }
            else if (uploadMetadata.DataPackageID > 0)
            {
                // Could associate an EUS Instrument ID here
                // private const string DATA_PACKAGE_EUS_INSTRUMENT_ID = "40000";
                // string eusInstrumentID = GetEUSInstrumentID(uploadMetadata.EUSInstrumentID, DATA_PACKAGE_EUS_INSTRUMENT_ID);
                // groupObject.Add(new Dictionary<string, string> { { "name", eusInstrumentID }, { "type", "omics.dms.instrument_id" } });

                groupObject.Add(new Dictionary<string, string> {
                    { "name", uploadMetadata.DataPackageID.ToString(CultureInfo.InvariantCulture) },
                    { "type", "omics.dms.datapackage_id" } });
            }
            else
            {
                // ReSharper disable once NotResolvedInText
                throw new ArgumentOutOfRangeException("Must define a DatasetID or a DataPackageID; cannot create the metadata object");
            }

            var eusInfoMap = new Dictionary<string, object>
            {
                {"groups", groupObject}
            };

            if (uploadMetadata.DatasetID > 0)
            {
                eusInfoMap.Add("instrumentId", eusInstrumentID);
                eusInfo.EUSInstrumentID = StringToInt(eusInstrumentID, 0);

                eusInfoMap.Add("instrumentName", uploadMetadata.DMSInstrumentName);
            }

            // All bundles pushed into MyEMSL must have an associated Proposal ID
            if (string.IsNullOrWhiteSpace(uploadMetadata.EUSProposalID))
            {
                // This dataset or data package does not have an EUS_Proposal_ID
                eusInfo.EUSProposalID = DEFAULT_EUS_PROPOSAL_ID;
            }
            else
            {
                eusInfo.EUSProposalID = uploadMetadata.EUSProposalID;
            }

            eusInfoMap.Add("proposalID", eusInfo.EUSProposalID);

            // For datasets, eusOperatorID is the instrument operator EUS ID
            // For data packages, it is the EUS ID of the data package owner
            if (uploadMetadata.EUSOperatorID == 0)
            {
                // This should have already been flagged in upstream code
                // But if we reach this function and it is still 0, we will use the default operator ID
                eusInfo.EUSInstrumentID = DEFAULT_EUS_OPERATOR_ID;
            }
            else
            {
                eusInfo.EUSUploaderID = uploadMetadata.EUSOperatorID;
            }

            // Store the instrument operator EUS ID in the uploaderEusId field to indicate
            // the person on whose behalf the capture task manager is uploading the dataset
            eusInfoMap.Add("uploaderEusId", eusInfo.EUSUploaderID.ToString());

            metadataObject.Add("bundleName", "omics_dms");
            metadataObject.Add("creationDate", DateTime.UtcNow.ToUnixTime().ToString(CultureInfo.InvariantCulture));
            metadataObject.Add("eusInfo", eusInfoMap);

            metadataObject.Add("file", lstUnmatchedFiles);

            metadataObject.Add("version", "1.2.0");

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
        private static string GetEUSInstrumentID(string eusInstrumentId, string instrumentIdIfUnknown)
        {
            return string.IsNullOrWhiteSpace(eusInstrumentId) ? instrumentIdIfUnknown : eusInstrumentId;
        }

        #endregion

        private static int StringToInt(string valueText, int defaultValue)
        {
            if (int.TryParse(valueText, out int value))
                return value;

            return defaultValue;
        }
    }

}
