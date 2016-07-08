using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Net;
using System.Text.RegularExpressions;

namespace Pacifica.Core
{
    public class Upload : IUpload
    {
        /// <summary>
        /// Deafult EUS ID is "Monroe, Matthew"
        /// </summary>
        public const int DEFAULT_EUS_OPERATOR_ID = 43428;

        private const string DATA_PACKAGE_EUS_INSTRUMENT_ID = "40000";

        // VOrbiETD04 is 34127
        private const string UNKNOWN_INSTRUMENT_EUS_INSTRUMENT_ID = "34127";

        public struct udtEUSInfo
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
            /// EUS ID of the instrument operator
            /// </summary>
            public int EUSUploaderID;

            public void Clear()
            {
                EUSInstrumentID = 0;
                EUSProposalID = string.Empty;
                EUSUploaderID = 0;
            }
        }

        public struct udtUploadMetadata
        {
            public int DatasetID;               // 0 for data packages
            public int DataPackageID;
            public string SubFolder;
            public string DatasetName;			// Only used for datasets; not Data Packages
            public string DateCodeString;		// Only used for datasets; not Data Packages
            public string DMSInstrumentName;	// Only used for datasets; not Data Packages
            public string EUSInstrumentID;		// Only used for datasets; not Data Packages
            public string EUSProposalID;		// Only used for datasets; not Data Packages

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
                EUSInstrumentID = string.Empty;
                EUSProposalID = string.Empty;
                EUSOperatorID = DEFAULT_EUS_OPERATOR_ID;
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

        private CookieContainer mCookieJar;

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
        /// <param name="transferFolderPath">Transfer foler path for this dataset, for example \\proto-4\DMS3_Xfer\SysVirol_IFT001_Pool_17_B_10x_27Aug13_Tiger_13-07-36</param>
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
            if (DebugEvent != null)
            {
                DebugEvent(this, new MessageEventArgs(callingFunction, currentTask));
            }
        }

        private void RaiseErrorEvent(string callingFunction, string errorMessage)
        {
            if (ErrorEvent != null)
            {
                ErrorEvent(this, new MessageEventArgs(callingFunction, errorMessage));
            }
        }


        void EasyHttp_StatusUpdate(object sender, StatusEventArgs e)
        {
            if (StatusUpdate != null)
            {
                StatusUpdate(this, e);
            }
        }

        private void RaiseUploadCompleted(string serverResponse)
        {
            if (UploadCompleted != null)
            {
                UploadCompleted(this, new UploadCompletedEventArgs(serverResponse));
            }
        }

        #endregion

        #region IUpload Members

        public bool StartUpload(Dictionary<string, object> metadataObject, out string statusURL)
        {
            NetworkCredential loginCredentials = null;
            const EasyHttp.eDebugMode debugMode = EasyHttp.eDebugMode.DebugDisabled;

            // ReSharper disable once ExpressionIsAlwaysNull
            return StartUpload(metadataObject, loginCredentials, debugMode, out statusURL);
        }


        public bool StartUpload(Dictionary<string, object> metadataObject, EasyHttp.eDebugMode debugMode, out string statusURL)
        {
            NetworkCredential loginCredentials = null;

            // ReSharper disable once ExpressionIsAlwaysNull
            return StartUpload(metadataObject, loginCredentials, debugMode, out statusURL);
        }

        public bool StartUpload(Dictionary<string, object> metadataObject, NetworkCredential loginCredentials, out string statusURL)
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
            Dictionary<string, object> metadataObject,
            NetworkCredential loginCredentials,
            EasyHttp.eDebugMode debugMode,
            out string statusURL)
        {
            statusURL = string.Empty;
            ErrorMessage = string.Empty;

            Configuration.UseTestInstance = UseTestInstance;

            var fileList = (List<FileInfoObject>)metadataObject["file"];

            // Grab the list of files from the top-level "file" object
            // Keys in this dictionary are the source file path; values are metadata about the file
            var fileListObject = new SortedDictionary<string, FileInfoObject>();

            // This is a list of dictionary objects
            // Dictionary keys will be sha1Hash, destinationDirectory, and fileName
            var newFileObj = new List<Dictionary<string, string>>();

            foreach (var file in fileList)
            {

                var fiObj = new FileInfoObject(file.AbsoluteLocalPath, file.RelativeDestinationDirectory, file.Sha1HashHex);

                fileListObject.Add(file.AbsoluteLocalPath, fiObj);
                newFileObj.Add(fiObj.SerializeToDictionaryObject());

            }

            metadataObject["file"] = newFileObj;

            var mdJson = Utilities.ObjectToJson(metadataObject);

            // Create the metadata.txt file
            var metadataFilename = Path.GetTempFileName();
            var mdTextFile = new FileInfo(metadataFilename);
            using (var sw = mdTextFile.CreateText())
            {
                sw.Write(mdJson);
            }

            try
            {
                // Copy the Metadata.txt file to the transfer folder, then delete the local copy
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

            NetworkCredential newCred = null;
            if (loginCredentials != null)
            {
                newCred = new NetworkCredential(loginCredentials.UserName,
                                                loginCredentials.Password, loginCredentials.Domain);
            }

            // The following Callback allows us to access the MyEMSL server even if the certificate is expired or untrusted
            // This hack was added in March 2014 because Proto-10 reported error 
            //   "Could not establish trust relationship for the SSL/TLS secure channel"
            //   when accessing https://my.emsl.pnl.gov/
            // This workaround requires these two using statements:
            //   using System.Net.Security;
            //   using System.Security.Cryptography.X509Certificates;

            // Could use this to ignore all certificates (not wise)
            // System.Net.ServicePointManager.ServerCertificateValidationCallback = (sender, certificate, chain, sslPolicyErrors) => true;

            // Instead, only allow certain domains, as defined by ValidateRemoteCertificate
            if (ServicePointManager.ServerCertificateValidationCallback == null)
                ServicePointManager.ServerCertificateValidationCallback += Utilities.ValidateRemoteCertificate;

            // Call the testauth service to obtain a cookie for this session
            var authURL = Configuration.TestAuthUri;
            var auth = new Auth(new Uri(authURL));

            mCookieJar = null;
            var location = "Undefined/Location";
            var serverUri = "https://ServerIsOffline/dummy_page?test";
            var timeoutSeconds = 30;
            var postData = string.Empty;
            Match reMatch;
            HttpStatusCode responseStatusCode;

            if (debugMode == EasyHttp.eDebugMode.MyEMSLOfflineMode)
            {
                RaiseDebugEvent("ProcessMetadata", "Creating .tar file locally");
            }
            else
            {
                if (!auth.GetAuthCookies(out mCookieJar))
                {
                    ErrorMessage = "Auto-login to " + Configuration.TestAuthUri + " failed authentication";
                    RaiseErrorEvent("Upload.StartUpload", ErrorMessage);
                    throw new ApplicationException(ErrorMessage);
                }

                var redirectedServer = Configuration.IngestServerUri;
                var preallocateUrl = redirectedServer + "/myemsl/cgi-bin/preallocate";

                RaiseDebugEvent("ProcessMetadata", "Preallocating with " + preallocateUrl);

                var preallocateReturn = EasyHttp.Send(preallocateUrl, mCookieJar,
                                                         out responseStatusCode, postData,
                                                         EasyHttp.HttpMethod.Get, timeoutSeconds, newCred);

                string scheme;

                // This is just a local configuration that states which is preferred.
                // It doesn't inform what is supported on the server.
                if (Configuration.UseSecureDataTransfer)
                {
                    scheme = Configuration.SecuredScheme; // https
                }
                else
                {
                    scheme = Configuration.UnsecuredScheme; // http
                }

                string server;
                var reServerName = new Regex(@"^Server:[\t ]*(?<server>.*)$", RegexOptions.Multiline);
                reMatch = reServerName.Match(preallocateReturn);

                if (reMatch.Success)
                {
                    server = reMatch.Groups["server"].Value.Trim();
                }
                else
                {
                    Utilities.Logout(mCookieJar);
                    ErrorMessage = "Preallocate did not return a server: " + preallocateReturn;
                    RaiseErrorEvent("Upload.StartUpload", ErrorMessage);
                    throw new ApplicationException(string.Format("Preallocate {0} did not return a server.", preallocateUrl));
                }

                var reLocation = new Regex(@"^Location:[\t ]*(?<loc>.*)$", RegexOptions.Multiline);

                reMatch = reLocation.Match(preallocateReturn);
                if (reMatch.Success)
                {
                    location = reMatch.Groups["loc"].Value.Trim();
                }
                else
                {
                    Utilities.Logout(mCookieJar);
                    ErrorMessage = "Preallocate did not return a location: " + preallocateReturn;
                    RaiseErrorEvent("Upload.StartUpload", ErrorMessage);
                    throw new ApplicationException(string.Format("Preallocate {0} did not return a location.", preallocateUrl));
                }

                serverUri = scheme + "://" + server;

                var storageUrl = serverUri + location;

                RaiseDebugEvent("ProcessMetadata", "Sending file to " + storageUrl);
            }

            // Note: The response data is usually empty
            // The success/failure of the upload is determined via the call to Finish
            var responseData = EasyHttp.SendFileListToDavAsTar(location, serverUri, fileListObject, mdTextFile.FullName, mCookieJar, newCred, debugMode);

            if (debugMode != EasyHttp.eDebugMode.DebugDisabled)
            {
                // A .tar file was created locally; it was not sent to the server
                // Thus, do not call "finish"
                // Instead, simply logout
                Utilities.Logout(mCookieJar);
                return false;
            }

            // Upload succeeded; call the finish URL
            // This call is typically fast, but has been observed to take well over 10 seconds
            // For safety, using a timeout of 5 minutes
            var finishUrl = serverUri + "/myemsl/cgi-bin/finish" + location;
            RaiseDebugEvent("ProcessMetadata", "Sending finish via " + finishUrl);
            timeoutSeconds = 300;
            postData = string.Empty;

            var success = false;
            var finishError = false;
            var finishErrorMsg = string.Empty;

            try
            {
                var finishResult = EasyHttp.Send(finishUrl, mCookieJar,
                                                    out responseStatusCode, postData,
                                                    EasyHttp.HttpMethod.Get, timeoutSeconds, newCred);


                // The finish CGI script returns "Status:[URL]\nAccepted\n" on success...
                // This RegEx looks for Accepted in the text, optionally preceded by a Status: line
                var reStatusURL = new Regex(@"(^Status:(?<url>.*)\n)?(?<accepted>^Accepted)\n", RegexOptions.Multiline);

                var reErrorMessage = new Regex(@"^\[Errno \d+.+\n", RegexOptions.Multiline);
                var reErrorMatch = reErrorMessage.Match(finishResult);

                reMatch = reStatusURL.Match(finishResult);
                if (reMatch.Groups["accepted"].Success && !reMatch.Groups["url"].Success)
                {
                    // File was accepted, but the Status URL is empty
                    // This likely indicates a problem
                    ErrorMessage = "File was accepted, but the Status URL is empty; " + finishResult;
                    RaiseErrorEvent("StartUpload", ErrorMessage);
                    RaiseUploadCompleted(finishResult);

                    // ReSharper disable once RedundantAssignment
                    success = false;
                }
                else if (reMatch.Groups["accepted"].Success && reMatch.Groups["url"].Success)
                {
                    statusURL = reMatch.Groups["url"].Value.Trim();

                    if (statusURL.EndsWith("/1323420608"))
                    {
                        // This URL is always returned when an error occurs
                        ErrorMessage = "File was accepted, but the status URL is 1323420608 (indicates upload error): " + finishResult.Replace("\n", " \\n ");
                        RaiseErrorEvent("StartUpload", ErrorMessage);
                        success = false;
                    }
                    else
                    {
                        if (reErrorMatch.Success)
                        {
                            ErrorMessage = "File was accepted, but an error message was reported: " + finishResult;
                            RaiseErrorEvent("StartUpload", ErrorMessage);
                            success = false;
                        }
                        else
                        {
                            success = true;
                        }
                    }

                    RaiseUploadCompleted(statusURL);

                }
                else
                {
                    Utilities.Logout(mCookieJar);
                    ErrorMessage = "Upload failed with message: " + finishResult;
                    finishError = true;
                    finishErrorMsg = finishUrl + " failed with message: " + finishResult;
                }

            }
            catch (Exception ex)
            {
                Utilities.Logout(mCookieJar);
                ErrorMessage = "Exception calling MyEMSL finish: " + ex.Message;
                finishError = true;
                finishErrorMsg = ErrorMessage + ", StackTrace: " + PRISM.clsStackTraceFormatter.GetExceptionStackTrace(ex);
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

            if (finishError)
            {
                RaiseErrorEvent("Upload.StartUpload", ErrorMessage);
                throw new ApplicationException(finishErrorMsg);
            }

            Utilities.Logout(mCookieJar);
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
        /// Dictionary where of the information to translate to JSON; 
        /// Keys are key names; values are either strings or dictionary objects or even a list of dictionary objects 
        /// </returns>
        public static Dictionary<string, object> CreateMetadataObject(
            udtUploadMetadata uploadMetadata,
            List<FileInfoObject> lstUnmatchedFiles,
            out udtEUSInfo eusInfo)
        {
            var metadataObject = new Dictionary<string, object>();
            var groupObject = new List<Dictionary<string, string>>();

            eusInfo = new udtEUSInfo();
            eusInfo.Clear();

            // Set up the MyEMSL tagging information

            if (uploadMetadata.DatasetID > 0)
            {
                groupObject.Add(new Dictionary<string, string> { { "name", uploadMetadata.DMSInstrumentName }, { "type", "omics.dms.instrument" } });

                var eusInstrumentID = GetEUSInstrumentID(uploadMetadata.EUSInstrumentID, UNKNOWN_INSTRUMENT_EUS_INSTRUMENT_ID);

                groupObject.Add(new Dictionary<string, string> { { "name", eusInstrumentID }, { "type", "omics.dms.instrument_id" } });
                groupObject.Add(new Dictionary<string, string> { { "name", uploadMetadata.DateCodeString }, { "type", "omics.dms.date_code" } });
                groupObject.Add(new Dictionary<string, string> { { "name", uploadMetadata.DatasetName }, { "type", "omics.dms.dataset" } });
                groupObject.Add(new Dictionary<string, string> { { "name", uploadMetadata.DatasetID.ToString(CultureInfo.InvariantCulture) }, { "type", "omics.dms.dataset_id" } });
            }
            else if (uploadMetadata.DataPackageID > 0)
            {
                // ToDo: Update DATA_PACKAGE_EUS_INSTRUMENT_ID and uncomment this
                // string eusInstrumentID = GetEUSInstrumentID(uploadMetadata.EUSInstrumentID, DATA_PACKAGE_EUS_INSTRUMENT_ID);                
                // groupObject.Add(new Dictionary<string, string> { { "name", eusInstrumentID }, { "type", "omics.dms.instrument_id" } });

                groupObject.Add(new Dictionary<string, string> { { "name", uploadMetadata.DataPackageID.ToString(CultureInfo.InvariantCulture) }, { "type", "omics.dms.datapackage_id" } });
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
                // Lookup the EUS_Instrument_ID
                // If empty, use 34127, which is VOrbiETD04
                var eusInstrumentID = GetEUSInstrumentID(uploadMetadata.EUSInstrumentID, UNKNOWN_INSTRUMENT_EUS_INSTRUMENT_ID);

                eusInfoMap.Add("instrumentId", eusInstrumentID);
                eusInfo.EUSInstrumentID = StringToInt(eusInstrumentID, 0);

                eusInfoMap.Add("instrumentName", uploadMetadata.DMSInstrumentName);

                if (string.IsNullOrWhiteSpace(uploadMetadata.EUSProposalID))
                {
                    // This dataset does not have an EUS_Proposal_ID
                    // Use 17797, which is "Development of High Throughput Proteomic Production Operations"
                    eusInfo.EUSProposalID = "17797";
                }
                else
                {
                    eusInfo.EUSProposalID = uploadMetadata.EUSProposalID;
                }

                eusInfoMap.Add("proposalID", eusInfo.EUSProposalID);
            }

            // For datasets, eusOperatorID is the instrument operator EUS ID
            // For data packages, it is the EUS ID of the data package owner
            if (uploadMetadata.EUSOperatorID == 0)
            {
                // This should have already been flagged in upstream code
                // But if we reach this function and it is still 0, we will use the default operator ID
                eusInfo.EUSInstrumentID = Upload.DEFAULT_EUS_OPERATOR_ID;
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
            int value;
            if (int.TryParse(valueText, out value))
                return value;

            return defaultValue;
        }
    }

}