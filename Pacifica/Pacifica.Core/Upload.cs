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
        /// Default EUS ID is "Monroe, Matthew"
        /// </summary>
        public const int DEFAULT_EUS_OPERATOR_ID = 43428;

        // Proposal ID 17797 is "Development of High Throughput Proteomic Production Operations"
        // It is a string because it may contain suffix letters
        public const string DEFAULT_EUS_PROPOSAL_ID = "17797";

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
            /// EUS ID of the instrument operator (for datasets) or the data package owner (for Data Packages)
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
            public string EUSProposalID;		// Originally only used by datasets. Used by Data Packages starting in October 2016 since required by policy

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
			var fileList = Utilities.GetFileListFromMetadataObject(metadataObject);
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


            Configuration.UseTestInstance = UseTestInstance;

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

            //NetworkCredential newCred = null;
            //if (loginCredentials != null)
            //{
            //    newCred = new NetworkCredential(loginCredentials.UserName,
            //                                    loginCredentials.Password, loginCredentials.Domain);
            //}

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

            var location = "upload";
            var serverUri = "https://ServerIsOffline/dummy_page?test";
            //var timeoutSeconds = 30;
            var postData = string.Empty;
            HttpStatusCode responseStatusCode;

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
            Dictionary<string, object> responseJSON = Utilities.JsonToObject(responseData);
            //var jsa = (Jayrock.Json.JsonArray)Jayrock.Json.Conversion.JsonConvert.Import(responseData);
            //var responseJSON = Utilities.JsonArrayToDictionaryList(jsa);

            //Dictionary<string, object> responseJSON = (Dictionary<string, object>)Jayrock.Json.Conversion.JsonConvert.Import(responseData);

            int transactionID = Convert.ToInt32(responseJSON["job_id"].ToString());

            statusURL = Configuration.IngestServerUri+ "/get_state?job_id=" + transactionID;

            var success = false;
           // var finishError = false;
            var finishErrorMsg = string.Empty;

            try
            {
                var statusResult = EasyHttp.Send(statusURL, out responseStatusCode);

                //var jsaResult = (Jayrock.Json.JsonArray)Jayrock.Json.Conversion.JsonConvert.Import(statusResult);
                //var statusJSON = Utilities.JsonArrayToDictionaryList(jsaResult);

                Dictionary<string, object> statusJSON = Utilities.JsonToObject(statusResult);

                if ( statusJSON["state"].ToString().ToLower() == "ok")
                {
                    success = true;
                    RaiseUploadCompleted(statusURL);
                }else if(statusJSON["state"].ToString().ToLower() == "failed")
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

                // The finish CGI script returns "Status:[URL]\nAccepted\n" on success...
                // This RegEx looks for Accepted in the text, optionally preceded by a Status: line
                //var reStatusURL = new Regex(@"(^Status:(?<url>.*)\n)?(?<accepted>^Accepted)\n", RegexOptions.Multiline);

                //    var reErrorMessage = new Regex(@"^\[Errno \d+.+\n", RegexOptions.Multiline);
                //    var reErrorMatch = reErrorMessage.Match(finishResult);

                //    reMatch = reStatusURL.Match(finishResult);
                //    if (reMatch.Groups["accepted"].Success && !reMatch.Groups["url"].Success)
                //    {
                //        // File was accepted, but the Status URL is empty
                //        // This likely indicates a problem
                //        ErrorMessage = "File was accepted, but the Status URL is empty; " + finishResult;
                //        RaiseErrorEvent("StartUpload", ErrorMessage);
                //        RaiseUploadCompleted(finishResult);

                //        // ReSharper disable once RedundantAssignment
                //        success = false;
                //    }
                //    else if (reMatch.Groups["accepted"].Success && reMatch.Groups["url"].Success)
                //    {
                //        statusURL = reMatch.Groups["url"].Value.Trim();

                //        if (statusURL.EndsWith("/1323420608"))
                //        {
                //            // This URL is always returned when an error occurs
                //            ErrorMessage = "File was accepted, but the status URL is 1323420608 (indicates upload error): " + finishResult.Replace("\n", " \\n ");
                //            RaiseErrorEvent("StartUpload", ErrorMessage);
                //            success = false;
                //        }
                //        else
                //        {
                //            if (reErrorMatch.Success)
                //            {
                //                ErrorMessage = "File was accepted, but an error message was reported: " + finishResult;
                //                RaiseErrorEvent("StartUpload", ErrorMessage);
                //                success = false;
                //            }
                //            else
                //            {
                //                success = true;
                //            }
                //        }

                //        RaiseUploadCompleted(statusURL);

                //    }
                //    else
                //    {
                //        Utilities.Logout(mCookieJar);
                //        ErrorMessage = "Upload failed with message: " + finishResult;
                //        finishError = true;
                //        finishErrorMsg = finishUrl + " failed with message: " + finishResult;
                //    }

                }
                catch (Exception ex)
                {
                ErrorMessage = "Exception calling MyEMSL finish: " + ex.Message;
                //finishError = true;
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

            //if (finishError)
            //{
            //    RaiseErrorEvent("Upload.StartUpload", ErrorMessage);
            //    throw new ApplicationException(finishErrorMsg);
            //}

            //Utilities.Logout(mCookieJar);
            return success;
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
        //public bool StartUploadOld(
        //    List<Dictionary<string, object>> metadataObject,
        //    NetworkCredential loginCredentials,
        //    EasyHttp.eDebugMode debugMode,
        //    out string statusURL)
        //{
        //    statusURL = string.Empty;
        //    ErrorMessage = string.Empty;

        //    Configuration.UseTestInstance = UseTestInstance;

            //var fileList = (List<FileInfoObject>)metadataObject["file"];ƒget
            //var fileList = 
            // Grab the list of files from the top-level "file" object
            // Keys in this dictionary are the source file path; values are metadata about the file
            // var fileListObject = new SortedDictionary<string, FileInfoObject>();

            // This is a list of dictionary objects
            // Dictionary keys will be sha1Hash, destinationDirectory, and fileName
            // var newFileObj = new List<Dictionary<string, string>>();
            //
            // foreach (var file in fileList)
            // {
            //
            //     var fiObj = new FileInfoObject(file.AbsoluteLocalPath, file.RelativeDestinationDirectory, file.Sha1HashHex);
            //
            //     fileListObject.Add(file.AbsoluteLocalPath, fiObj);
            //     newFileObj.Add(fiObj.SerializeToDictionaryObject());
            //
            // }
            //
            // metadataObject["file"] = newFileObj;

            //var mdJson = Utilities.ObjectToJson(metadataObject);

            // Create the metadata.txt file
        //    var metadataFilename = Path.GetTempFileName();
        //    var mdTextFile = new FileInfo(metadataFilename);
        //    using (var sw = mdTextFile.CreateText())
        //    {
        //        sw.Write(mdJson);
        //    }

        //    try
        //    {
        //        // Copy the Metadata.txt file to the transfer folder, then delete the local copy
        //        if (!string.IsNullOrWhiteSpace(TransferFolderPath))
        //        {
        //            var fiTargetFile = new FileInfo(Path.Combine(TransferFolderPath, Utilities.GetMetadataFilenameForJob(JobNumber)));
        //            if (fiTargetFile.Directory != null && !fiTargetFile.Directory.Exists)
        //                fiTargetFile.Directory.Create();

        //            mdTextFile.CopyTo(fiTargetFile.FullName, true);
        //        }

        //    }
        //    // ReSharper disable once EmptyGeneralCatchClause
        //    catch
        //    {
        //        // Ignore errors here
        //    }


        //    if (fileList.Count == 0)
        //    {
        //        RaiseDebugEvent("ProcessMetadata", "File list is empty; nothing to do");
        //        RaiseUploadCompleted(string.Empty);
        //        return true;
        //    }

        //    NetworkCredential newCred = null;
        //    if (loginCredentials != null)
        //    {
        //        newCred = new NetworkCredential(loginCredentials.UserName,
        //                                        loginCredentials.Password, loginCredentials.Domain);
        //    }

        //    // The following Callback allows us to access the MyEMSL server even if the certificate is expired or untrusted
        //    // This hack was added in March 2014 because Proto-10 reported error
        //    //   "Could not establish trust relationship for the SSL/TLS secure channel"
        //    //   when accessing https://my.emsl.pnl.gov/
        //    // This workaround requires these two using statements:
        //    //   using System.Net.Security;
        //    //   using System.Security.Cryptography.X509Certificates;

        //    // Could use this to ignore all certificates (not wise)
        //    // System.Net.ServicePointManager.ServerCertificateValidationCallback = (sender, certificate, chain, sslPolicyErrors) => true;

        //    // Instead, only allow certain domains, as defined by ValidateRemoteCertificate
        //    if (ServicePointManager.ServerCertificateValidationCallback == null)
        //        ServicePointManager.ServerCertificateValidationCallback += Utilities.ValidateRemoteCertificate;

        //    // Call the testauth service to obtain a cookie for this session
        //    // var authURL = Configuration.TestAuthUri;
        //    // var auth = new Auth(new Uri(authURL));

        //    // mCookieJar = null;
        //    var location = "Undefined/Location";
        //    var serverUri = "https://ServerIsOffline/dummy_page?test";
        //    var timeoutSeconds = 30;
        //    var postData = string.Empty;
        //    Match reMatch;
        //    HttpStatusCode responseStatusCode;

        //    if (debugMode == EasyHttp.eDebugMode.MyEMSLOfflineMode)
        //    {
        //        RaiseDebugEvent("ProcessMetadata", "Creating .tar file locally");
        //    }
        //    else
        //    {
        //        // if (!auth.GetAuthCookies(out mCookieJar))
        //        // {
        //        //     ErrorMessage = "Auto-login to " + Configuration.TestAuthUri + " failed authentication";
        //        //     RaiseErrorEvent("Upload.StartUpload", ErrorMessage);
        //        //     throw new ApplicationException(ErrorMessage);
        //        // }

        //        var redirectedServer = Configuration.IngestServerUri;
        //        // var preallocateUrl = redirectedServer + "/myemsl/cgi-bin/preallocate";
        //        //
        //        // RaiseDebugEvent("ProcessMetadata", "Preallocating with " + preallocateUrl);
        //        //
        //        // var preallocateReturn = EasyHttp.Send(preallocateUrl, mCookieJar,
        //        //                                          out responseStatusCode, postData,
        //        //                                          EasyHttp.HttpMethod.Get, timeoutSeconds, newCred);

        //        string scheme;

        //        // This is just a local configuration that states which is preferred.
        //        // It doesn't inform what is supported on the server.
        //        if (Configuration.UseSecureDataTransfer)
        //        {
        //            scheme = Configuration.SecuredScheme; // https
        //        }
        //        else
        //        {
        //            scheme = Configuration.UnsecuredScheme; // http
        //        }
        //        //
        //        // string server;
        //        // var reServerName = new Regex(@"^Server:[\t ]*(?<server>.*)$", RegexOptions.Multiline);
        //        // reMatch = reServerName.Match(preallocateReturn);
        //        //
        //        // if (reMatch.Success)
        //        // {
        //        //     server = reMatch.Groups["server"].Value.Trim();
        //        // }
        //        // else
        //        // {
        //        //     Utilities.Logout(mCookieJar);
        //        //     ErrorMessage = "Preallocate did not return a server: " + preallocateReturn;
        //        //     RaiseErrorEvent("Upload.StartUpload", ErrorMessage);
        //        //     throw new ApplicationException(string.Format("Preallocate {0} did not return a server.", preallocateUrl));
        //        // }

        //        string server = redirectedServer;

        //        // var reLocation = new Regex(@"^Location:[\t ]*(?<loc>.*)$", RegexOptions.Multiline);

        //        // reMatch = reLocation.Match(preallocateReturn);
        //        // if (reMatch.Success)
        //        // {
        //        //     location = reMatch.Groups["loc"].Value.Trim();
        //        // }
        //        // else
        //        // {
        //        //     Utilities.Logout(mCookieJar);
        //        //     ErrorMessage = "Preallocate did not return a location: " + preallocateReturn;
        //        //     RaiseErrorEvent("Upload.StartUpload", ErrorMessage);
        //        //     throw new ApplicationException(string.Format("Preallocate {0} did not return a location.", preallocateUrl));
        //        // }

        //        serverUri = scheme + "://" + server;

        //        var storageUrl = serverUri + location;

        //        RaiseDebugEvent("ProcessMetadata", "Sending file to " + storageUrl);
        //    }

        //    // Note: The response data is usually empty
        //    // The success/failure of the upload is determined via the call to Finish
        //    var responseData = EasyHttp.SendFileListToDavAsTar(location, serverUri, fileListObject, mdTextFile.FullName, mCookieJar, newCred, debugMode);

        //    if (debugMode != EasyHttp.eDebugMode.DebugDisabled)
        //    {
        //        // A .tar file was created locally; it was not sent to the server
        //        // Thus, do not call "finish"
        //        // Instead, simply logout
        //        Utilities.Logout(mCookieJar);
        //        return false;
        //    }

        //    // Upload succeeded; call the finish URL
        //    // This call is typically fast, but has been observed to take well over 10 seconds
        //    // For safety, using a timeout of 5 minutes
        //    var finishUrl = serverUri + "/myemsl/cgi-bin/finish" + location;
        //    RaiseDebugEvent("ProcessMetadata", "Sending finish via " + finishUrl);
        //    timeoutSeconds = 300;
        //    postData = string.Empty;

        //    var success = false;
        //    var finishError = false;
        //    var finishErrorMsg = string.Empty;

        //    try
        //    {
        //        var finishResult = EasyHttp.Send(finishUrl, mCookieJar,
        //                                            out responseStatusCode, postData,
        //                                            EasyHttp.HttpMethod.Get, timeoutSeconds, newCred);


        //        // The finish CGI script returns "Status:[URL]\nAccepted\n" on success...
        //        // This RegEx looks for Accepted in the text, optionally preceded by a Status: line
        //        var reStatusURL = new Regex(@"(^Status:(?<url>.*)\n)?(?<accepted>^Accepted)\n", RegexOptions.Multiline);

        //        var reErrorMessage = new Regex(@"^\[Errno \d+.+\n", RegexOptions.Multiline);
        //        var reErrorMatch = reErrorMessage.Match(finishResult);

        //        reMatch = reStatusURL.Match(finishResult);
        //        if (reMatch.Groups["accepted"].Success && !reMatch.Groups["url"].Success)
        //        {
        //            // File was accepted, but the Status URL is empty
        //            // This likely indicates a problem
        //            ErrorMessage = "File was accepted, but the Status URL is empty; " + finishResult;
        //            RaiseErrorEvent("StartUpload", ErrorMessage);
        //            RaiseUploadCompleted(finishResult);

        //            // ReSharper disable once RedundantAssignment
        //            success = false;
        //        }
        //        else if (reMatch.Groups["accepted"].Success && reMatch.Groups["url"].Success)
        //        {
        //            statusURL = reMatch.Groups["url"].Value.Trim();

        //            if (statusURL.EndsWith("/1323420608"))
        //            {
        //                // This URL is always returned when an error occurs
        //                ErrorMessage = "File was accepted, but the status URL is 1323420608 (indicates upload error): " + finishResult.Replace("\n", " \\n ");
        //                RaiseErrorEvent("StartUpload", ErrorMessage);
        //                success = false;
        //            }
        //            else
        //            {
        //                if (reErrorMatch.Success)
        //                {
        //                    ErrorMessage = "File was accepted, but an error message was reported: " + finishResult;
        //                    RaiseErrorEvent("StartUpload", ErrorMessage);
        //                    success = false;
        //                }
        //                else
        //                {
        //                    success = true;
        //                }
        //            }

        //            RaiseUploadCompleted(statusURL);

        //        }
        //        else
        //        {
        //            Utilities.Logout(mCookieJar);
        //            ErrorMessage = "Upload failed with message: " + finishResult;
        //            finishError = true;
        //            finishErrorMsg = finishUrl + " failed with message: " + finishResult;
        //        }

        //    }
        //    catch (Exception ex)
        //    {
        //        Utilities.Logout(mCookieJar);
        //        ErrorMessage = "Exception calling MyEMSL finish: " + ex.Message;
        //        finishError = true;
        //        finishErrorMsg = ErrorMessage + ", StackTrace: " + PRISM.clsStackTraceFormatter.GetExceptionStackTrace(ex);
        //    }

        //    try
        //    {
        //        // Delete the local temporary file
        //        mdTextFile.Delete();
        //    }
        //    // ReSharper disable once EmptyGeneralCatchClause
        //    catch
        //    {
        //        // Ignore errors here
        //    }

        //    if (finishError)
        //    {
        //        RaiseErrorEvent("Upload.StartUpload", ErrorMessage);
        //        throw new ApplicationException(finishErrorMsg);
        //    }

        //    Utilities.Logout(mCookieJar);
        //    return success;
        //}

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
            udtUploadMetadata uploadMetadata,
            List<FileInfoObject> lstUnmatchedFiles,
            out udtEUSInfo eusInfo)
        {
            eusInfo = new udtEUSInfo();
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
                    { "key", "omics.dms.dataset_id" },
                    { "value", uploadMetadata.DatasetID.ToString(CultureInfo.InvariantCulture) }
                });

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
            foreach ( FileInfoObject f in lstUnmatchedFiles ){
                metadataObject.Add(new Dictionary<string, object> {
                    { "destinationTable", "Files" },
                    { "name", f.FileName },
					{ "absolutelocalpath", f.AbsoluteLocalPath},
                    { "subdir", f.RelativeDestinationDirectory },
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
            udtUploadMetadata uploadMetadata,
            List<FileInfoObject> lstUnmatchedFiles,
            out udtEUSInfo eusInfo)
        {
            var metadataObject = new Dictionary<string, object>();
            var groupObject = new List<Dictionary<string, string>>();

            // Lookup the EUS_Instrument_ID
            // If empty, use 34127, which is VOrbiETD04
            var eusInstrumentID = GetEUSInstrumentID(uploadMetadata.EUSInstrumentID, UNKNOWN_INSTRUMENT_EUS_INSTRUMENT_ID);

            eusInfo = new udtEUSInfo();
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
            //object fileListObject;
            var description = new List<string>();
            //var instrumentIdIncluded = false;
            string value;
            string keyname;
            string keyvalue;
            string tableName;
            var kvLookup = new Dictionary<string, object>();
            int fileCount = 0;
            object fileSize = 0;

            kvLookup.Add("omics.dms.dataset_id", "Dataset_ID");
            kvLookup.Add("omics.dms.datapackage_id", "DataPackage_ID");
            kvLookup.Add("omics.dms.instrument", "DMS_Instrument");
            kvLookup.Add("omics.dms.instrument_id", "EUS_Instrument_ID");
            kvLookup.Add("Transactions.proposal", "EUS_Proposal_ID");
            kvLookup.Add("Transactions.submitter", "EUS_User_ID");
            kvLookup.Add("Transactions.instrument", "EUS_Instrument_ID");


            foreach (Dictionary<string, object> item in metadataObject)
            {
                if (GetDictionaryValue(item, "destinationTable", out value))
                {
                    tableName = value;
                    if (tableName == "TransactionKeyValue")
                    {
                        if (GetDictionaryValue(item, "key", out value))
                        {
                            keyname = value;
                            if (GetDictionaryValue(item, "value", out value))
                            {
                                keyvalue = value;
                                if(GetDictionaryValue(kvLookup, keyname, out value))
                                {
									description.Add(value + "=" + keyvalue);
                                }

                            }
                        }

                    }else if (tableName == "Files")
                    {
                        if (item.TryGetValue("size", out fileSize)) {
                            fileCount += 1;
                        }

                    }
                }
            }
            return string.Join("; ", description);

        }


        /// <summary>
        /// Return a string description of the EUS info encoded by metadataObject
        /// </summary>
        /// <param name="metadataObject"></param>
        /// <returns></returns>
        public static string GetMetadataObjectDescriptionOld(Dictionary<string, object> metadataObject)
        {
            object eusInfoMapObject;
            object groupObject;
            object fileListObject;

            if (!metadataObject.TryGetValue("eusInfo", out eusInfoMapObject))
            {
                return "Error: [eusInfo] dictionary key not found";
            }

            var eusInfoMapDict = eusInfoMapObject as Dictionary<string, object>;
            if (eusInfoMapDict == null)
            {
                return "Error: [eusInfo] dictionary was not in the correct format";
            }

            if (!eusInfoMapDict.TryGetValue("groups", out groupObject)) {
                return "Error: [eusInfo.groups] dictionary key not found";
            }

            var groupObjects = groupObject as List<Dictionary<string, string>>;
            if (groupObjects == null)
            {
                return "Error: [eusInfo.groups] was not in the correct format";
            }

            var description = new List<string>();
            var instrumentIdIncluded = false;

            string value;

            if (GetMetadataGroupValue(groupObjects, "omics.dms.dataset_id", out value))
                description.Add("Dataset_ID=" + value);

            if (GetMetadataGroupValue(groupObjects, "omics.dms.datapackage_id", out value))
                description.Add("DataPackage_ID=" + value);

            if (GetMetadataGroupValue(groupObjects, "omics.dms.instrument", out value))
                description.Add("DMS_Instrument=" + value);

            if (GetMetadataGroupValue(groupObjects, "omics.dms.instrument_id", out value))
            {
                description.Add("EUS_Instrument_ID=" + value);
                instrumentIdIncluded = true;
            }

            if (!instrumentIdIncluded && GetDictionaryValue(eusInfoMapDict, "instrumentId", out value))
            {
                // EUS Instrument Id is stored both in eusInfoMapDict and in groupObjects
                description.Add("EUS_Instrument_ID=" + value);
            }

            if (GetDictionaryValue(eusInfoMapDict, "proposalID", out value))
                description.Add("EUS_Proposal_ID=" + value);

            if (GetDictionaryValue(eusInfoMapDict, "uploaderEusId", out value))
                description.Add("EUS_User_ID=" + value);

            if (!metadataObject.TryGetValue("file", out fileListObject))
            {
                return "Error: [file] dictionary key not found";
            }

            var fileList = fileListObject as List<Pacifica.Core.FileInfoObject>;
            if (fileList == null)
            {
                return "Error: [file] list was not in the correct format";
            }

            description.Add("FileCount=" + fileList.Count);

            return string.Join("; ", description);
        }

        private static bool GetDictionaryValue(IReadOnlyDictionary<string, object> eusInfoMapObject, string keyName, out string matchedValue)
        {
            object value;
            if (eusInfoMapObject.TryGetValue(keyName, out value))
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
                string candidateKeyName;

                if (!groupDefinition.TryGetValue("type", out candidateKeyName))
                    continue;

                if (!string.Equals(candidateKeyName, groupKeyName))
                    continue;

                // Found the desired dictionary entry
                string value;
                if (!groupDefinition.TryGetValue("name", out value))
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
            int value;
            if (int.TryParse(valueText, out value))
                return value;

            return defaultValue;
        }
    }

}
