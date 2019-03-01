using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Net;
using ICSharpCode.SharpZipLib.Tar;
using PRISM;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Pacifica.Core
{
    public class EasyHttp
    {
        #region "Constants"

        /// <summary>
        /// Response to return when the thread used to send a request aborts
        /// </summary>
        private const string REQUEST_ABORTED_RESPONSE = "(no response, request aborted)";

        /// <summary>
        /// Response to return when the thread used to send a request catches an exception
        /// </summary>
        private const string REQUEST_EXCEPTION_RESPONSE = "(no response, exception caught)";

        /// <summary>
        /// Response to return the Policy server reports Precondition Failed
        /// </summary>
        public const string REQUEST_PRECONDITION_FAILED_RESPONSE = "(no response, precondition failed)";

        /// <summary>
        /// Response to return when a request times out
        /// </summary>
        public const string REQUEST_TIMEOUT_RESPONSE = "(no response, request timed out)";

        /// <summary>
        /// Used to report status
        /// </summary>
        public const string UPLOADING_FILES = "Uploading files";

        /// <summary>
        /// Block size for tar files
        /// </summary>
        private const int TAR_BLOCK_SIZE_BYTES = 512;

        /// <summary>
        /// Metadata file name
        /// </summary>
        public const string MYEMSL_METADATA_FILE_NAME = "metadata.txt";

        #endregion

        #region "Fields"

        private static X509Certificate2 mLoginCertificate;

        private static Thread mThreadedSend;

        private static UrlContactInfo mUrlContactInfo;

        #endregion

        #region "Enums"

        /// <summary>
        /// An enumeration of standard HTTP methods.
        /// </summary>
        /// <remarks>
        /// Use ExtensionMethods.GetDescription
        /// to pull the description value out of this type.
        /// </remarks>
        public enum HttpMethod
        {
            [Description("GET")]
            Get = 0,
            [Description("POST")]
            Post = 1,
            // ReSharper disable once UnusedMember.Global
            [Description("PUT")]
            Put = 2
        }

        /// <summary>
        /// Debug options
        /// </summary>
        public enum eDebugMode
        {
            [Description("Debugging is disabled")]
            DebugDisabled = 0,
            [Description("Authenticate with MyEMSL, but create a local .tar file")]
            CreateTarLocal = 1,
            [Description("Do not contact MyEMSL; create a local .tar file")]
            MyEMSLOfflineMode = 2
        }

        #endregion

        #region "Events"

        /// <summary>
        /// This event is raised if we are unable to connect to MyEMSL, leading to events
        /// System.Net.WebException: Unable to connect to the remote server
        /// System.Net.Sockets.SocketException: A connection attempt failed because the connected party did not properly respond after a period of time
        /// </summary>
        public static event MessageEventHandler MyEMSLOffline;

        /// <summary>
        /// This event is used by SendFileListToIngester to report upload status
        /// </summary>
        public static event StatusUpdateEventHandler StatusUpdate;

        /// <summary>
        /// Error event
        /// </summary>
        public static event EventNotifier.ErrorEventEventHandler ErrorEvent;

        #endregion

        /// <summary>
        /// Abort thread mThreadedSend
        /// </summary>
        private static void AbortThreadedSendNow()
        {
            try
            {
                mThreadedSend?.Abort();
            }
            catch
            {
                // Ignore errors
            }
        }

        /// <summary>
        /// Retrieve a file
        /// </summary>
        /// <param name="config">Configuration options</param>
        /// <param name="url">URL</param>
        /// <param name="cookies">Cookies</param>
        /// <param name="responseStatusCode">Response status code</param>
        /// <param name="downloadFilePath">Local file path to save the file as</param>
        /// <param name="timeoutSeconds">Timeout, in seconds</param>
        /// <param name="loginCredentials">Login credentials</param>
        /// <returns>True if success, false if an error</returns>
        // ReSharper disable once UnusedMember.Global
        public static bool GetFile(
            Configuration config,
            string url,
            CookieContainer cookies,
            out HttpStatusCode responseStatusCode,
            string downloadFilePath,
            int timeoutSeconds = 100,
            NetworkCredential loginCredentials = null)
        {
            var request = InitializeRequest(config, url, ref cookies, ref timeoutSeconds, loginCredentials);
            responseStatusCode = HttpStatusCode.NotFound;

            // Prepare the request object
            const HttpMethod method = HttpMethod.Get;
            request.Method = method.GetDescription<HttpMethod>();
            request.PreAuthenticate = false;

            // Receive response
            HttpWebResponse response = null;
            try
            {
                request.Timeout = timeoutSeconds * 1000;
                response = (HttpWebResponse)request.GetResponse();
                responseStatusCode = response.StatusCode;

                if (responseStatusCode == HttpStatusCode.OK)
                {
                    // Download the file

                    var responseStream = response.GetResponseStream();

                    if (responseStream == null)
                    {
                        throw new WebException("Response stream is null in GetFile");
                    }

                    var buffer = new byte[32767];
                    using (var outFile = new FileStream(downloadFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))
                    {
                        int bytesRead;
                        while ((bytesRead = responseStream.Read(buffer, 0, buffer.Length)) != 0)
                            outFile.Write(buffer, 0, bytesRead);
                    }

                }
                else
                {
                    throw new WebException(string.Format(
                        "HTTP response code not OK in GetFile: {0}, {1}",
                        response.StatusCode, response.StatusDescription));
                }
            }
            catch (WebException ex)
            {
                HandleWebException(ex, url, out responseStatusCode);
            }
            finally
            {
                ((IDisposable)response)?.Dispose();
            }

            return true;
        }

        /// <summary>
        /// Get the headers for a URL
        /// </summary>
        /// <param name="config">Configuration options</param>
        /// <param name="url">URL</param>
        /// <param name="responseStatusCode">Response status code</param>
        /// <param name="timeoutSeconds">Timeout, in seconds</param>
        /// <returns>Headers</returns>
        // ReSharper disable once UnusedMember.Global
        public static WebHeaderCollection GetHeaders(
            Configuration config,
            string url,
            out HttpStatusCode responseStatusCode,
            int timeoutSeconds = 100)
        {
            return GetHeaders(config, url, new CookieContainer(), out responseStatusCode, timeoutSeconds);
        }

        /// <summary>
        /// Get the headers for a URL
        /// </summary>
        /// <param name="config">Configuration options</param>
        /// <param name="url">URL</param>
        /// <param name="cookies">Cookies</param>
        /// <param name="responseStatusCode"></param>
        /// <param name="timeoutSeconds">Timeout, in seconds</param>
        /// <param name="loginCredentials">Login credentials</param>
        /// <returns>Headers</returns>
        public static WebHeaderCollection GetHeaders(
            Configuration config,
            string url,
            CookieContainer cookies,
            out HttpStatusCode responseStatusCode,
            int timeoutSeconds = 100,
            NetworkCredential loginCredentials = null)
        {
            const double maxTimeoutHours = 0.1;
            var request = InitializeRequest(config, url, ref cookies, ref timeoutSeconds, loginCredentials, maxTimeoutHours);
            responseStatusCode = HttpStatusCode.NotFound;

            // Prepare the request object
            request.Method = "HEAD";
            request.PreAuthenticate = false;

            // Receive response
            HttpWebResponse response = null;
            try
            {
                request.Timeout = timeoutSeconds * 1000;
                response = (HttpWebResponse)request.GetResponse();
                responseStatusCode = response.StatusCode;

                return response.Headers;

            }
            catch (WebException ex)
            {
                HandleWebException(ex, url, out responseStatusCode);

                return null;
            }
            finally
            {
                ((IDisposable)response)?.Dispose();
            }
        }

        private static string GetTrimmedResponseData(Stream responseStream, int maxLines = 20)
        {
            if (responseStream == null)
                return string.Empty;

            var responseData = new StringBuilder();
            if (maxLines < 1)
                maxLines = 1;

            using (var sr = new StreamReader(responseStream))
            {
                var linesRead = 0;
                while (!sr.EndOfStream && linesRead < maxLines)
                {
                    responseData.AppendLine(sr.ReadLine());
                    linesRead++;
                }
            }

            return responseData.ToString();
        }

        private static void HandleWebException(WebException ex, string url)
        {
            var responseData = new WebResponseData();
            HandleWebException(ex, url, responseData);
        }

        private static void HandleWebException(WebException ex, string url, out HttpStatusCode responseStatusCode)
        {
            responseStatusCode = HttpStatusCode.RequestTimeout;

            var responseData = new WebResponseData
            {
                ResponseStatusCode = responseStatusCode
            };

            try
            {
                HandleWebException(ex, url, responseData);
            }
            catch
            {
                responseStatusCode = responseData.ResponseStatusCode;
                throw;
            }

        }

        private static void HandleWebException(WebException ex, string url, WebResponseData responseData)
        {

            responseData.RegisterException(ex);

            if (ex.Response != null)
            {
                var responseStream = ex.Response.GetResponseStream();
                responseData.ResponseText = GetTrimmedResponseData(responseStream);

                responseData.ResponseStatusCode = ((HttpWebResponse)ex.Response).StatusCode;
            }
            else
            {
                if (ex.Message.IndexOf("timed out", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    responseData.ResponseText = REQUEST_TIMEOUT_RESPONSE;
                    responseData.ResponseStatusCode = HttpStatusCode.RequestTimeout;
                }
                else if (ex.Message.IndexOf("Precondition Failed", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    responseData.ResponseText = REQUEST_PRECONDITION_FAILED_RESPONSE;
                    responseData.ResponseStatusCode = HttpStatusCode.PreconditionFailed;
                }
                else
                {
                    responseData.ResponseText = string.Empty;
                }
            }

            if (ex.Message.IndexOf("Unable to connect", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                MyEMSLOffline?.Invoke(null, new MessageEventArgs("HandleWebException", ex.Message));
            }

            if (string.IsNullOrWhiteSpace(responseData.ResponseText))
                throw new Exception("Empty response for " + url + ": " + ex.Message, ex);

            throw new Exception("Response from " + url + ": " + responseData.ResponseText, ex);
        }

        /// <summary>
        /// Initialize a request
        /// </summary>
        /// <param name="config">Configuration options</param>
        /// <param name="url">URL</param>
        /// <param name="cookies">Cookies</param>
        /// <param name="timeoutSeconds">Timeout, in seconds</param>
        /// <param name="loginCredentials">Login credentials</param>
        /// <param name="maxTimeoutHours"></param>
        /// <returns>Web request</returns>
        public static HttpWebRequest InitializeRequest(
            Configuration config,
            string url,
            ref CookieContainer cookies,
            ref int timeoutSeconds,
            NetworkCredential loginCredentials,
            double maxTimeoutHours = 24)
        {

            if (cookies == null)
            {
                cookies = new CookieContainer();
            }

            if (timeoutSeconds < 3)
                timeoutSeconds = 3;

            var maxTimeoutHoursInt = (int)(maxTimeoutHours * 60 * 60);
            if (timeoutSeconds > maxTimeoutHoursInt)
                timeoutSeconds = maxTimeoutHoursInt;

            var urlContactInfo = new UrlContactInfo(config, url, cookies, timeoutSeconds: timeoutSeconds, loginCredentials: loginCredentials);

            var request = InitializeRequest(urlContactInfo);
            return request;
        }

        /// <summary>
        /// Initialize a request
        /// </summary>
        /// <returns></returns>
        private static HttpWebRequest InitializeRequest(UrlContactInfo urlContactInfo)
        {
            urlContactInfo.ResponseData.ResetExceptionInfo();

            var uri = new Uri(urlContactInfo.Url);
            var cleanUserName = Utilities.GetUserName(true);

            var request = (HttpWebRequest)WebRequest.Create(uri);
            urlContactInfo.Config.SetProxy(request);

            if (urlContactInfo.LoginCredentials == null)
            {
                if (mLoginCertificate == null)
                {
                    var certificateFilePath = ResolveCertFile(urlContactInfo.Config, "InitializeRequest", out var errorMessage);

                    if (string.IsNullOrWhiteSpace(certificateFilePath))
                    {
                        throw new Exception(errorMessage);
                    }

                    mLoginCertificate = new X509Certificate2();
                    var password = Utilities.DecodePassword(Configuration.CLIENT_CERT_PASSWORD);
                    mLoginCertificate.Import(certificateFilePath, password, X509KeyStorageFlags.PersistKeySet);
                }
                request.ClientCertificates.Add(mLoginCertificate);
            }
            else
            {
                var c = new CredentialCache
                {
                    { new Uri(urlContactInfo.Url), "Basic", new NetworkCredential(urlContactInfo.LoginCredentials.UserName, urlContactInfo.LoginCredentials.SecurePassword) }
                };
                request.Credentials = c;
            }

            var cookie = new Cookie("user_name", cleanUserName)
            {
                Domain = "pnl.gov"
            };

            urlContactInfo.Cookies.Add(cookie);
            request.CookieContainer = urlContactInfo.Cookies;

            return request;
        }

        /// <summary>
        /// Return True if responseText is one of the custom responses that this class uses to report errors (timeout, abort, or exception)
        /// </summary>
        /// <param name="responseText"></param>
        public static bool IsResponseError(string responseText)
        {
            switch (responseText)
            {
                case REQUEST_ABORTED_RESPONSE:
                case REQUEST_EXCEPTION_RESPONSE:
                case REQUEST_TIMEOUT_RESPONSE:
                    return true;
                default:
                    return false;
            }
        }

        /// <summary>
        /// Report an error
        /// </summary>
        /// <param name="message"></param>
        /// <param name="ex">Exception (allowed to be nothing)</param>
        private static void OnErrorEvent(string message, Exception ex)
        {
            if (ErrorEvent == null)
            {
                ConsoleMsgUtils.ShowError(message, ex, false, false);
            }
            ErrorEvent?.Invoke(message, ex);
        }

        /// <summary>
        /// Report a status update
        /// </summary>
        /// <param name="percentCompleted">Value between 0 and 100</param>
        /// <param name="totalBytesSent">Total bytes to send</param>
        /// <param name="totalBytesToSend">Total bytes sent</param>
        /// <param name="statusMessage">Status message</param>
        private static void RaiseStatusUpdate(
            double percentCompleted, long totalBytesSent,
            long totalBytesToSend, string statusMessage)
        {
            StatusUpdate?.Invoke(null, new StatusEventArgs(percentCompleted, totalBytesSent, totalBytesToSend, statusMessage));
        }

        /// <summary>
        /// Get or post data to a URL
        /// </summary>
        /// <param name="config">Configuration options</param>
        /// <param name="url">URL</param>
        /// <param name="responseStatusCode">Response status code</param>
        /// <param name="timeoutSeconds">Timeout, in seconds</param>
        /// <returns>Response data</returns>
        public static string Send(
            Configuration config,
            string url,
            out HttpStatusCode responseStatusCode,
            int timeoutSeconds = 100)
        {
            const string postData = "";
            return Send(config, url, out responseStatusCode, postData, HttpMethod.Get, timeoutSeconds);
        }

        /// <summary>
        /// Get or post data to a URL
        /// </summary>
        /// <param name="config">Configuration options</param>
        /// <param name="url">URL</param>
        /// <param name="cookies">Cookies</param>
        /// <param name="responseStatusCode">Response status code</param>
        /// <param name="timeoutSeconds">Timeout, in seconds</param>
        /// <returns>Response data</returns>
        // ReSharper disable once UnusedMember.Global
        public static string Send(
            Configuration config,
            string url,
            CookieContainer cookies,
            out HttpStatusCode responseStatusCode,
            int timeoutSeconds = 100)
        {
            const string postData = "";
            return Send(config, url, cookies, out responseStatusCode, postData, HttpMethod.Get, timeoutSeconds);
        }

        /// <summary>
        /// Get or post data to a URL
        /// </summary>
        /// <param name="config">Configuration options</param>
        /// <param name="url">URL</param>
        /// <param name="responseStatusCode">Response status code</param>
        /// <param name="postData">Data to post (when method is HttpMethod.Post)</param>
        /// <param name="method">Get, Post, or Put</param>
        /// <param name="timeoutSeconds">Timeout, in seconds</param>
        /// <returns>Response data</returns>
        public static string Send(
            Configuration config,
            string url,
            out HttpStatusCode responseStatusCode,
            string postData,
            HttpMethod method = HttpMethod.Get,
            int timeoutSeconds = 100)
        {
            return Send(config, url, new CookieContainer(), out responseStatusCode, postData, method, timeoutSeconds);
        }

        /// <summary>
        /// Get or post data to a URL
        /// </summary>
        /// <param name="config">Configuration options</param>
        /// <param name="url">URL</param>
        /// <param name="responseStatusCode">Response status code</param>
        /// <param name="postData">Data to post (when method is HttpMethod.Post)</param>
        /// <param name="method">Get, Post, or Put</param>
        /// <param name="timeoutSeconds">Timeout, in seconds</param>
        /// <param name="contentType">Form/post content-type</param>
        /// <param name="sendStringInHeader">If True, and the method is Get, include postData in the header</param>
        /// <param name="loginCredentials">Login credentials</param>
        /// <returns>Response data</returns>
        // ReSharper disable once UnusedMember.Global
        public static string Send(
            Configuration config,
            string url,
            out HttpStatusCode responseStatusCode,
            string postData,
            HttpMethod method,
            int timeoutSeconds,
            string contentType,
            bool sendStringInHeader,
            NetworkCredential loginCredentials)
        {
            return Send(config, url, new CookieContainer(), out responseStatusCode, postData, method, timeoutSeconds, contentType, sendStringInHeader, loginCredentials);
        }

        /// <summary>
        /// Get or post data to a URL
        /// </summary>
        /// <param name="config">Configuration options</param>
        /// <param name="url">URL</param>
        /// <param name="cookies">Cookies</param>
        /// <param name="responseStatusCode">Response status code</param>
        /// <param name="postData">Data to post (when method is HttpMethod.Post)</param>
        /// <param name="method">Get, Post, or Put</param>
        /// <param name="timeoutSeconds">Timeout, in seconds</param>
        /// <param name="loginCredentials">Login credentials</param>
        /// <returns>Response data</returns>
        // ReSharper disable once UnusedMember.Global
        public static string Send(
            Configuration config,
            string url,
            CookieContainer cookies,
            out HttpStatusCode responseStatusCode,
            string postData,
            HttpMethod method,
            int timeoutSeconds,
            NetworkCredential loginCredentials)
        {
            const string contentType = "";
            const bool sendStringInHeader = false;
            return Send(config, url, cookies, out responseStatusCode, postData, method, timeoutSeconds, contentType, sendStringInHeader, loginCredentials);
        }

        /// <summary>
        /// Get or post data to a URL
        /// </summary>
        /// <param name="config">Configuration options</param>
        /// <param name="url">URL</param>
        /// <param name="cookies">Cookies</param>
        /// <param name="responseStatusCode">Response status code</param>
        /// <param name="postData">Data to post (when method is HttpMethod.Post)</param>
        /// <param name="method">Get, Post, or Put</param>
        /// <param name="timeoutSeconds">Timeout, in seconds</param>
        /// <param name="contentType">Form/post content-type</param>
        /// <param name="sendStringInHeader">If True, and the method is Get, include postData in the header</param>
        /// <param name="loginCredentials">Login credentials</param>
        /// <returns>Response data</returns>
        /// <remarks>Uses the Task Parallel Library (TPL)</remarks>
        public static string Send(
            Configuration config,
            string url,
            CookieContainer cookies,
            out HttpStatusCode responseStatusCode,
            string postData = "",
            HttpMethod method = HttpMethod.Get,
            int timeoutSeconds = 100,
            string contentType = "",
            bool sendStringInHeader = false,
            NetworkCredential loginCredentials = null)
        {

            var urlContactInfo = new UrlContactInfo(
                config, url, cookies, postData, method,
                timeoutSeconds, contentType, sendStringInHeader, loginCredentials);

            // Contact the URL in a separate thread so that we can abort the call if it takes too long
            var task = Task.Factory.StartNew(() => Send(urlContactInfo));

            var success = task.Wait((timeoutSeconds + 5) * 1000);

            if (success)
            {
                responseStatusCode = urlContactInfo.ResponseData.ResponseStatusCode;
            }
            else
            {
                urlContactInfo.ResponseData.ResponseText = REQUEST_TIMEOUT_RESPONSE;
                urlContactInfo.ResponseData.ResponseStatusCode = HttpStatusCode.RequestTimeout;
                responseStatusCode = urlContactInfo.ResponseData.ResponseStatusCode;
            }

            return urlContactInfo.ResponseData.ResponseText;
        }

        /// <summary>
        /// Get or post data to a URL
        /// </summary>
        /// <param name="config">Configuration options</param>
        /// <param name="url">URL</param>
        /// <param name="responseStatusCode">Response status code</param>
        /// <param name="timeoutSeconds">Timeout, in seconds</param>
        /// <returns>Response data</returns>
        /// <remarks>Uses ThreadStart instead of TPL</remarks>
        public static string SendViaThreadStart(
            Configuration config,
            string url,
            out HttpStatusCode responseStatusCode,
            int timeoutSeconds = 100)
        {
            const string postData = "";
            return SendViaThreadStart(config, url, new CookieContainer(), out responseStatusCode, postData, HttpMethod.Get, timeoutSeconds);
        }

        /// <summary>
        /// Get or post data to a URL
        /// </summary>
        /// <param name="config">Configuration options</param>
        /// <param name="url">URL</param>
        /// <param name="cookies">Cookies</param>
        /// <param name="responseStatusCode">Response status code</param>
        /// <param name="postData">Data to post (when method is HttpMethod.Post)</param>
        /// <param name="method">Get, Post, or Put</param>
        /// <param name="timeoutSeconds">Timeout, in seconds</param>
        /// <param name="contentType">Form/post content-type</param>
        /// <param name="sendStringInHeader">If True, and the method is Get, include postData in the header</param>
        /// <param name="loginCredentials">Login credentials</param>
        /// <returns>Response data</returns>
        /// <remarks>Uses ThreadStart instead of TPL</remarks>
        public static string SendViaThreadStart(
            Configuration config,
            string url,
            CookieContainer cookies,
            out HttpStatusCode responseStatusCode,
            string postData = "",
            HttpMethod method = HttpMethod.Get,
            int timeoutSeconds = 100,
            string contentType = "",
            bool sendStringInHeader = false,
            NetworkCredential loginCredentials = null)
        {

            try
            {
                if (timeoutSeconds < 5)
                    timeoutSeconds = 5;

                mUrlContactInfo = new UrlContactInfo(
                    config, url, cookies, postData, method,
                    timeoutSeconds, contentType, sendStringInHeader, loginCredentials);

                var startTime = DateTime.UtcNow;
                var runtimeExceeded = false;
                var threadAborted = false;

                mThreadedSend = new Thread(StartThreadedSend);
                mThreadedSend.Start();

                // Loop until URL call finishes, or until timeoutSeconds elapses
                while (mThreadedSend.ThreadState != ThreadState.Stopped)
                {
                    ProgRunner.SleepMilliseconds(25);

                    if (mThreadedSend.ThreadState == ThreadState.Aborted)
                    {
                        threadAborted = true;
                        break;
                    }

                    if (DateTime.UtcNow.Subtract(startTime).TotalSeconds < timeoutSeconds + 5)
                        continue;

                    var abortThread = new Thread(AbortThreadedSendNow);
                    abortThread.Start();
                    ProgRunner.SleepMilliseconds(25);

                    runtimeExceeded = true;
                    threadAborted = true;
                    break;
                }

                if (!threadAborted && !mUrlContactInfo.ResponseData.ExceptionCaught)
                {
                    responseStatusCode = mUrlContactInfo.ResponseData.ResponseStatusCode;
                    return mUrlContactInfo.ResponseData.ResponseText;
                }

                string responseTextToReturn;

                if (runtimeExceeded)
                {
                    mUrlContactInfo.ResponseData.ResponseText = REQUEST_TIMEOUT_RESPONSE;
                    mUrlContactInfo.ResponseData.ResponseStatusCode = HttpStatusCode.RequestTimeout;
                    responseTextToReturn = mUrlContactInfo.ResponseData.ResponseText;
                }
                else if (mUrlContactInfo.ResponseData.ExceptionCaught)
                {
                    mUrlContactInfo.ResponseData.ResponseText = REQUEST_EXCEPTION_RESPONSE;

                    if (mUrlContactInfo.ResponseData.ResponseStatusCode != HttpStatusCode.PreconditionFailed)
                        mUrlContactInfo.ResponseData.ResponseStatusCode = HttpStatusCode.BadRequest;

                    if (string.IsNullOrWhiteSpace(mUrlContactInfo.ResponseData.ExceptionMessage))
                        responseTextToReturn = mUrlContactInfo.ResponseData.ResponseText;
                    else
                        responseTextToReturn = mUrlContactInfo.ResponseData.ResponseText + "; " + mUrlContactInfo.ResponseData.ExceptionMessage;
                }
                else
                {
                    mUrlContactInfo.ResponseData.ResponseText = REQUEST_ABORTED_RESPONSE;
                    mUrlContactInfo.ResponseData.ResponseStatusCode = HttpStatusCode.BadRequest;
                    responseTextToReturn = mUrlContactInfo.ResponseData.ResponseText;
                }

                responseStatusCode = mUrlContactInfo.ResponseData.ResponseStatusCode;

                return responseTextToReturn;

            }
            catch (Exception ex)
            {
                mUrlContactInfo.ResponseData.ResponseStatusCode = HttpStatusCode.BadRequest;
                throw new Exception("Caught exception while trying to start a thread to contact " + url, ex);
            }

        }

        /// <summary>
        /// Get or post data to a URL
        /// </summary>
        private static void Send(UrlContactInfo urlContactInfo)
        {
            var request = InitializeRequest(urlContactInfo);
            urlContactInfo.ResponseData.ResponseStatusCode = HttpStatusCode.NotFound;

            // Prepare the request object
            request.Method = urlContactInfo.Method.GetDescription<HttpMethod>();
            request.PreAuthenticate = false;

            if (urlContactInfo.SendStringInHeader && urlContactInfo.Method == HttpMethod.Get)
            {
                request.Headers.Add("X-Json-Data", urlContactInfo.PostData);
            }

            // Set form/post content-type if necessary
            if (urlContactInfo.Method == HttpMethod.Post && !string.IsNullOrEmpty(urlContactInfo.PostData) && string.IsNullOrEmpty(urlContactInfo.ContentType))
            {
                urlContactInfo.ContentType = "application/x-www-form-urlencoded";
            }

            // Set Content-Type
            if (urlContactInfo.Method == HttpMethod.Post && !string.IsNullOrEmpty(urlContactInfo.ContentType))
            {
                request.ContentType = urlContactInfo.ContentType;
                if (urlContactInfo.PostData != null)
                {
                    request.ContentLength = urlContactInfo.PostData.Length;
                }
            }

            // Write POST data, if POST
            if (urlContactInfo.Method == HttpMethod.Post)
            {
                using (var sw = new StreamWriter(request.GetRequestStream()))
                {
                    sw.Write(urlContactInfo.PostData);
                }
            }

            // Receive response
            urlContactInfo.ResponseData.ResponseText = string.Empty;
            HttpWebResponse response = null;
            try
            {
                request.Timeout = urlContactInfo.TimeoutSeconds * 1000;
                response = (HttpWebResponse)request.GetResponse();
                urlContactInfo.ResponseData.ResponseStatusCode = response.StatusCode;
                var responseStream = response.GetResponseStream();

                if (responseStream != null)
                {
                    using (var sr = new StreamReader(responseStream))
                    {
                        urlContactInfo.ResponseData.ResponseText = sr.ReadToEnd();
                    }
                }
            }
            catch (WebException ex)
            {
                HandleWebException(ex, urlContactInfo.Url, urlContactInfo.ResponseData);
            }
            finally
            {
                if (response != null)
                {
                    if (response is IDisposable toDispose)
                    {
                        toDispose.Dispose();
                    }
                }
            }

        }

        /// <summary>
        /// Upload a file via POST
        /// </summary>
        /// <param name="config"></param>
        /// <param name="location"></param>
        /// <param name="serverBaseAddress"></param>
        /// <param name="fileListObject"></param>
        /// <param name="metadataFilePath"></param>
        /// <param name="debugMode"></param>
        /// <returns></returns>
        public static string SendFileListToIngester(
            Configuration config,
            string location, string serverBaseAddress,
            SortedDictionary<string, FileInfoObject> fileListObject,
            string metadataFilePath,
            eDebugMode debugMode = eDebugMode.DebugDisabled)
        {

            var certificateFilePath = ResolveCertFile(config, "SendFileListToIngester", out var errorMessage);

            if (string.IsNullOrWhiteSpace(certificateFilePath))
            {
                throw new Exception(errorMessage);
            }

            var baseUri = new Uri(serverBaseAddress);
            var uploadUri = new Uri(baseUri, location);
            HttpWebRequest oWebRequest = null;
            var metadataFile = new FileInfo(metadataFilePath);

            // Compute the total number of bytes that will be written to the tar file
            var contentLength = ComputeTarFileSize(fileListObject, metadataFile, debugMode);

            long bytesWritten = 0;
            var lastStatusUpdateTime = DateTime.UtcNow;

            RaiseStatusUpdate(0, bytesWritten, contentLength, string.Empty);

            // Set this to .CreateTarLocal to debug things and create the .tar file locally instead of sending to the server
            // See method PerformTask in clsArchiveUpdate
            var writeToDisk = (debugMode != eDebugMode.DebugDisabled); // aka WriteFile or SaveFile

            if (writeToDisk && Environment.MachineName.IndexOf("proto", StringComparison.CurrentCultureIgnoreCase) >= 0)
            {
                throw new Exception("Should not have writeToDisk set to True when running on a Proto-x server");
            }

            if (!writeToDisk)
            {
                // Make the request
                oWebRequest = (HttpWebRequest)WebRequest.Create(uploadUri);

                var certificate = new X509Certificate2();
                var password = Utilities.DecodePassword(Configuration.CLIENT_CERT_PASSWORD);
                certificate.Import(certificateFilePath, password, X509KeyStorageFlags.PersistKeySet);
                oWebRequest.ClientCertificates.Add(certificate);

                config.SetProxy(oWebRequest);

                oWebRequest.KeepAlive = true;
                oWebRequest.Method = WebRequestMethods.Http.Post;
                oWebRequest.AllowWriteStreamBuffering = false;
                oWebRequest.Accept = "*/*";
                oWebRequest.Expect = null;
                oWebRequest.Timeout = -1;
                oWebRequest.ReadWriteTimeout = -1;
                oWebRequest.ContentLength = contentLength;
                oWebRequest.ContentType = "application/octet-stream";
            }

            Stream oRequestStream;

            if (writeToDisk)
                oRequestStream = new FileStream(@"C:\CTM_Workdir\TestFile3.tar", FileMode.Create, FileAccess.Write, FileShare.Read);
            else
                oRequestStream = oWebRequest.GetRequestStream();

            // Use SharpZipLib to create the tar file on-the-fly and directly push into the request stream
            // This way, the .tar file is never actually created on a local hard drive
            // Code modeled after https://github.com/icsharpcode/SharpZipLib/wiki/GZip-and-Tar-Samples

            var tarOutputStream = new TarOutputStream(oRequestStream);

            var dctDirectoryEntries = new SortedSet<string>();

            // Add the metadata.txt file
            AppendFileToTar(tarOutputStream, metadataFile, MYEMSL_METADATA_FILE_NAME, ref bytesWritten);

            // Add the "data" directory, which will hold all of the files
            // Need a dummy "data" directory to do this
            var tempFolder = Utilities.GetTempDirectory(config);
            var dummyDataFolder = new DirectoryInfo(Path.Combine(tempFolder.FullName, "data"));
            if (!dummyDataFolder.Exists)
                dummyDataFolder.Create();

            AppendFolderToTar(tarOutputStream, dummyDataFolder, "data", ref bytesWritten);

            var startTime = DateTime.UtcNow;

            foreach (var fileToArchive in fileListObject)
            {
                var sourceFile = new FileInfo(fileToArchive.Key);

                if (!string.IsNullOrEmpty(fileToArchive.Value.RelativeDestinationDirectory))
                {
                    if (sourceFile.Directory == null)
                        throw new DirectoryNotFoundException("Cannot access the parent folder for the source file: " + fileToArchive.Value.RelativeDestinationFullPath);

                    if (!dctDirectoryEntries.Contains(sourceFile.Directory.FullName))
                    {
                        // Make a directory entry
                        AppendFolderToTar(tarOutputStream, sourceFile.Directory, fileToArchive.Value.RelativeDestinationDirectory, ref bytesWritten);

                        dctDirectoryEntries.Add(sourceFile.Directory.FullName);
                    }
                }

                AppendFileToTar(tarOutputStream, sourceFile, fileToArchive.Value.RelativeDestinationFullPath, ref bytesWritten);

                var percentComplete = bytesWritten / (double)contentLength * 100;

                // Initially limit status updates to every 3 seconds
                // Increase the time between updates as upload time progresses, with a maximum interval of 90 seconds
                var statusIntervalSeconds = Math.Min(90, 3 + DateTime.UtcNow.Subtract(startTime).TotalSeconds / 10);

                if (DateTime.UtcNow.Subtract(lastStatusUpdateTime).TotalSeconds >= statusIntervalSeconds)
                {
                    lastStatusUpdateTime = DateTime.UtcNow;
                    RaiseStatusUpdate(percentComplete, bytesWritten, contentLength, UPLOADING_FILES + ": " + sourceFile.Name);
                }
            }

            // Close the tar file memory stream (to flush the buffers)
            tarOutputStream.IsStreamOwner = false;
            tarOutputStream.Close();
            bytesWritten += TAR_BLOCK_SIZE_BYTES + TAR_BLOCK_SIZE_BYTES;

            RaiseStatusUpdate(100, bytesWritten, contentLength, string.Empty);

            // Close the request
            oRequestStream.Close();

            RaiseStatusUpdate(100, contentLength, contentLength, string.Empty);

            if (writeToDisk)
                return string.Empty;

            var responseData = string.Empty;

            WebResponse response = null;
            try
            {
                // The response should be empty if everything worked
                response = oWebRequest.GetResponse();
                var responseStream = response.GetResponseStream();
                if (responseStream != null)
                {
                    using (var sr = new StreamReader(responseStream))
                    {
                        responseData = sr.ReadToEnd();
                    }
                }
            }
            catch (WebException ex)
            {
                HandleWebException(ex, uploadUri.ToString());
            }
            finally
            {
                ((IDisposable)response)?.Dispose();
            }

            return responseData;

        }

        /// <summary>
        /// Start a thread to contact the url
        /// </summary>
        private static void StartThreadedSend()
        {
            try
            {
                Send(mUrlContactInfo);
            }
            catch (Exception ex)
            {
                mUrlContactInfo.ResponseData.RegisterException(ex);
                OnErrorEvent(ex.Message, ex);
            }

        }

        private static long AddTarFileContentLength(string pathInArchive, long fileSizeBytes)
        {
            return AddTarFileContentLength(pathInArchive, fileSizeBytes, out _);
        }

        private static long AddTarFileContentLength(string pathInArchive, long fileSizeBytes, out int headerBlocks)
        {

            long contentLength = 0;
            bool longPath;

            if (pathInArchive.EndsWith("/"))
            {
                // Directory entry
                longPath = (pathInArchive.Length > 100);
            }
            else
            {
                // File entry
                longPath = (pathInArchive.Length > 100);
            }

            // Header block for current file
            headerBlocks = 1;

            if (longPath)
            {
                // SharpZipLib will add two extra 512 byte blocks since this file has an extra long file path
                //  (if the path is over 512 chars, SharpZipLib will add 3 blocks, etc.)
                //
                // The first block will have filename "././@LongLink" and placeholder metadata (file date, file size, etc.)
                // The next block will have the actual long filename
                // The third block is the standard file info block
                var extraBlocks = (int)(Math.Ceiling(pathInArchive.Length / 512.0)) + 1;
                headerBlocks += extraBlocks;
            }

            contentLength += TAR_BLOCK_SIZE_BYTES * headerBlocks;
            // File contents
            long fileBlocks = (int)Math.Ceiling(fileSizeBytes / (double)TAR_BLOCK_SIZE_BYTES);
            contentLength += fileBlocks * TAR_BLOCK_SIZE_BYTES;

            return contentLength;
        }

        private static long ComputeTarFileSize(SortedDictionary<string, FileInfoObject> fileListObject, FileInfo metadataFile, eDebugMode debugMode)
        {
            long contentLength = 0;

            var debugging = (debugMode != eDebugMode.DebugDisabled);

            if (debugging)
            {
                // Note that "HB" stands for HeaderBlocks
                ConsoleMsgUtils.ShowDebug("FileSize".PadRight(12) + "addonBytes".PadRight(12) + "StartOffset".PadRight(12) + "HB".PadRight(3) + "FilePath");
            }

            // Add the metadata file
            var addonBytes = AddTarFileContentLength(MYEMSL_METADATA_FILE_NAME, metadataFile.Length);

            if (debugging)
                ConsoleMsgUtils.ShowDebug(metadataFile.Length.ToString().PadRight(12) + addonBytes.ToString().PadRight(12) + contentLength.ToString().PadRight(12) + "1".PadRight(3) +
                                          "metadata.txt");

            contentLength += addonBytes;

            // Add the data/ directory

            if (debugging)
                ConsoleMsgUtils.ShowDebug("0".PadRight(12) + TAR_BLOCK_SIZE_BYTES.ToString().PadRight(12) + contentLength.ToString().PadRight(12) + "1".PadRight(3) +
                                          "data/");

            contentLength += TAR_BLOCK_SIZE_BYTES;

            var dctDirectoryEntries = new SortedSet<string>();

            // Add the files to be archived
            foreach (var fileToArchive in fileListObject)
            {
                var sourceFile = new FileInfo(fileToArchive.Key);

                int headerBlocks;
                if (!string.IsNullOrEmpty(fileToArchive.Value.RelativeDestinationDirectory))
                {
                    if (sourceFile.Directory == null)
                        throw new DirectoryNotFoundException("Cannot access the parent folder for the source file: " + fileToArchive.Value.RelativeDestinationFullPath);

                    if (!dctDirectoryEntries.Contains(sourceFile.Directory.FullName))
                    {
                        var dirPathInArchive = fileToArchive.Value.RelativeDestinationDirectory.TrimEnd('/') + "/";
                        addonBytes = AddTarFileContentLength(dirPathInArchive, 0, out headerBlocks);

                        if (debugging)
                            ConsoleMsgUtils.ShowDebug(
                                "0".PadRight(12) +
                                addonBytes.ToString().PadRight(12) +
                                contentLength.ToString().PadRight(12) +
                                headerBlocks.ToString().PadRight(3) +
                                FileTools.CompactPathString(dirPathInArchive, 75));

                        contentLength += addonBytes;

                        dctDirectoryEntries.Add(sourceFile.Directory.FullName);
                    }
                }

                var pathInArchive = "";
                if (!string.IsNullOrWhiteSpace(fileToArchive.Value.RelativeDestinationDirectory))
                    pathInArchive += fileToArchive.Value.RelativeDestinationDirectory.TrimEnd('/') + '/';

                pathInArchive += fileToArchive.Value.FileName;

                addonBytes = AddTarFileContentLength(pathInArchive, fileToArchive.Value.FileSizeInBytes, out headerBlocks);

                if (debugging)
                    ConsoleMsgUtils.ShowDebug(
                        fileToArchive.Value.FileSizeInBytes.ToString().PadRight(12) +
                        addonBytes.ToString().PadRight(12) +
                        contentLength.ToString().PadRight(12) +
                        headerBlocks.ToString().PadRight(3) +
                        FileTools.CompactPathString(fileToArchive.Value.RelativeDestinationFullPath, 100));

                contentLength += addonBytes;

            }

            // Append two empty blocks (appended by SharpZipLib at the end of the .tar file)
            for (var i = 1; i <= 2; i++)
            {
                if (debugging)
                {
                    ConsoleMsgUtils.ShowDebug("0".PadRight(12) + TAR_BLOCK_SIZE_BYTES.ToString().PadRight(12) +
                                              contentLength.ToString().PadRight(12) +
                                              "0".PadRight(3) +
                                              "512 block at end of .tar");
                }

                contentLength += TAR_BLOCK_SIZE_BYTES;
            }

            // Round up contentLength to the nearest 10240 bytes
            // Note that recordCount is a long to prevent overflow errors when computing finalPaddingLength
            var recordCount = (long)Math.Ceiling(contentLength / (double)TarBuffer.DefaultRecordSize);
            var finalPaddingLength = (recordCount * TarBuffer.DefaultRecordSize) - contentLength;

            if (debugging)
                ConsoleMsgUtils.ShowDebug("0".PadRight(12) + finalPaddingLength.ToString().PadRight(12) + contentLength.ToString().PadRight(12) + "0".PadRight(3) +
                                          "Padding at end (to make multiple of " + TarBuffer.DefaultRecordSize + ")");

            contentLength = recordCount * TarBuffer.DefaultRecordSize;

            if (debugging)
                ConsoleMsgUtils.ShowDebug("0".PadRight(12) + "0".PadRight(12) + contentLength.ToString().PadRight(12) + "0".PadRight(3) +
                                          "End of file");

            return contentLength;
        }

        private static void AppendFolderToTar(TarOutputStream tarOutputStream, FileSystemInfo sourceFolder, string pathInArchive, ref long bytesWritten)
        {
            var tarEntry = TarEntry.CreateEntryFromFile(sourceFolder.FullName);

            // Override the name
            if (!pathInArchive.EndsWith("/"))
                pathInArchive += "/";

            tarEntry.Name = pathInArchive;
            tarOutputStream.PutNextEntry(tarEntry);
            bytesWritten += AddTarFileContentLength(pathInArchive, 0);

        }

        private static void AppendFileToTar(TarOutputStream tarOutputStream, FileInfo sourceFile, string destFilenameInTar, ref long bytesWritten)
        {
            using (Stream inputStream = new FileStream(sourceFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                var fileSize = sourceFile.Length;

                // Create a tar entry named as appropriate. You can set the name to anything,
                // but avoid names starting with drive or UNC.

                var entry = TarEntry.CreateTarEntry(destFilenameInTar);

                // Must set size, otherwise TarOutputStream will fail when output exceeds.
                entry.Size = fileSize;

                // Add the entry to the tar stream, before writing the data.
                tarOutputStream.PutNextEntry(entry);

                // this is copied from TarArchive.WriteEntryCore
                var localBuffer = new byte[32 * 1024];
                while (true)
                {
                    var numRead = inputStream.Read(localBuffer, 0, localBuffer.Length);
                    if (numRead <= 0)
                    {
                        break;
                    }
                    tarOutputStream.Write(localBuffer, 0, numRead);
                }

                bytesWritten += AddTarFileContentLength(destFilenameInTar, sourceFile.Length);

            }
            tarOutputStream.CloseEntry();
        }

        /// <summary>
        /// Determine the path to the MyEMSL Certificate file
        /// </summary>
        /// <param name="config">Pacifica Config</param>
        /// <param name="callingMethod">Calling method</param>
        /// <param name="errorMessage">Output: error message</param>
        /// <returns>Path to the file if found, otherwise an empty string</returns>
        public static string ResolveCertFile(Configuration config, string callingMethod, out string errorMessage)
        {
            var certificateFilePath = config.ResolveClientCertFile();

            if (!string.IsNullOrWhiteSpace(certificateFilePath))
            {
                errorMessage = string.Empty;
                return certificateFilePath;
            }

            // Example message:
            // Authentication failure in InitializeRequest; MyEMSL certificate file not found in the current directory or at C:\client_certs\svc-dms.pfx
            errorMessage = "Authentication failure in " + callingMethod + "; " +
                           "MyEMSL certificate file not found in the current directory or at " + Configuration.CLIENT_CERT_FILEPATH;

            return string.Empty;
        }

    }
}
