using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Net;
using ICSharpCode.SharpZipLib.Tar;
using PRISM;

namespace Pacifica.Core
{
    public class EasyHttp
    {
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
            [Description("PUT")]
            Put = 2
        }


        public enum eDebugMode
        {
            [Description("Debugging is disabled")]
            DebugDisabled = 0,
            [Description("Authenticate with MyEMSL, but create a local .tar file")]
            CreateTarLocal = 1,
            [Description("Do not contact MyEMSL; create a local .tar file")]
            MyEMSLOfflineMode = 2
        }

        private const int TAR_BLOCK_SIZE_BYTES = 512;
        public const string MYEMSL_METADATA_FILE_NAME = "metadata.txt";

        public static event StatusUpdateEventHandler StatusUpdate;

        private static void RaiseStatusUpdate(
            double percentCompleted, long totalBytesSent,
            long totalBytesToSend, string statusMessage)
        {
            StatusUpdate?.Invoke(null, new StatusEventArgs(percentCompleted, totalBytesSent, totalBytesToSend, statusMessage));
        }

        public static bool GetFile(
            string url,
            CookieContainer cookies,
            out HttpStatusCode responseStatusCode,
            string downloadFilePath,
            int timeoutSeconds = 100,
            NetworkCredential loginCredentials = null)
        {
            var request = InitializeRequest(url, ref cookies, ref timeoutSeconds, loginCredentials, maxTimeoutHours: 24);
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
                        throw new WebException("Response stream is null");
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
                    throw new WebException("HTTP response code not OK: " + response.StatusCode + ", " + response.StatusDescription);
                }
            }
            catch (WebException ex)
            {
                var responseData = string.Empty;
                if (ex.Response != null)
                {
                    var responseStream = ex.Response.GetResponseStream();
                    if (responseStream != null)
                    {
                        using (var sr = new StreamReader(responseStream))
                        {
                            const int maxLines = 20;
                            var linesRead = 0;
                            while (!sr.EndOfStream && linesRead < maxLines)
                            {
                                responseData += sr.ReadLine() + Environment.NewLine;
                                linesRead++;
                            }
                        }
                    }

                    responseStatusCode = ((HttpWebResponse)ex.Response).StatusCode;
                }
                else
                {
                    if (ex.Message.Contains("timed out"))
                        responseStatusCode = HttpStatusCode.RequestTimeout;
                }
                throw new Exception(responseData, ex);
            }
            finally
            {
                ((IDisposable)response)?.Dispose();
            }

            return true;
        }

        public static WebHeaderCollection GetHeaders(
            string url,
            out HttpStatusCode responseStatusCode,
            int timeoutSeconds = 100)
        {
            return GetHeaders(url, new CookieContainer(), out responseStatusCode, timeoutSeconds);
        }

        public static WebHeaderCollection GetHeaders(
            string url,
            CookieContainer cookies,
            out HttpStatusCode responseStatusCode,
            int timeoutSeconds = 100,
            NetworkCredential loginCredentials = null)
        {
            const double maxTimeoutHours = 0.1;
            var request = InitializeRequest(url, ref cookies, ref timeoutSeconds, loginCredentials, maxTimeoutHours);
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
                var responseData = string.Empty;
                if (ex.Response != null)
                {
                    var responseStream = ex.Response.GetResponseStream();
                    if (responseStream != null)
                    {
                        using (var sr = new StreamReader(responseStream))
                        {
                            responseData = sr.ReadToEnd();
                        }
                    }
                    responseStatusCode = ((HttpWebResponse)ex.Response).StatusCode;
                }
                else
                {
                    if (ex.Message.Contains("timed out"))
                        responseStatusCode = HttpStatusCode.RequestTimeout;
                }

                if (string.IsNullOrWhiteSpace(responseData))
                    responseData = ex.Message;

                throw new Exception(responseData, ex);
            }
            finally
            {
                ((IDisposable)response)?.Dispose();
            }
        }

        public static HttpWebRequest InitializeRequest(
            string url,
            ref CookieContainer cookies,
            ref int timeoutSeconds,
            NetworkCredential loginCredentials,
            double maxTimeoutHours = 24)
        {
            var uri = new Uri(url);
            var cleanUserName = Utilities.GetUserName(true);

            var request = (HttpWebRequest)WebRequest.Create(uri);
            Configuration.SetProxy(request);

            if (timeoutSeconds < 3)
                timeoutSeconds = 3;

            var maxTimeoutHoursInt = (int)(maxTimeoutHours * 60 * 60);
            if (timeoutSeconds > maxTimeoutHoursInt)
                timeoutSeconds = maxTimeoutHoursInt;

            if (loginCredentials == null)
            {
                request.UseDefaultCredentials = true;
            }
            else
            {
                var c = new CredentialCache();
                c.Add(new Uri(url), "Basic", new NetworkCredential(loginCredentials.UserName, loginCredentials.SecurePassword));
                request.Credentials = c;
            }

            if (cookies == null)
            {
                cookies = new CookieContainer();
            }

            var cookie = new Cookie("user_name", cleanUserName)
            {
                Domain = "pnl.gov"
            };
            cookies.Add(cookie);
            request.CookieContainer = cookies;
            return request;
        }

        public static string Send(
            string url,
            out HttpStatusCode responseStatusCode,
            int timeoutSeconds = 100)
        {
            const string postData = "";
            return Send(url, out responseStatusCode, postData, HttpMethod.Get, timeoutSeconds);
        }

        public static string Send(
            string url,
            CookieContainer cookies,
            out HttpStatusCode responseStatusCode,
            int timeoutSeconds = 100)
        {
            const string postData = "";
            return Send(url, cookies, out responseStatusCode, postData, HttpMethod.Get, timeoutSeconds);
        }

        public static string Send(
            string url,
            out HttpStatusCode responseStatusCode,
            string postData,
            HttpMethod method = HttpMethod.Get,
            int timeoutSeconds = 100)
        {
            const string contentType = "";
            const bool sendStringInHeader = false;
            NetworkCredential loginCredentials = null;

            return Send(url, new CookieContainer(), out responseStatusCode, postData, method, timeoutSeconds, contentType, sendStringInHeader, loginCredentials);
        }

        public static string Send(
            string url,
            out HttpStatusCode responseStatusCode,
            string postData,
            HttpMethod method,
            int timeoutSeconds,
            string contentType,
            bool sendStringInHeader,
            NetworkCredential loginCredentials)
        {
            return Send(url, new CookieContainer(), out responseStatusCode, postData, method, timeoutSeconds, contentType, sendStringInHeader, loginCredentials);
        }

        public static string Send(
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
            return Send(url, cookies, out responseStatusCode, postData, method, timeoutSeconds, contentType, sendStringInHeader, loginCredentials);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="url"></param>
        /// <param name="cookies"></param>
        /// <param name="responseStatusCode"></param>
        /// <param name="postData"></param>
        /// <param name="method"></param>
        /// <param name="timeoutSeconds"></param>
        /// <param name="contentType"></param>
        /// <param name="sendStringInHeader"></param>
        /// <param name="loginCredentials"></param>
        /// <returns>Response data</returns>
        public static string Send(
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

            var request = InitializeRequest(url, ref cookies, ref timeoutSeconds, loginCredentials, maxTimeoutHours: 24);
            responseStatusCode = HttpStatusCode.NotFound;

            // Prepare the request object
            request.Method = method.GetDescription<HttpMethod>();
            request.PreAuthenticate = false;

            if (sendStringInHeader && method == HttpMethod.Get)
            {
                request.Headers.Add("X-Json-Data", postData);
            }

            // Set form/post content-type if necessary
            if (method == HttpMethod.Post && !string.IsNullOrEmpty(postData) && string.IsNullOrEmpty(contentType))
            {
                contentType = "application/x-www-form-urlencoded";
            }

            // Set Content-Type
            if (method == HttpMethod.Post && !string.IsNullOrEmpty(contentType))
            {
                request.ContentType = contentType;
                if (postData != null)
                {
                    request.ContentLength = postData.Length;
                }
            }

            // Write POST data, if POST
            if (method == HttpMethod.Post)
            {
                using (var sw = new StreamWriter(request.GetRequestStream()))
                {
                    sw.Write(postData);
                }
            }

            // Receive response
            var responseData = string.Empty;
            HttpWebResponse response = null;
            try
            {
                request.Timeout = timeoutSeconds * 1000;
                response = (HttpWebResponse)request.GetResponse();
                responseStatusCode = response.StatusCode;
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
                if (ex.Response != null)
                {
                    var responseStream = ex.Response.GetResponseStream();
                    if (responseStream != null)
                    {
                        using (var sr = new StreamReader(responseStream))
                        {
                            responseData = sr.ReadToEnd();
                        }
                    }
                    responseStatusCode = ((HttpWebResponse)ex.Response).StatusCode;
                }
                else
                {
                    if (ex.Message.Contains("timed out"))
                        responseStatusCode = HttpStatusCode.RequestTimeout;
                }

                if (string.IsNullOrWhiteSpace(responseData))
                    throw new Exception(ex.Message, ex);
                else
                    throw new Exception(responseData, ex);

            }
            finally
            {
                ((IDisposable)response)?.Dispose();
            }

            return responseData;
        }

        public static string SendFileListToDavAsTar(string url, string serverBaseAddress,
            SortedDictionary<string, FileInfoObject> fileListObject,
            string metadataFilePath,
            CookieContainer cookies,
            NetworkCredential loginCredentials = null,
            eDebugMode debugMode = eDebugMode.DebugDisabled)
        {

            var baseUri = new Uri(serverBaseAddress);
            var uploadUri = new Uri(baseUri, url);

            var credUriStr = url.Substring(0, url.LastIndexOf('/'));
            var credCheckUri = new Uri(baseUri, credUriStr);

            ICredentials i1;
            ICredentials i2;
            var c1 = new CredentialCache();
            var c2 = new CredentialCache();

            if (loginCredentials == null)
            {
                loginCredentials = CredentialCache.DefaultNetworkCredentials;
                i1 = loginCredentials;
                i2 = loginCredentials;
            }
            else
            {
                // Basic authentication cannot be used with DefaultNetworkCredentials.
                c1.Add(credCheckUri, "Basic", new NetworkCredential(loginCredentials.UserName,
                    loginCredentials.SecurePassword));
                i1 = c1;
                c2.Add(uploadUri, "Basic", new NetworkCredential(loginCredentials.UserName,
                    loginCredentials.SecurePassword));
                i2 = c2;
            }

            // Make a HEAD request to register the proper authentication stuff
            var oWebRequest = (HttpWebRequest)WebRequest.Create(credCheckUri);

            oWebRequest.CookieContainer = cookies;

            Configuration.SetProxy(oWebRequest);

            oWebRequest.Method = WebRequestMethods.Http.Head;
            oWebRequest.Credentials = i1;
            oWebRequest.PreAuthenticate = true;
            oWebRequest.KeepAlive = true;
            oWebRequest.UnsafeAuthenticatedConnectionSharing = true;

            var fiMetadataFile = new FileInfo(metadataFilePath);

            // Compute the total number of bytes that will be written to the tar file
            var contentLength = ComputeTarFileSize(fileListObject, fiMetadataFile, debugMode);

            const double percentComplete = 0;		// Value between 0 and 100
            long bytesWritten = 0;
            var lastStatusUpdateTime = DateTime.UtcNow;

            RaiseStatusUpdate(percentComplete, bytesWritten, contentLength, string.Empty);

            // Set this to True to debug things and create the .tar file locally instead of sending to the server
            var writeToDisk = (debugMode != eDebugMode.DebugDisabled); // aka Writefile or Savefile

            if (writeToDisk && Environment.MachineName.IndexOf("proto", StringComparison.CurrentCultureIgnoreCase) >= 0)
            {
                throw new Exception("Should not have writeToDisk set to True when running on a Proto-x server");
            }

            // ReSharper disable once ConditionIsAlwaysTrueOrFalse
            if (!writeToDisk)
            {
                // Make the request
                oWebRequest = (HttpWebRequest)WebRequest.Create(uploadUri);

                if (cookies == null)
                {
                    cookies = new CookieContainer();
                }
                oWebRequest.CookieContainer = cookies;

                Configuration.SetProxy(oWebRequest);

                oWebRequest.Credentials = i2;
                oWebRequest.PreAuthenticate = true;
                oWebRequest.KeepAlive = true;
                oWebRequest.Method = WebRequestMethods.Http.Put;
                oWebRequest.AllowWriteStreamBuffering = false;
                oWebRequest.Accept = "*/*";
                oWebRequest.Expect = null;
                oWebRequest.Timeout = -1;
                oWebRequest.ReadWriteTimeout = -1;
                oWebRequest.ContentLength = contentLength;

            }

            Stream oRequestStream;

            // ReSharper disable once ConditionIsAlwaysTrueOrFalse
            if (writeToDisk)
                oRequestStream = new FileStream(@"E:\CapMan_WorkDir\TestFile3.tar", FileMode.Create, FileAccess.Write, FileShare.Read);
            else
                oRequestStream = oWebRequest.GetRequestStream();

            // Use SharpZipLib to create the tar file on-the-fly and directly push into the request stream
            // This way, the .tar file is never actually created on a local hard drive
            // Code modeled after https://github.com/icsharpcode/SharpZipLib/wiki/GZip-and-Tar-Samples

            var tarOutputStream = new TarOutputStream(oRequestStream);

            var dctDirectoryEntries = new SortedSet<string>();

            // Add the metadata.txt file
            AppendFileToTar(tarOutputStream, fiMetadataFile, MYEMSL_METADATA_FILE_NAME, ref bytesWritten);

            // Add the "data" directory, which will hold all of the files
            // Need a dummy "data" directory to do this
            var diTempFolder = Utilities.GetTempDirectory();
            var diDummyDataFolder = new DirectoryInfo(Path.Combine(diTempFolder.FullName, "data"));
            if (!diDummyDataFolder.Exists)
                diDummyDataFolder.Create();

            AppendFolderToTar(tarOutputStream, diDummyDataFolder, "data", ref bytesWritten);

            foreach (var fileToArchive in fileListObject)
            {
                var fiSourceFile = new FileInfo(fileToArchive.Key);

                if (!string.IsNullOrEmpty(fileToArchive.Value.RelativeDestinationDirectory))
                {
                    if (fiSourceFile.Directory == null)
                        throw new DirectoryNotFoundException("Cannot access the parent folder for the source file: " + fileToArchive.Value.RelativeDestinationFullPath);

                    if (!dctDirectoryEntries.Contains(fiSourceFile.Directory.FullName))
                    {
                        // Make a directory entry
                        AppendFolderToTar(tarOutputStream, fiSourceFile.Directory, "data/" + fileToArchive.Value.RelativeDestinationDirectory, ref bytesWritten);

                        dctDirectoryEntries.Add(fiSourceFile.Directory.FullName);
                    }
                }

                AppendFileToTar(tarOutputStream, fiSourceFile, "data/" + fileToArchive.Value.RelativeDestinationFullPath, ref bytesWritten);

                if (DateTime.UtcNow.Subtract(lastStatusUpdateTime).TotalSeconds >= 2)
                {
                    // Limit status updates to every 2 seconds
                    RaiseStatusUpdate(percentComplete, bytesWritten, contentLength, string.Empty);
                    lastStatusUpdateTime = DateTime.UtcNow;
                }
            }

            // Close the tar file memory stream (to flush the buffers)
            tarOutputStream.IsStreamOwner = false;
            tarOutputStream.Close();
            bytesWritten += TAR_BLOCK_SIZE_BYTES;

            RaiseStatusUpdate(100, bytesWritten, contentLength, string.Empty);

            // Close the request
            oRequestStream.Close();

            RaiseStatusUpdate(100, contentLength, contentLength, string.Empty);

            // ReSharper disable once ConditionIsAlwaysTrueOrFalse
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
                if (ex.Response != null)
                {
                    var responseStream = ex.Response.GetResponseStream();
                    if (responseStream != null)
                    {
                        using (var sr = new StreamReader(responseStream))
                        {
                            responseData = sr.ReadToEnd();
                        }
                    }

                }
                else
                {
                    responseData = string.Empty;
                }
                throw new Exception(responseData, ex);
            }
            finally
            {
                ((IDisposable)response)?.Dispose();
            }

            return responseData;
        }

        private static long AddTarFileContentLength(string pathInArchive, long fileSizeBytes)
        {
            int headerBlocks;
            return AddTarFileContentLength(pathInArchive, fileSizeBytes, out headerBlocks);
        }

        private static long AddTarFileContentLength(string pathInArchive, long fileSizeBytes, out int headerBlocks)
        {

            long contentLength = 0;
            bool longPath;

            if (pathInArchive.EndsWith("/"))
            {
                // Directory entry
                longPath = (pathInArchive.Length >= 100);
            }
            else
            {
                // File entry
                longPath = (pathInArchive.Length >= 100);
            }

            // Header block for current file
            headerBlocks = 1;
            contentLength += TAR_BLOCK_SIZE_BYTES;

            if (longPath)
            {
                // SharpZipLib will add two extra 512 byte blocks since this file has an extra long file path 
                //  (if the path is over 512 chars then SharpZipLib will add 3 blocks, etc.)				
                //
                // The first block will have filename "././@LongLink" and placeholder metadata (file date, file size, etc.)
                // The next block will have the actual long filename
                var extraBlocks = (int)(Math.Ceiling(pathInArchive.Length / 512.0));
                headerBlocks += extraBlocks;
                contentLength += TAR_BLOCK_SIZE_BYTES + TAR_BLOCK_SIZE_BYTES * extraBlocks;
            }

            // File contents
            long fileBlocks = (int)Math.Ceiling(fileSizeBytes / (double)TAR_BLOCK_SIZE_BYTES);
            contentLength += fileBlocks * TAR_BLOCK_SIZE_BYTES;

            return contentLength;
        }

        private static long ComputeTarFileSize(SortedDictionary<string, FileInfoObject> fileListObject, FileInfo fiMetadataFile, eDebugMode debugMode)
        {
            long contentLength = 0;

            var debugging = (debugMode != eDebugMode.DebugDisabled);

            if (debugging)
            {
                // Note that "HB" stands for HeaderBlocks
                Console.WriteLine();
                Console.WriteLine("FileSize".PadRight(12) + "addonBytes".PadRight(12) + "StartOffset".PadRight(12) + "HB".PadRight(3) + "FilePath");
            }

            // Add the metadata file
            var addonBytes = AddTarFileContentLength(MYEMSL_METADATA_FILE_NAME, fiMetadataFile.Length);

            if (debugging)
                Console.WriteLine(fiMetadataFile.Length.ToString().PadRight(12) + addonBytes.ToString().PadRight(12) + contentLength.ToString().PadRight(12) + "1".PadRight(3) + "metadata.txt");

            contentLength += addonBytes;

            // Add the data/ directory

            if (debugging)
                Console.WriteLine("0".PadRight(12) + TAR_BLOCK_SIZE_BYTES.ToString().PadRight(12) + contentLength.ToString().PadRight(12) + "1".PadRight(3) + "data/");

            contentLength += TAR_BLOCK_SIZE_BYTES;

            var dctDirectoryEntries = new SortedSet<string>();

            // Add the files to be archived
            foreach (var fileToArchive in fileListObject)
            {
                var fiSourceFile = new FileInfo(fileToArchive.Key);

                int headerBlocks;
                if (!string.IsNullOrEmpty(fileToArchive.Value.RelativeDestinationDirectory))
                {
                    if (fiSourceFile.Directory == null)
                        throw new DirectoryNotFoundException("Cannot access the parent folder for the source file: " + fileToArchive.Value.RelativeDestinationFullPath);

                    if (!dctDirectoryEntries.Contains(fiSourceFile.Directory.FullName))
                    {
                        var dirPathInArchive = "data/" + fileToArchive.Value.RelativeDestinationDirectory + "/";
                        addonBytes = AddTarFileContentLength(dirPathInArchive, 0, out headerBlocks);

                        if (debugging)
                            Console.WriteLine(
                                "0".PadRight(12) +
                                addonBytes.ToString().PadRight(12) +
                                contentLength.ToString().PadRight(12) +
                                headerBlocks.ToString().PadRight(3) +
                                clsFileTools.CompactPathString(dirPathInArchive, 75));

                        contentLength += addonBytes;

                        dctDirectoryEntries.Add(fiSourceFile.Directory.FullName);
                    }
                }

                var pathInArchive = "data/";
                if (!string.IsNullOrWhiteSpace(fileToArchive.Value.RelativeDestinationDirectory))
                    pathInArchive += fileToArchive.Value.RelativeDestinationDirectory + '/';

                pathInArchive += fileToArchive.Value.FileName;

                addonBytes = AddTarFileContentLength(pathInArchive, fileToArchive.Value.FileSizeInBytes, out headerBlocks);

                if (debugging)
                    Console.WriteLine(
                        fileToArchive.Value.FileSizeInBytes.ToString().PadRight(12) +
                        addonBytes.ToString().PadRight(12) +
                        contentLength.ToString().PadRight(12) +
                        headerBlocks.ToString().PadRight(3) +
                        clsFileTools.CompactPathString("data/" + fileToArchive.Value.RelativeDestinationFullPath, 100));

                contentLength += addonBytes;

            }

            // Append one empty block (appended by SharpZipLib at the end of the .tar file
            if (debugging)
                Console.WriteLine("0".PadRight(12) + TAR_BLOCK_SIZE_BYTES.ToString().PadRight(12) + contentLength.ToString().PadRight(12) + "0".PadRight(3) + "512 block at end of .tar");

            contentLength += TAR_BLOCK_SIZE_BYTES;

            // Round up contentLength to the nearest 10240 bytes
            // Note that recordCount is a long to prevent overflow errors when computing finalPadderLength
            var recordCount = (long)Math.Ceiling(contentLength / (double)TarBuffer.DefaultRecordSize);
            var finalPadderLength = (recordCount * TarBuffer.DefaultRecordSize) - contentLength;

            if (debugging)
                Console.WriteLine("0".PadRight(12) + finalPadderLength.ToString().PadRight(12) + contentLength.ToString().PadRight(12) + "0".PadRight(3) + "Padder block at end (to make multiple of " + TarBuffer.DefaultRecordSize + ")");

            contentLength = recordCount * TarBuffer.DefaultRecordSize;

            if (debugging)
                Console.WriteLine("0".PadRight(12) + "0".PadRight(12) + contentLength.ToString().PadRight(12) + "0".PadRight(3) + "End of file");

            return contentLength;
        }

        private static void AppendFolderToTar(TarOutputStream tarOutputStream, DirectoryInfo diFolder, string pathInArchive, ref long bytesWritten)
        {
            var tarEntry = TarEntry.CreateEntryFromFile(diFolder.FullName);

            // Override the name
            if (!pathInArchive.EndsWith("/"))
                pathInArchive += "/";

            tarEntry.Name = pathInArchive;
            tarOutputStream.PutNextEntry(tarEntry);
            bytesWritten += AddTarFileContentLength(pathInArchive, 0);

        }

        private static void AppendFileToTar(TarOutputStream tarOutputStream, FileInfo fiSourceFile, string destFilenameInTar, ref long bytesWritten)
        {
            using (Stream inputStream = new FileStream(fiSourceFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                var fileSize = fiSourceFile.Length;

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

                bytesWritten += AddTarFileContentLength(destFilenameInTar, fiSourceFile.Length);

            }
            tarOutputStream.CloseEntry();
        }
    }
}