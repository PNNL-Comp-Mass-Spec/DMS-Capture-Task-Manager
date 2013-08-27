using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Text;
using System.Xml;
using System.Xml.XPath;

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

		public static event StatusUpdateEventHandler StatusUpdate;

		private static void RaiseStatusUpdate(string bundleIdentifier,
			int percentCompleted, long totalBytesSent,
			long totalBytesToSend, string averageUploadSpeed)
		{
			if (StatusUpdate != null)
			{
				StatusUpdate(bundleIdentifier, percentCompleted, totalBytesSent, totalBytesToSend, averageUploadSpeed);
			}
		}

		public static bool GetFile(
			string url,
			CookieContainer cookies,
			out HttpStatusCode responseStatusCode,
			string downloadFilePath,
			int timeoutSeconds = 100,
			NetworkCredential loginCredentials = null)
		{
			double maxTimeoutHours = 24;
			HttpWebRequest request = InitializeRequest(url, ref cookies, ref timeoutSeconds, loginCredentials, maxTimeoutHours);
			responseStatusCode = HttpStatusCode.NotFound;

			// Prepare the request object
			HttpMethod method = HttpMethod.Get;
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

					Stream ReceiveStream = response.GetResponseStream();

					byte[] buffer = new byte[32767];
					FileStream outFile = new FileStream(downloadFilePath, FileMode.Create);

					int bytesRead;
					while((bytesRead = ReceiveStream.Read(buffer, 0, buffer.Length)) != 0)
						outFile.Write(buffer, 0, bytesRead);

					outFile.Close();

				}
				else
				{
					throw new WebException("HTTP response code not OK: " + response.StatusCode + ", " + response.StatusDescription);
				}
			}
			catch (WebException ex)
			{
				string responseData = string.Empty;
				if (ex.Response != null)
				{
					using (StreamReader sr = new StreamReader(ex.Response.GetResponseStream()))
					{
						int maxLines = 20;
						int linesRead = 0;
						while (sr.Peek() > -1 && linesRead < maxLines)
						{
							responseData += sr.ReadLine() + Environment.NewLine;
							linesRead++;
						}
					}
				}
				responseStatusCode = ((HttpWebResponse)ex.Response).StatusCode;
				throw new Exception(responseData, ex);
			}
			finally
			{
				if (response != null)
				{
					((IDisposable)response).Dispose();
				}
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
			double maxTimeoutHours = 0.1;
			HttpWebRequest request = InitializeRequest(url, ref cookies, ref timeoutSeconds, loginCredentials, maxTimeoutHours);
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
				string responseData;
				if (ex.Response != null)
				{
					using (StreamReader sr = new StreamReader(ex.Response.GetResponseStream()))
					{
						responseData = sr.ReadToEnd();
					}
				}
				else
				{
					responseData = string.Empty;
				}
				if (string.IsNullOrWhiteSpace(responseData))
					responseData = ex.Message;

				responseStatusCode = ((HttpWebResponse)ex.Response).StatusCode;
				throw new Exception(responseData, ex);
			}
			finally
			{
				if (response != null)
				{
					((IDisposable)response).Dispose();
				}
			}
			
		}

		public static HttpWebRequest InitializeRequest(
			string url, 
			ref CookieContainer cookies, 
			ref int timeoutSeconds, 
			NetworkCredential loginCredentials, 
			double maxTimeoutHours = 24)
		{
			Uri uri = new Uri(url);
			string cleanUserName = Utilities.GetUserName(true);

			HttpWebRequest request = (HttpWebRequest)WebRequest.Create(uri);
			Configuration.SetProxy(request);

			if (timeoutSeconds < 3)
				timeoutSeconds = 3;
			
			int maxTimeoutHoursInt = (int)(maxTimeoutHours * 60 * 60);
			if (timeoutSeconds > maxTimeoutHoursInt)
				timeoutSeconds = maxTimeoutHoursInt;

			if (loginCredentials == null)
			{
				request.UseDefaultCredentials = true;
			}
			else
			{
				CredentialCache c = new CredentialCache();
				c.Add(new Uri(url), "Basic", new NetworkCredential(loginCredentials.UserName,
					loginCredentials.SecurePassword));
				request.Credentials = c;
			}

			if (cookies == null)
			{
				cookies = new CookieContainer();
			}

			Cookie cookie = new Cookie("user_name", cleanUserName);
			cookie.Domain = "pnl.gov";
			cookies.Add(cookie);
			request.CookieContainer = cookies;
			return request;
		}

		public static string Send(
			string url,
			out HttpStatusCode responseStatusCode,
			int timeoutSeconds = 100)
		{
			string postData = "";
			return Send(url, out responseStatusCode, postData, HttpMethod.Get, timeoutSeconds);
		}

		public static string Send(
			string url,
			CookieContainer cookies,
			out HttpStatusCode responseStatusCode, 
			int timeoutSeconds = 100)
		{
			string postData = "";
			return Send(url, cookies, out responseStatusCode, postData, HttpMethod.Get, timeoutSeconds);
		}

		public static string Send(
			string url,
			out HttpStatusCode responseStatusCode, 
			string postData,
			HttpMethod method = HttpMethod.Get,
			int timeoutSeconds = 100)
		{
			string contentType = "";
			bool sendStringInHeader = false;
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
			string contentType = "";
			bool sendStringInHeader = false;
			return Send(url, new CookieContainer(), out responseStatusCode, postData, method, timeoutSeconds, contentType, sendStringInHeader, loginCredentials);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="url"></param>
		/// <param name="cookies"></param>
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

			double maxTimeoutHours = 24;
			HttpWebRequest request = InitializeRequest(url, ref cookies, ref timeoutSeconds, loginCredentials, maxTimeoutHours);
			responseStatusCode = HttpStatusCode.NotFound;

			// Prepare the request object
			request.Method = method.GetDescription<HttpMethod>();
			request.PreAuthenticate = false;

			if (sendStringInHeader && method == HttpMethod.Get)
			{
				request.Headers.Add("X-Json-Data", postData);
			}
			
			// Set form/post content-type if necessary
			if (method == HttpMethod.Post && !string.IsNullOrEmpty(postData) && contentType == "")
			{
				contentType = "application/x-www-form-urlencoded";
			}

			// Set Content-Type
			if (method == HttpMethod.Post && !string.IsNullOrEmpty(contentType))
			{
				request.ContentType = contentType;
				request.ContentLength = postData.Length;
			}

			// Write POST data, if POST
			if (method == HttpMethod.Post)
			{
				using (StreamWriter sw = new StreamWriter(request.GetRequestStream()))
				{
					sw.Write(postData);
				}
			}

			// Receive response
			string responseData;
			HttpWebResponse response = null;
			try
			{
				request.Timeout = timeoutSeconds * 1000;
				response = (HttpWebResponse)request.GetResponse();
				responseStatusCode = response.StatusCode;

				using (StreamReader sr = new StreamReader(response.GetResponseStream()))
				{
					responseData = sr.ReadToEnd();
				}
				
			}
			catch (WebException ex)
			{
				if (ex.Response != null)
				{
					using (StreamReader sr = new StreamReader(ex.Response.GetResponseStream()))
					{
						responseData = sr.ReadToEnd();
					}
				}
				else
				{
					responseData = string.Empty;
				}
				responseStatusCode = ((HttpWebResponse)ex.Response).StatusCode;
				throw new Exception(responseData, ex);
			}
			finally
			{
				if (response != null)
				{
					((IDisposable)response).Dispose();
				}
			}

			return responseData;
		}

		private static byte[] CreatePropFindRequest(List<string> properties)
		{
			XmlWriterSettings settings = new XmlWriterSettings();
			settings.Encoding = Encoding.UTF8;

			MemoryStream stream = new MemoryStream();
			XmlWriter writer = XmlWriter.Create(stream, settings);

			writer.WriteStartElement("D", "propfind", "DAV:");
			writer.WriteStartElement("prop", "DAV:");

			foreach (string p in properties)
			{
				string ns, localName;
				ParsePropertyName(p, out ns, out localName);
				writer.WriteElementString(localName, "DAV:", null);
			}

			writer.WriteEndElement();
			writer.WriteEndElement();
			writer.Flush();
			return stream.ToArray();
		}

		private static void ParsePropertyName(string myProperty, out string ns, out string localName)
		{
			int index = 0;
			if (string.IsNullOrEmpty(myProperty))
			{
				throw new ArgumentNullException("myProperty");
			}

			index = Math.Max(myProperty.LastIndexOfAny(new Char[] { '/', ':', '#' }) + 1, 0);

			ns = myProperty.Substring(0, index);
			localName = myProperty.Substring(index);
		}

		private static XmlReader ParsePropFindResponse(string response)
		{
			if (string.IsNullOrEmpty(response))
			{
				throw new ArgumentNullException("response");
			}
			return XmlReader.Create(new StringReader(response));
		}

		private static void CheckDavAttributes(string url, string filePath)
		{
			byte[] buffer;
			XmlReader reader;
			HttpWebRequest request;

			var al = new List<string>();
			al.Add("getlastmodified");
			al.Add("getcontentlength");
			al.Add("resourcetype");

			buffer = CreatePropFindRequest(al);
			request = (HttpWebRequest)WebRequest.Create(url);
			Configuration.SetProxy(request);

			request.Method = "PROPFIND";
			request.ContentType = "text/xml";
			request.Headers.Add("Translate", "f");
			request.Headers.Add("Depth", "0");
			request.SendChunked = true;

			Stream stream = request.GetRequestStream();
			stream.Write(buffer, 0, buffer.Length);
			stream.Flush();
			stream.Dispose();

			WebResponse response = null;

			try
			{
				response = request.GetResponse();
			}
			catch (Exception ex)
			{
				Debug.Print(ex.Message);
			}

			if (response != null)
			{
				string content = new StreamReader(response.GetResponseStream()).ReadToEnd();
				reader = ParsePropFindResponse(content);
				XmlNamespaceManager nsmgr = new XmlNamespaceManager(reader.NameTable);
				nsmgr.AddNamespace("dav", "DAV:");
				XPathDocument doc = new XPathDocument(reader);
			}
		}

		[Obsolete]
		public static string SendFileToDav(string url, string serverBaseAddress,
			 string filePath, NetworkCredential loginCredentials = null, bool createNewFile = true)
		{
			return SendFileToDav(url, serverBaseAddress, filePath, new CookieContainer(),
				loginCredentials, createNewFile);
		}

		public static string SendFileToDav(string url, string serverBaseAddress,
			string filePath, CookieContainer cookies,
			NetworkCredential loginCredentials = null, bool createNewFile = true)
		{
			FileInfo fi = new FileInfo(filePath);
			string createFileResponse;

			Uri baseUri = new Uri(serverBaseAddress);

			Uri uploadUri = new Uri(baseUri, url);

			string credUriStr = url.Substring(0, url.LastIndexOf('/'));
			Uri credCheckUri = new Uri(baseUri, credUriStr);

			if (!createNewFile)
			{
				CheckDavAttributes(uploadUri.AbsoluteUri, filePath);
				createFileResponse = SendFileToDav(url, serverBaseAddress,
					filePath, cookies, loginCredentials, true);
				return createFileResponse;
			}

			ICredentials i1;
			ICredentials i2;
			CredentialCache c1 = new CredentialCache();
			CredentialCache c2 = new CredentialCache();

			if (loginCredentials == null)
			{
				loginCredentials = CredentialCache.DefaultNetworkCredentials;
				i1 = loginCredentials;
				i2 = loginCredentials;
			}
			else
			{
				//Basic authentication cannot be used with DefaultNetworkCredentials.
				c1.Add(credCheckUri, "Basic", new NetworkCredential(loginCredentials.UserName,
					loginCredentials.SecurePassword));
				i1 = c1;
				c2.Add(uploadUri, "Basic", new NetworkCredential(loginCredentials.UserName,
					loginCredentials.SecurePassword));
				i2 = c2;
			}

			//TODO - remove NGT 7/11/2011
			//If Negotiate is added to the CredentialCache it will be used instead of Basic.
			//The problem occurs when your computer is not part of the domain and yet still
			//has a clear path to a4.my.emsl.pnl.gov.
			//c1.Add(credCheckUri, "Negotiate", (NetworkCredential)loginCredentials);
			//c2.Add(uploadUri, "Negotiate", (NetworkCredential)loginCredentials);

			// Make a HEAD request to register the proper authentication stuff
			HttpWebRequest oWebRequest = (HttpWebRequest)WebRequest.Create(credCheckUri);

			oWebRequest.CookieContainer = cookies;
			//TODO - pass proxy in as a parameter to keep this class clean.
			Configuration.SetProxy(oWebRequest);

			oWebRequest.Method = WebRequestMethods.Http.Head;
			oWebRequest.Credentials = i1;
			oWebRequest.PreAuthenticate = true;
			oWebRequest.KeepAlive = true;
			oWebRequest.UnsafeAuthenticatedConnectionSharing = true;

			using (WebResponse oWResponse = oWebRequest.GetResponse()) { }

			long triggerPoint = fi.Length / 20;
			int triggerCount = 1;

			string responseData;
			long contentLength = fi.Length;
			FileStream contentStream = fi.Open(FileMode.Open, FileAccess.Read);
			BinaryReader br = new BinaryReader(contentStream);
			byte[] contentBuffer = new Byte[32767];
			int chunkSize = 32767;

			// Make the request
			oWebRequest = (HttpWebRequest)WebRequest.Create(uploadUri);

			if (cookies == null)
			{
				cookies = new CookieContainer();
			}
			oWebRequest.CookieContainer = cookies;
			//TODO - pass proxy in as a parameter to keep this class clean.
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

			Stream oRequestStream = oWebRequest.GetRequestStream();

			int percent = 0;
			long sent = 0;
			System.DateTime lastStatusUpdateTime = System.DateTime.UtcNow;

			RaiseStatusUpdate(filePath, percent, sent, contentLength, string.Empty);
			while (contentStream.Position < contentLength)
			{
				contentBuffer = br.ReadBytes(chunkSize);
				oRequestStream.Write(contentBuffer, 0, contentBuffer.Length);
				if (contentStream.Position >= triggerCount * triggerPoint)
				{
					triggerCount += 1;
				}

				sent = contentStream.Position;
				percent = (int)Math.Round((((double)sent / (double)contentLength) * 100));

				if (System.DateTime.UtcNow.Subtract(lastStatusUpdateTime).TotalSeconds >= 2)
				{
					// Limit status updates to every 2 seconds
					lastStatusUpdateTime = System.DateTime.UtcNow;
					RaiseStatusUpdate(filePath, percent, sent, contentLength, string.Empty);
				}
			}
			contentStream.Flush();
			oRequestStream.Close();
			contentStream.Close();
			contentStream = null;

			RaiseStatusUpdate(filePath, 100, sent, contentLength, string.Empty);

			WebResponse response = null;
			try
			{
				response = oWebRequest.GetResponse();
				using (StreamReader sr = new StreamReader(response.GetResponseStream()))
				{
					responseData = sr.ReadToEnd();
				}
			}
			catch (WebException ex)
			{
				if (ex.Response != null)
				{
					using (StreamReader sr = new StreamReader(ex.Response.GetResponseStream()))
					{
						responseData = sr.ReadToEnd();
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
				if (response != null)
				{
					((IDisposable)response).Dispose();
				}
			}

			return responseData;
		}
	}
}