using System;
using System.ComponentModel;
using System.IO;
using System.Net;

namespace Pacifica.Core
{
	public class Configuration
	{
		private static string _localTempDirectory = Path.GetTempPath();
		public static string LocalTempDirectory
		{
			get
			{
				return _localTempDirectory;
			}
			set
			{
				_localTempDirectory = value;
			}
		}

		private static bool _uploadFiles = true;
		public static bool UploadFiles
		{
			get
			{
				return _uploadFiles;
			}
			set
			{
				_uploadFiles = value;
			}
		}

		public static bool _useSecureDataTransfer = true;
		public static bool UseSecureDataTransfer
		{
			get
			{
				return _useSecureDataTransfer;
			}
			set
			{
				_useSecureDataTransfer = value;
			}
		}

		public static string Scheme
		{
			get
			{
				string scheme;
				if (UseSecureDataTransfer)
				{
					scheme = SecuredScheme;
				}
				else
				{
					scheme = UnsecuredScheme;
				}
				return scheme + "://";
			}
		}

		public static string _unsecuredScheme = "http";
		public static string UnsecuredScheme
		{
			get
			{
				return _unsecuredScheme;
			}
		}

		public static string _securedScheme = "https";
		public static string SecuredScheme
		{
			get
			{
				return _securedScheme;
			}
		}

		public static string TemporaryBundlePath
		{
			get
			{
				return Configuration.LocalTempDirectory;
			}
			set
			{
				Configuration.LocalTempDirectory = value;
			}
		}

		private static string _serverHostName = "ingest.my.emsl.pnl.gov";
		public static string ServerHostName
		{
			get
			{
				return _serverHostName;
			}
			set
			{
				_serverHostName = value;
			}
		}

		private static string _serverUri = string.Empty;
		public static string ServerUri
		{
			get
			{
				string scheme;
				if (UseSecureDataTransfer)
				{
					scheme = SecuredScheme;
				}
				else
				{
					scheme = UnsecuredScheme;
				}
				return scheme + "://" + ServerHostName;
			}
		}

		private static string _testAuthRelativePath = "/myemsl/testauth/";
		public static string TestAuthUri
		{
			get
			{
				return ServerUri + _testAuthRelativePath;
			}
		}

		private static string _httpProxyUrl = string.Empty;
		public static string HttpProxyUrl
		{
			get
			{
				return _httpProxyUrl;
			}
			set
			{
				_httpProxyUrl = value;

			}
		}

		public static void SetProxy(HttpWebRequest oWebRequest)
		{
			if (!string.IsNullOrWhiteSpace(Configuration.HttpProxyUrl))
			{
				oWebRequest.Proxy = new WebProxy(new Uri(Configuration.HttpProxyUrl));
			}
		}

		private static Auth _authInstance;
		/// <summary>
		/// Gets the most up to date Auth object that should be used for testing authentication, 
		/// and setting (explicit) and saving (implicit) cookies.
		/// </summary>
		/// <value>Returns a null if the Configuration.TestAuthUri does not parse correctly.</value>
		public static Auth AuthInstance
		{
			get
			{
				try
				{
					if (_authInstance == null ||
					_authInstance.Location != TestAuthUri)
					{
						_authInstance = new Auth(new Uri(Configuration.TestAuthUri));
					}
				}
				catch
				{
					return null;
				}
				return _authInstance;
			}
		}
	}
}
