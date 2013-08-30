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
					// https
					scheme = SecuredScheme;
				}
				else
				{
					// http
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

		public static string BundlePath
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

		private static string _apiRelativePath = "/myemsl/api/";

		/// <summary>
		/// By default, returns https://my.emsl.pnl.gov/myemsl/api/
		/// </summary>
		public static string ApiUri
		{
			get
			{
				return SearchServerUri + _apiRelativePath;
			}
		}

		private static string _elasticSearchRelativePath = "/myemsl/elasticsearch/";

		/// <summary>
		/// By default, returns https://my.emsl.pnl.gov/myemsl/elasticsearch/
		/// </summary>
		public static string ElasticSearchUri
		{
			get
			{
				return SearchServerUri + _elasticSearchRelativePath;
			}
		}

		private static string _ingestServerHostName = "ingest.my.emsl.pnl.gov";
		public static string IngestServerHostName
		{
			get
			{
				return _ingestServerHostName;
			}
			set
			{
				_ingestServerHostName = value;
			}
		}

		/// <summary>
		/// By default, returns https://ingest.my.emsl.pnl.gov
		/// </summary>
		public static string IngestServerUri
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
				return scheme + "://" + IngestServerHostName;
			}
		}


		private static string _searchServerHostName = "my.emsl.pnl.gov";
		public static string SearchServerHostName
		{
			get
			{
				return _searchServerHostName;
			}
			set
			{
				_searchServerHostName = value;
			}
		}

		/// <summary>
		/// By default, returns https://my.emsl.pnl.gov
		/// </summary>
		public static string SearchServerUri
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
				return scheme + "://" + SearchServerHostName;
			}
		}

		private static string _testAuthRelativePath = "/myemsl/testauth/";

		/// <summary>
		/// By default, returns https://ingest.my.emsl.pnl.gov/myemsl/testauth/
		/// </summary>
		public static string TestAuthUri
		{
			get
			{
				return IngestServerUri + _testAuthRelativePath;
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
