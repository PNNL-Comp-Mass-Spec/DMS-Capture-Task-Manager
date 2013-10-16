using System;
using System.IO;
using System.Net;

namespace Pacifica.Core
{
	public class Configuration
	{
		private static string mLocalTempDirectory = Path.GetTempPath();
		public static string LocalTempDirectory
		{
			get
			{
				return mLocalTempDirectory;
			}
			set
			{
				mLocalTempDirectory = value;
			}
		}

		public static bool mUseSecureDataTransfer = true;
		public static bool UseSecureDataTransfer
		{
			get
			{
				return mUseSecureDataTransfer;
			}
			set
			{
				mUseSecureDataTransfer = value;
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

		private const string UNSECURED_SCHEME = "http";
		public static string UnsecuredScheme
		{
			get
			{
				return UNSECURED_SCHEME;
			}
		}

		private const string SECURED_SCHEME = "https";
		public static string SecuredScheme
		{
			get
			{
				return SECURED_SCHEME;
			}
		}

		public static string BundlePath
		{
			get
			{
				return LocalTempDirectory;
			}
			set
			{
				LocalTempDirectory = value;
			}
		}

		private const string ELASTIC_SEARCH_RELATIVE_PATH = "/myemsl/elasticsearch/";

		/// <summary>
		/// By default, returns https://my.emsl.pnl.gov/myemsl/elasticsearch/
		/// </summary>
		public static string ElasticSearchUri
		{
			get
			{
				return SearchServerUri + ELASTIC_SEARCH_RELATIVE_PATH;
			}
		}

		private static string mIngestServerHostName = "ingest.my.emsl.pnl.gov";
		public static string IngestServerHostName
		{
			get
			{
				return mIngestServerHostName;
			}
			set
			{
				mIngestServerHostName = value;
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


		private static string mSearchServerHostName = "my.emsl.pnl.gov";
		public static string SearchServerHostName
		{
			get
			{
				return mSearchServerHostName;
			}
			set
			{
				mSearchServerHostName = value;
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

		private const string TEST_AUTH_RELATIVE_PATH = "/myemsl/testauth/";

		/// <summary>
		/// By default, returns https://ingest.my.emsl.pnl.gov/myemsl/testauth/
		/// </summary>
		public static string TestAuthUri
		{
			get
			{
				return IngestServerUri + TEST_AUTH_RELATIVE_PATH;
			}
		}

		private static string mHttpProxyUrl = string.Empty;
		public static string HttpProxyUrl
		{
			get
			{
				return mHttpProxyUrl;
			}
			set
			{
				mHttpProxyUrl = value;

			}
		}

		public static void SetProxy(HttpWebRequest oWebRequest)
		{
			if (!string.IsNullOrWhiteSpace(HttpProxyUrl))
			{
				oWebRequest.Proxy = new WebProxy(new Uri(HttpProxyUrl));
			}
		}

		private static Auth mAuthInstance;
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
					if (mAuthInstance == null || mAuthInstance.Location != TestAuthUri)
					{
						mAuthInstance = new Auth(new Uri(TestAuthUri));
					}
				}
				catch
				{
					return null;
				}
				return mAuthInstance;
			}
		}
	}
}
