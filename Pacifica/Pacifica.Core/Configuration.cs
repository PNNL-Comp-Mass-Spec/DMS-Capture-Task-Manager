using System;
using System.IO;
using System.Net;

namespace Pacifica.Core
{
    public class Configuration
    {
        /// <summary>
        /// Elastic search host name on the production server
        /// </summary>
        public const string DEFAULT_ELASTIC_SEARCH_HOST_NAME = "my.emsl.pnl.gov";

        /// <summary>
        /// Item search service host name on the production server
        /// </summary>
        public const string DEFAULT_ITEM_SEARCH_HOST_NAME = "undefined_does_not_exist.my.emsl.pnl.gov";

        /// <summary>
        /// Ingest host name on the production server
        /// </summary>
        public const string DEFAULT_INGEST_HOST_NAME = "ingest.my.emsl.pnl.gov";

        /// <summary>
        /// Elastic search host name on the test server
        /// </summary>
        public const string TEST_ELASTIC_SEARCH_HOST_NAME = "test0.my.emsl.pnl.gov";

        /// <summary>
        /// Item search service host name on the test server
        /// </summary>
        public const string TEST_ITEM_SEARCH_HOST_NAME = "dev1.my.emsl.pnl.gov";

        /// <summary>
        /// Ingest host name on the test server
        /// </summary>
        public const string TEST_INGEST_HOST_NAME = "test3.my.emsl.pnl.gov";


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

        private const string API_RELATIVE_PATH = "/myemsl/api/";

        /// <summary>
        /// By default, returns https://my.emsl.pnl.gov/myemsl/api/
        /// </summary>
        public static string ApiUri
        {
            get
            {
                return SearchServerUri + API_RELATIVE_PATH;
            }
        }

        private const string ELASTIC_SEARCH_RELATIVE_PATH = "/myemsl/elasticsearch/";       // MyEMSLReader.Reader will append: simple_items
        private const string TEST_SERVER_SEARCH_RELATIVE_PATH = "/myemsl/search/simple/";   // MyEMSLReader.Reader will append: index.shtml

        /// <summary>
        /// By default, returns https://my.emsl.pnl.gov/myemsl/elasticsearch/
        /// </summary>
        public static string ElasticSearchUri
        {
            get
            {
                if (UseTestInstance)
                    return SearchServerUri + TEST_SERVER_SEARCH_RELATIVE_PATH;
                else
                    return SearchServerUri + ELASTIC_SEARCH_RELATIVE_PATH;
            }
        }

        private const string ITEM_SEARCH_RELATIVE_PATH = "/myemsl/status/index.php/api/item_search/";

        /// <summary>
        /// By default, returns https://dev1.my.emsl.pnl.gov/myemsl/status/index.php/api/item_search/
        /// </summary>
        public static string ItemSearchUri
        {
            get
            {
                return SearchServerUri + ITEM_SEARCH_RELATIVE_PATH;
            }
        }

        private static string mIngestServerHostName = DEFAULT_INGEST_HOST_NAME;
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

        private static string mSearchServerHostName = DEFAULT_ELASTIC_SEARCH_HOST_NAME;
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

        public const string TEST_AUTH_RELATIVE_PATH = "/myemsl/testauth/";

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

        private static bool mUseItemSearch;

        public static bool UseItemSearch
        {
            get
            {
                return mUseItemSearch;
            }

            set
            {
                mUseItemSearch = value;
                UpdateHostNames();
            }
        }


        private static bool mUseTestInstance;

        /// <summary>
        /// When true, upload to test3.my.emsl.pnl.gov instead of ingest.my.emsl.pnl.gov
        /// </summary>
        public static bool UseTestInstance
        {
            get
            {
                return mUseTestInstance;
            }

            set
            {
                mUseTestInstance = value;
                UpdateHostNames();
            }
        }

        private static void UpdateHostNames()
        {
            if (mUseTestInstance)
            {
                Configuration.IngestServerHostName = TEST_INGEST_HOST_NAME;

                if (mUseItemSearch)
                    Configuration.SearchServerHostName = TEST_ITEM_SEARCH_HOST_NAME;
                else
                    Configuration.SearchServerHostName = TEST_ELASTIC_SEARCH_HOST_NAME;

            }
            else
            {
                Configuration.IngestServerHostName = DEFAULT_INGEST_HOST_NAME;

                if (mUseItemSearch)
                    Configuration.SearchServerHostName = DEFAULT_ITEM_SEARCH_HOST_NAME;
                else
                    Configuration.SearchServerHostName = DEFAULT_ELASTIC_SEARCH_HOST_NAME;
                
            }
        }

    }
}
