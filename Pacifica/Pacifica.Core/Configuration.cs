using System;
using System.IO;
using System.Net;

namespace Pacifica.Core
{
    public class Configuration
    {
        /// <summary>
        /// Policy Server host name on the production server
        /// </summary>
        public const string DEFAULT_POLICY_SERVER_HOST_NAME = "policydms.my.emsl.pnl.gov";

        /// <summary>
        /// Metadata Server host name on the production server
        /// </summary>
        public const string DEFAULT_METADATA_SERVER_HOST_NAME = "metadata.my.emsl.pnl.gov";

        /// <summary>
        /// Item search service host name on the production server
        /// </summary>
        public const string DEFAULT_ITEM_SEARCH_HOST_NAME = "metadata.my.emsl.pnl.gov";

        /// <summary>
        /// Ingest host name on the production server
        /// </summary>

        /// <summary>
        /// Elastic search host name on the test server
        /// </summary>
        //public const string TEST_ELASTIC_SEARCH_HOST_NAME = "test0.my.emsl.pnl.gov";
        [Obsolete("Deprecated in summary 2017")]
        public const string TEST_ELASTIC_SEARCH_HOST_NAME = "192.168.1.173:9200";

        /// <summary>
        /// Item search service host name on the test server
        /// </summary>
        //public const string TEST_ITEM_SEARCH_HOST_NAME = "dev1.my.emsl.pnl.gov";
        [Obsolete("Deprecated in summary 2017")]
        public const string TEST_ITEM_SEARCH_HOST_NAME = "metdatadev.my.emsl.pnl.gov";
        public const string DEFAULT_INGEST_HOST_NAME = "ingestdms.my.emsl.pnl.gov";

        /// <summary>
        /// Ingest host name on the test server
        /// </summary>
        //public const string TEST_INGEST_HOST_NAME = "test3.my.emsl.pnl.gov";
        public const string TEST_INGEST_HOST_NAME = "ingestdmsdev.my.emsl.pnl.gov";

        public const string CLIENT_CERT_FILEPATH = @"C:\client_certs\svc-dms.pfx";
        public const string CLIENT_CERT_PASSWORD = "cnr5evm";

        /// <summary>
        /// Local temp directory
        /// </summary>
        public static string LocalTempDirectory { get; set; } = Path.GetTempPath();

        public static bool mUseSecureDataTransfer = true;
        public static bool UseSecureDataTransfer { get; set; } = mUseSecureDataTransfer;

        public static string Scheme
        {
            get
            {
                var scheme = UseSecureDataTransfer ? SecuredScheme : UnsecuredScheme;
                return scheme + "://";
            }
        }

        private const string UNSECURED_SCHEME = "http";
        public static string UnsecuredScheme => UNSECURED_SCHEME;

        private const string SECURED_SCHEME = "https";
        public static string SecuredScheme => SECURED_SCHEME;

        public static string BundlePath { get; set; } = LocalTempDirectory;

        /// <summary>
        /// By default, returns https://dev1.my.emsl.pnl.gov/myemsl/status/index.php/api/item_search/
        /// </summary>
        //public static string ItemSearchUri => SearchServerUri + ITEM_SEARCH_RELATIVE_PATH;

        public static string IngestServerHostName { get; set; } = DEFAULT_INGEST_HOST_NAME;

        /// <summary>
        /// By default, returns https://ingest.my.emsl.pnl.gov
        /// </summary>
        public static string IngestServerUri { get; } = Scheme + IngestServerHostName;

        public static string PolicyServerHostName { get; set; } = DEFAULT_POLICY_SERVER_HOST_NAME;

        /// <summary>
        /// By default, returns https://policy.my.emsl.pnl.gov
        /// </summary>
        public static string PolicyServerUri { get; } = Scheme + PolicyServerHostName;

        public static string MetadataServerHostName { get; set; } = DEFAULT_METADATA_SERVER_HOST_NAME;

        /// <summary>
        /// By default, returns https://metadata.my.emsl.pnl.gov
        /// </summary>
        public static string MetadataServerUri { get; } = Scheme + MetadataServerHostName;

        [Obsolete("Deprecated in summary 2017")]
        public static string SearchServerHostName { get; set; } = DEFAULT_ELASTIC_SEARCH_HOST_NAME;

        /// <summary>
        /// By default, returns https://my.emsl.pnl.gov
        /// </summary>
        [Obsolete("Deprecated in summary 2017")]
        public static string SearchServerUri { get; } = Scheme + SearchServerHostName;

        /// <summary>
        /// Proxy server URL
        /// </summary>
        /// <remarks>Ignored if an empty string (which is default)</remarks>
        public static string HttpProxyUrl { get; set; } = string.Empty;

        /// <summary>
        /// Associate the proxy server (if defined) with the WebRequest
        /// </summary>
        /// <param name="oWebRequest"></param>
        public static void SetProxy(HttpWebRequest oWebRequest)
        {
            if (!string.IsNullOrWhiteSpace(HttpProxyUrl))
            {
                oWebRequest.Proxy = new WebProxy(new Uri(HttpProxyUrl));
            }
        }

        [Obsolete("Deprecated in summary 2017")]
        private static bool mUseItemSearch;

        /// <summary>
        /// When true, use the Item Search service (released in summer 2017)
        /// </summary>
        [Obsolete("Deprecated in summary 2017")]
        public static bool UseItemSearch
        {
            get => mUseItemSearch;

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
            get => mUseTestInstance;

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
                IngestServerHostName = TEST_INGEST_HOST_NAME;

                // Deprecated in Summer 2017:
                //if (mUseItemSearch)
                //    SearchServerHostName = TEST_ITEM_SEARCH_HOST_NAME;
                //else
                //    SearchServerHostName = TEST_ELASTIC_SEARCH_HOST_NAME;

            }
            else
            {
                IngestServerHostName = DEFAULT_INGEST_HOST_NAME;

                // Deprecated in Summer 2017:
                //if (mUseItemSearch)
                //    SearchServerHostName = DEFAULT_ITEM_SEARCH_HOST_NAME;
                //else
                //    SearchServerHostName = DEFAULT_ELASTIC_SEARCH_HOST_NAME;

            }
        }

    }
}
