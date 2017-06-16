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
        public const string DEFAULT_POLICY_SERVER_HOST_NAME = "policydmsdev.my.emsl.pnl.gov";


        /// <summary>
        /// Metadata Server host name on the production server
        /// </summary>
        public const string DEFAULT_METADATA_SERVER_HOST_NAME = "metadatadev.my.emsl.pnl.gov";


        /// <summary>
        /// Elastic search host name on the production server
        /// </summary>
        public const string DEFAULT_ELASTIC_SEARCH_HOST_NAME = "192.168.1.173:9200";


        /// <summary>
        /// Item search service host name on the production server
        /// </summary>
        //public const string DEFAULT_ITEM_SEARCH_HOST_NAME = "undefined_does_not_exist.my.emsl.pnl.gov";
        public const string DEFAULT_ITEM_SEARCH_HOST_NAME = "metadatadev.my.emsl.pnl.gov";

        /// <summary>
        /// Ingest host name on the production server
        /// </summary>
        public const string DEFAULT_INGEST_HOST_NAME = "ingestdmsdev.my.emsl.pnl.gov";


        /// <summary>
        /// Elastic search host name on the test server
        /// </summary>
        //public const string TEST_ELASTIC_SEARCH_HOST_NAME = "test0.my.emsl.pnl.gov";
        public const string TEST_ELASTIC_SEARCH_HOST_NAME = "192.168.1.173:9200";

        /// <summary>
        /// Item search service host name on the test server
        /// </summary>
        //public const string TEST_ITEM_SEARCH_HOST_NAME = "dev1.my.emsl.pnl.gov";
        public const string TEST_ITEM_SEARCH_HOST_NAME = "metdatadev.my.emsl.pnl.gov";


        /// <summary>
        /// Ingest host name on the test server
        /// </summary>
        //public const string TEST_INGEST_HOST_NAME = "test3.my.emsl.pnl.gov";
        public const string TEST_INGEST_HOST_NAME = "ingestdmsdev.my.emsl.pnl.gov";

        public const string CLIENT_CERT_FILEPATH = @"C:\client_certs\svc-dms.pfx";
        public const string CLIENT_CERT_PASSWORD = "dms4fun";

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
                string scheme;
                scheme = UseSecureDataTransfer ? SecuredScheme : UnsecuredScheme;
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

        public static string SearchServerHostName { get; set; } = DEFAULT_ELASTIC_SEARCH_HOST_NAME;

        /// <summary>
        /// By default, returns https://my.emsl.pnl.gov
        /// </summary>
        public static string SearchServerUri { get; } = Scheme + SearchServerHostName;

        public static string HttpProxyUrl { get; set; } = string.Empty;

        public static void SetProxy(HttpWebRequest oWebRequest)
        {
            if (!string.IsNullOrWhiteSpace(HttpProxyUrl))
            {
                oWebRequest.Proxy = new WebProxy(new Uri(HttpProxyUrl));
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
                IngestServerHostName = TEST_INGEST_HOST_NAME;

                if (mUseItemSearch)
                    SearchServerHostName = TEST_ITEM_SEARCH_HOST_NAME;
                else
                    SearchServerHostName = TEST_ELASTIC_SEARCH_HOST_NAME;

            }
            else
            {
                IngestServerHostName = DEFAULT_INGEST_HOST_NAME;

                if (mUseItemSearch)
                    SearchServerHostName = DEFAULT_ITEM_SEARCH_HOST_NAME;
                else
                    SearchServerHostName = DEFAULT_ELASTIC_SEARCH_HOST_NAME;

            }
        }

    }
}
