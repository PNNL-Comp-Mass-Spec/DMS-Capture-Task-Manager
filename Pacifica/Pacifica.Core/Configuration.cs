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
        /// Policy Server host name for testing
        /// </summary>
        public const string TEST_POLICY_SERVER_HOST_NAME = "policydmsdev.my.emsl.pnl.gov";

        /// <summary>
        /// Metadata Server host name on the production server
        /// </summary>
        public const string DEFAULT_METADATA_SERVER_HOST_NAME = "metadata.my.emsl.pnl.gov";

        /// <summary>
        /// Metadata Server host name for testing
        /// </summary>
        public const string TEST_METADATA_SERVER_HOST_NAME = "metadatadev.my.emsl.pnl.gov";

        /// <summary>
        /// Ingest host name on the production server
        /// </summary>
        public const string DEFAULT_INGEST_HOST_NAME = "ingestdms.my.emsl.pnl.gov";

        /// <summary>
        /// Ingest host name on the test server
        /// </summary>
        public const string TEST_INGEST_HOST_NAME = "ingestdmsdev.my.emsl.pnl.gov";

        public const string CLIENT_CERT_FILEPATH = @"C:\client_certs\svc-dms.pfx";
        public const string CLIENT_CERT_PASSWORD = "cnr5evm";

        /// <summary>
        /// Local temp directory
        /// </summary>
        public string LocalTempDirectory { get; set; }

        /// <summary>
        /// If true, use https; otherwise use http
        /// </summary>
        public bool UseSecureDataTransfer { get; set; }

        /// <summary>
        /// Returns either https:// or http://
        /// </summary>
        public string Scheme
        {
            get
            {
                var scheme = UseSecureDataTransfer ? SecuredScheme : UnsecuredScheme;
                return scheme + "://";
            }
        }

        private const string UNSECURED_SCHEME = "http";
        public string UnsecuredScheme => UNSECURED_SCHEME;

        private const string SECURED_SCHEME = "https";
        public string SecuredScheme => SECURED_SCHEME;

        /// <summary>
        /// By default, returns https://dev1.my.emsl.pnl.gov/myemsl/status/index.php/api/item_search/
        /// </summary>
        //public string ItemSearchUri => SearchServerUri + ITEM_SEARCH_RELATIVE_PATH;

        public string IngestServerHostName { get; set; }

        /// <summary>
        /// By default, returns https://ingest.my.emsl.pnl.gov
        /// </summary>
        public string IngestServerUri => Scheme + IngestServerHostName;

        public string PolicyServerHostName { get; set; }

        /// <summary>
        /// By default, returns https://policy.my.emsl.pnl.gov
        /// </summary>
        public string PolicyServerUri => Scheme + PolicyServerHostName;

        public string MetadataServerHostName { get; set; }

        /// <summary>
        /// By default, returns https://metadata.my.emsl.pnl.gov
        /// </summary>
        public string MetadataServerUri => Scheme + MetadataServerHostName;

        /// <summary>
        /// Proxy server URL
        /// </summary>
        /// <remarks>Ignored if an empty string (which is default)</remarks>
        public string HttpProxyUrl { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public Configuration()
        {
            LocalTempDirectory = Path.GetTempPath();

            UseSecureDataTransfer = true;

            IngestServerHostName = DEFAULT_INGEST_HOST_NAME;
            PolicyServerHostName = DEFAULT_POLICY_SERVER_HOST_NAME;
            MetadataServerHostName = DEFAULT_METADATA_SERVER_HOST_NAME;

            HttpProxyUrl = string.Empty;

        }

        /// <summary>
        /// Associate the proxy server (if defined) with the WebRequest
        /// </summary>
        /// <param name="oWebRequest"></param>
        public void SetProxy(HttpWebRequest oWebRequest)
        {
            if (!string.IsNullOrWhiteSpace(HttpProxyUrl))
            {
                oWebRequest.Proxy = new WebProxy(new Uri(HttpProxyUrl));
            }
        }

        private bool mUseTestInstance;

        /// <summary>
        /// When true, upload to test3.my.emsl.pnl.gov instead of ingest.my.emsl.pnl.gov
        /// </summary>
        public bool UseTestInstance
        {
            get => mUseTestInstance;

            set
            {
                mUseTestInstance = value;
                UpdateHostNames();
            }
        }

        private void UpdateHostNames()
        {
            if (mUseTestInstance)
            {
                IngestServerHostName = TEST_INGEST_HOST_NAME;
                PolicyServerHostName = TEST_POLICY_SERVER_HOST_NAME;
                MetadataServerHostName = TEST_METADATA_SERVER_HOST_NAME;
            }
            else
            {
                IngestServerHostName = DEFAULT_INGEST_HOST_NAME;
                PolicyServerHostName = DEFAULT_POLICY_SERVER_HOST_NAME;
                MetadataServerHostName = DEFAULT_METADATA_SERVER_HOST_NAME;
            }
        }

    }
}
