using PRISM;
using System;
using System.IO;
using System.Net;
using System.Reflection;

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
        /// Cart Server host name on the production server
        /// </summary>
        public const string DEFAULT_CART_SERVER_HOST_NAME = "cart.my.emsl.pnl.gov";

        /// <summary>
        /// File Server host name on the production server
        /// </summary>
        public const string DEFAULT_FILE_SERVER_HOST_NAME = "files.my.emsl.pnl.gov";

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

        internal const string CLIENT_CERT_FILENAME = "svc-dms-cert_2018.pfx";
        public const string CLIENT_CERT_FILEPATH = @"C:\client_certs\" + CLIENT_CERT_FILENAME;
        public const string CLIENT_CERT_PASSWORD = "";

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
        /// Server for downloading files via a cart
        /// </summary>
        public string CartServerHostName { get; set; }

        /// <summary>
        /// Cart download server, default https://cart.my.emsl.pnl.gov
        /// </summary>
        public string CartServerUri => Scheme + CartServerHostName;

        /// <summary>
        /// Server for retrieving files one file at a time
        /// </summary>
        public string FileServerHostName { get; set; }

        /// <summary>
        /// File download server, default https://files.my.emsl.pnl.gov
        /// </summary>
        public string FileServerUri => Scheme + FileServerHostName;

        /// <summary>
        /// Ingest server name
        /// </summary>
        public string IngestServerHostName { get; set; }

        /// <summary>
        /// Ingest server, default https://ingest.my.emsl.pnl.gov
        /// </summary>
        public string IngestServerUri => Scheme + IngestServerHostName;

        /// <summary>
        /// Policy server name
        /// </summary>
        public string PolicyServerHostName { get; set; }

        /// <summary>
        /// Policy server, default https://policy.my.emsl.pnl.gov
        /// </summary>
        public string PolicyServerUri => Scheme + PolicyServerHostName;

        /// <summary>
        /// Metadata server name
        /// </summary>
        public string MetadataServerHostName { get; set; }

        /// <summary>
        /// Metadata server, default https://metadata.my.emsl.pnl.gov
        /// </summary>
        public string MetadataServerUri => Scheme + MetadataServerHostName;

        /// <summary>
        /// Proxy server, default empty string
        /// </summary>
        /// <remarks>Ignored if an empty string</remarks>
        public string HttpProxyUrl { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public Configuration()
        {
            LocalTempDirectory = Path.GetTempPath();

            UseSecureDataTransfer = true;

            CartServerHostName = DEFAULT_CART_SERVER_HOST_NAME;
            FileServerHostName = DEFAULT_FILE_SERVER_HOST_NAME;
            IngestServerHostName = DEFAULT_INGEST_HOST_NAME;
            PolicyServerHostName = DEFAULT_POLICY_SERVER_HOST_NAME;
            MetadataServerHostName = DEFAULT_METADATA_SERVER_HOST_NAME;

            HttpProxyUrl = string.Empty;

        }

        /// <summary>
        /// Look for the client certificate file (svc-dms.pfx)
        /// </summary>
        /// <returns>Path to the file if found, otherwise an empty string</returns>
        /// <remarks>First checks the directory with the executing assembly, then checks C:\client_certs\</remarks>
        public string ResolveClientCertFile()
        {
            try
            {
                // Full path to Pacifica.core.dll
                var assemblyPath = Assembly.GetExecutingAssembly().Location;

                // Look for svc-dms.pfx in the folder with Pacifica.core.dll
                var assemblyFile = new FileInfo(assemblyPath);
                if (assemblyFile.DirectoryName != null)
                {
                    var localCertFile = new FileInfo(Path.Combine(assemblyFile.DirectoryName, CLIENT_CERT_FILENAME));
                    if (localCertFile.Exists)
                        return localCertFile.FullName;
                }

                // Look for svc-dms.pfx at C:\client_certs\
                var sharedCertFile = new FileInfo(CLIENT_CERT_FILEPATH);
                if (sharedCertFile.Exists)
                    return sharedCertFile.FullName;
            }
            catch (Exception ex)
            {
                ConsoleMsgUtils.ShowWarning("Exception looking for " + CLIENT_CERT_FILENAME + ": " + ex.Message);
            }

            return string.Empty;
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
        /// When true, upload to ingestdmsdev.my.emsl.pnl.gov instead of ingestdms.my.emsl.pnl.gov
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
