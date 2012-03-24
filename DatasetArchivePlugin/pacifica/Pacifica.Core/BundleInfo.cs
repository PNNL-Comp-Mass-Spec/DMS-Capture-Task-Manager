namespace Pacifica.Core
{
    public class BundleInfo
    {
        private readonly string _bundleIdentifier;
        private readonly string _statusUrl;

        public BundleInfo(string bundleIdentifier, string statusUrl)
        {
            _bundleIdentifier = bundleIdentifier;
            _statusUrl = statusUrl;
        }

        public string BundleIdentifier
        {
            get
            {
                return _bundleIdentifier;
            }
        }

        public string StatusUrl
        {
            get
            {
                return _statusUrl;
            }
        }
    }
}