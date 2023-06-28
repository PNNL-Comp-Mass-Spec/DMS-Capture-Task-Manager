namespace CaptureTaskManager
{
    public static class Conversion
    {
        /// <summary>
        /// Convert string to boolean; default false if an error
        /// </summary>
        /// <param name="value"></param>
        public static bool CBoolSafe(string value)
        {
            return CBoolSafe(value, false);
        }

        /// <summary>
        /// Convert a string value to a boolean
        /// </summary>
        /// <param name="value"></param>
        /// <param name="defaultValue"></param>
        public static bool CBoolSafe(string value, bool defaultValue)
        {
            if (string.IsNullOrEmpty(value))
            {
                return defaultValue;
            }

            return bool.TryParse(value, out var parsedValue)
                ? parsedValue
                : defaultValue;
        }

        /// <summary>
        /// Convert a string value to an integer
        /// </summary>
        /// <param name="value"></param>
        /// <param name="defaultValue"></param>
        public static int CIntSafe(string value, int defaultValue)
        {
            if (string.IsNullOrEmpty(value))
            {
                return defaultValue;
            }

            return int.TryParse(value, out var parsedValue)
                ? parsedValue
                : defaultValue;
        }

        /// <summary>
        /// Surround a file (or directory) path with double quotes if it contains spaces
        /// </summary>
        /// <param name="filePath"></param>
        public static string PossiblyQuotePath(string filePath)
        {
            if (string.IsNullOrEmpty(filePath))
            {
                return string.Empty;
            }

            if (filePath.Contains(" "))
            {
                if (!filePath.StartsWith("\""))
                {
                    filePath = "\"" + filePath;
                }

                if (!filePath.EndsWith("\""))
                {
                    filePath += "\"";
                }
            }

            return filePath;
        }
    }
}